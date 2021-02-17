/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.driver.internal.cluster;

import io.netty.util.concurrent.EventExecutorGroup;

import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Logger;
import org.neo4j.driver.exceptions.DiscoveryException;
import org.neo4j.driver.exceptions.FatalDiscoveryException;
import org.neo4j.driver.exceptions.SecurityException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.net.ServerAddressResolver;

import static java.lang.String.format;
import static java.util.Collections.emptySet;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.internal.util.Futures.failedFuture;

/**
 * This class is used by all router tables to perform discovery.
 * In other words, the methods in this class could be called by multiple threads concurrently.
 */
public class RediscoveryImpl implements Rediscovery
{
    private static final String NO_ROUTERS_AVAILABLE = "Could not perform discovery for database '%s'. No routing server available.";
    private static final String RECOVERABLE_ROUTING_ERROR = "Failed to update routing table with server '%s'.";

    private final BoltServerAddress initialRouter;
    private final RoutingSettings settings;
    private final Logger logger;
    private final ClusterCompositionProvider provider;
    private final ServerAddressResolver resolver;
    private final EventExecutorGroup eventExecutorGroup;

    public RediscoveryImpl( BoltServerAddress initialRouter, RoutingSettings settings, ClusterCompositionProvider provider,
            EventExecutorGroup eventExecutorGroup, ServerAddressResolver resolver, Logger logger )
    {
        this.initialRouter = initialRouter;
        this.settings = settings;
        this.logger = logger;
        this.provider = provider;
        this.resolver = resolver;
        this.eventExecutorGroup = eventExecutorGroup;
    }

    /**
     * Given a database and its current routing table, and the global connection pool, use the global cluster composition provider to fetch a new
     * cluster composition, which would be used to update the routing table of the given database and global connection pool.
     *
     * @param routingTable current routing table of the given database.
     * @param connectionPool connection pool.
     * @return new cluster composition.
     */
    @Override
    public CompletionStage<ClusterComposition> lookupClusterComposition( RoutingTable routingTable, ConnectionPool connectionPool, Bookmark bookmark )
    {
        CompletableFuture<ClusterComposition> result = new CompletableFuture<>();
        // if we failed discovery, we will chain all errors into this one.
        ServiceUnavailableException baseError = new ServiceUnavailableException( String.format( NO_ROUTERS_AVAILABLE, routingTable.database().description() ) );
        lookupClusterComposition( routingTable, connectionPool, 0, 0, result, bookmark, baseError );
        return result;
    }

    private void lookupClusterComposition( RoutingTable routingTable, ConnectionPool pool,
            int failures, long previousDelay, CompletableFuture<ClusterComposition> result, Bookmark bookmark, Throwable baseError )
    {
        lookup( routingTable, pool, bookmark, baseError ).whenComplete( ( composition, completionError ) ->
        {
            Throwable error = Futures.completionExceptionCause( completionError );
            if ( error != null )
            {
                result.completeExceptionally( error );
            }
            else if ( composition != null )
            {
                result.complete( composition );
            }
            else
            {
                int newFailures = failures + 1;
                if ( newFailures >= settings.maxRoutingFailures() )
                {
                    // now we throw our saved error out
                    result.completeExceptionally( baseError );
                }
                else
                {
                    long nextDelay = Math.max( settings.retryTimeoutDelay(), previousDelay * 2 );
                    logger.info( "Unable to fetch new routing table, will try again in " + nextDelay + "ms" );
                    eventExecutorGroup.next().schedule(
                            () -> lookupClusterComposition( routingTable, pool, newFailures, nextDelay, result, bookmark, baseError ),
                            nextDelay, TimeUnit.MILLISECONDS
                    );
                }
            }
        } );
    }

    private CompletionStage<ClusterComposition> lookup( RoutingTable routingTable, ConnectionPool connectionPool, Bookmark bookmark, Throwable baseError )
    {
        CompletionStage<ClusterComposition> compositionStage;

        if ( routingTable.preferInitialRouter() )
        {
            compositionStage = lookupOnInitialRouterThenOnKnownRouters( routingTable, connectionPool, bookmark, baseError );
        }
        else
        {
            compositionStage = lookupOnKnownRoutersThenOnInitialRouter( routingTable, connectionPool, bookmark, baseError );
        }

        return compositionStage;
    }

    private CompletionStage<ClusterComposition> lookupOnKnownRoutersThenOnInitialRouter( RoutingTable routingTable, ConnectionPool connectionPool,
            Bookmark bookmark, Throwable baseError )
    {
        Set<BoltServerAddress> seenServers = new HashSet<>();
        return lookupOnKnownRouters( routingTable, connectionPool, seenServers, bookmark, baseError ).thenCompose( composition ->
        {
            if ( composition != null )
            {
                return completedFuture( composition );
            }
            return lookupOnInitialRouter( routingTable, connectionPool, seenServers, bookmark, baseError );
        } );
    }

    private CompletionStage<ClusterComposition> lookupOnInitialRouterThenOnKnownRouters( RoutingTable routingTable,
            ConnectionPool connectionPool, Bookmark bookmark, Throwable baseError )
    {
        Set<BoltServerAddress> seenServers = emptySet();
        return lookupOnInitialRouter( routingTable, connectionPool, seenServers, bookmark, baseError ).thenCompose( composition ->
        {
            if ( composition != null )
            {
                return completedFuture( composition );
            }
            return lookupOnKnownRouters( routingTable, connectionPool, new HashSet<>(), bookmark, baseError );
        } );
    }

    private CompletionStage<ClusterComposition> lookupOnKnownRouters( RoutingTable routingTable, ConnectionPool connectionPool, Set<BoltServerAddress> seenServers, Bookmark bookmark,
            Throwable baseError )
    {
        BoltServerAddress[] addresses = routingTable.routers().toArray();

        CompletableFuture<ClusterComposition> result = completedWithNull();
        for ( BoltServerAddress address : addresses )
        {
            result = result.thenCompose( composition ->
            {
                if ( composition != null )
                {
                    return completedFuture( composition );
                }
                else
                {
                    return lookupOnRouter( address, routingTable, connectionPool, bookmark, baseError )
                            .whenComplete( ( ignore, error ) -> seenServers.add( address ) );
                }
            } );
        }
        return result;
    }

    private CompletionStage<ClusterComposition> lookupOnInitialRouter( RoutingTable routingTable, ConnectionPool connectionPool, Set<BoltServerAddress> seenServers, Bookmark bookmark,
            Throwable baseError )
    {
        List<BoltServerAddress> addresses;
        try
        {
            addresses = resolve();
        }
        catch ( Throwable error )
        {
            return failedFuture( error );
        }
        addresses.removeAll( seenServers );

        CompletableFuture<ClusterComposition> result = completedWithNull();
        for ( BoltServerAddress address : addresses )
        {
            result = result.thenCompose( composition ->
            {
                if ( composition != null )
                {
                    return completedFuture( composition );
                }
                return lookupOnRouter( address, routingTable, connectionPool, bookmark, baseError );
            } );
        }
        return result;
    }

    private CompletionStage<ClusterComposition> lookupOnRouter( BoltServerAddress routerAddress,
            RoutingTable routingTable, ConnectionPool connectionPool, Bookmark bookmark, Throwable baseError )
    {
        CompletionStage<Connection> connectionStage = connectionPool.acquire( routerAddress );

        return connectionStage
                .thenCompose( connection -> provider.getClusterComposition( connection, routingTable.database(), bookmark ) )
                .handle( ( response, error ) -> {
                    Throwable cause = Futures.completionExceptionCause( error );
                    if ( cause != null )
                    {
                        return handleRoutingProcedureError( cause, routingTable, routerAddress, baseError );
                    }
                    else
                    {
                        return response;
                    }
                } );
    }

    private ClusterComposition handleRoutingProcedureError( Throwable error, RoutingTable routingTable,
            BoltServerAddress routerAddress, Throwable baseError )
    {
        if ( error instanceof SecurityException || error instanceof FatalDiscoveryException )
        {
            // auth error or routing error happened, terminate the discovery procedure immediately
            throw new CompletionException( error );
        }

        // Retriable error happened during discovery.
        DiscoveryException discoveryError = new DiscoveryException( format( RECOVERABLE_ROUTING_ERROR, routerAddress ), error );
        Futures.combineErrors( baseError, discoveryError ); // we record each failure here
        logger.warn( format( "Received a recoverable discovery error with server '%s', will continue discovery with other routing servers if available.",
                routerAddress ), discoveryError );
        routingTable.forget( routerAddress );
        return null;
    }

    @Override
    public List<BoltServerAddress> resolve()
    {
        return resolver.resolve( initialRouter )
                       .stream()
                       .map( BoltServerAddress::from )
                       .collect( toList() ); // collect to list to preserve the order
    }

    private Stream<BoltServerAddress> resolveAll( BoltServerAddress address )
    {
        try
        {
            return address.resolveAll().stream();
        }
        catch ( UnknownHostException e )
        {
            logger.error( "Failed to resolve address `" + address + "` to IPs due to error: " + e.getMessage(), e );
            return Stream.of( address );
        }
    }
}
