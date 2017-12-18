/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.exceptions.SecurityException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;

import static java.lang.String.format;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;

public class Rediscovery
{
    private static final String NO_ROUTERS_AVAILABLE = "Could not perform discovery. No routing servers available.";

    private final BoltServerAddress initialRouter;
    private final RoutingSettings settings;
    private final Logger logger;
    private final ClusterCompositionProvider provider;
    private final HostNameResolver hostNameResolver;
    private final EventExecutorGroup eventExecutorGroup;

    private volatile boolean useInitialRouter;

    public Rediscovery( BoltServerAddress initialRouter, RoutingSettings settings, ClusterCompositionProvider provider,
            EventExecutorGroup eventExecutorGroup, HostNameResolver hostNameResolver, Logger logger )
    {
        this( initialRouter, settings, provider, hostNameResolver, eventExecutorGroup, logger, true );
    }

    // Test-only constructor
    public Rediscovery( BoltServerAddress initialRouter, RoutingSettings settings, ClusterCompositionProvider provider,
            HostNameResolver hostNameResolver, EventExecutorGroup eventExecutorGroup, Logger logger,
            boolean useInitialRouter )
    {
        this.initialRouter = initialRouter;
        this.settings = settings;
        this.logger = logger;
        this.provider = provider;
        this.hostNameResolver = hostNameResolver;
        this.eventExecutorGroup = eventExecutorGroup;
        this.useInitialRouter = useInitialRouter;
    }

    /**
     * Given the current routing table and connection pool, use the connection composition provider to fetch a new
     * cluster composition, which would be used to update the routing table and connection pool.
     *
     * @param routingTable current routing table.
     * @param connectionPool connection pool.
     * @return new cluster composition.
     */
    public CompletionStage<ClusterComposition> lookupClusterComposition( RoutingTable routingTable,
            ConnectionPool connectionPool )
    {
        CompletableFuture<ClusterComposition> result = new CompletableFuture<>();
        lookupClusterComposition( routingTable, connectionPool, 0, 0, result );
        return result;
    }

    private void lookupClusterComposition( RoutingTable routingTable, ConnectionPool pool,
            int failures, long previousDelay, CompletableFuture<ClusterComposition> result )
    {
        lookup( routingTable, pool ).whenComplete( ( composition, completionError ) ->
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
                    result.completeExceptionally( new ServiceUnavailableException( NO_ROUTERS_AVAILABLE ) );
                }
                else
                {
                    long nextDelay = Math.max( settings.retryTimeoutDelay(), previousDelay * 2 );
                    logger.info( "Unable to fetch new routing table, will try again in " + nextDelay + "ms" );
                    eventExecutorGroup.next().schedule(
                            () -> lookupClusterComposition( routingTable, pool, newFailures, nextDelay, result ),
                            nextDelay, TimeUnit.MILLISECONDS
                    );
                }
            }
        } );
    }

    private CompletionStage<ClusterComposition> lookup( RoutingTable routingTable, ConnectionPool connectionPool )
    {
        CompletionStage<ClusterComposition> compositionStage;

        if ( useInitialRouter )
        {
            compositionStage = lookupOnInitialRouterThenOnKnownRouters( routingTable, connectionPool );
            useInitialRouter = false;
        }
        else
        {
            compositionStage = lookupOnKnownRoutersThenOnInitialRouter( routingTable, connectionPool );
        }

        return compositionStage.whenComplete( ( composition, error ) ->
        {
            if ( composition != null && !composition.hasWriters() )
            {
                useInitialRouter = true;
            }
        } );
    }

    private CompletionStage<ClusterComposition> lookupOnKnownRoutersThenOnInitialRouter( RoutingTable routingTable,
            ConnectionPool connectionPool )
    {
        Set<BoltServerAddress> seenServers = new HashSet<>();
        return lookupOnKnownRouters( routingTable, connectionPool, seenServers ).thenCompose( composition ->
        {
            if ( composition != null )
            {
                return completedFuture( composition );
            }
            return lookupOnInitialRouter( routingTable, connectionPool, seenServers );
        } );
    }

    private CompletionStage<ClusterComposition> lookupOnInitialRouterThenOnKnownRouters( RoutingTable routingTable,
            ConnectionPool connectionPool )
    {
        Set<BoltServerAddress> seenServers = Collections.emptySet();
        return lookupOnInitialRouter( routingTable, connectionPool, seenServers ).thenCompose( composition ->
        {
            if ( composition != null )
            {
                return completedFuture( composition );
            }
            return lookupOnKnownRouters( routingTable, connectionPool, new HashSet<>() );
        } );
    }

    private CompletionStage<ClusterComposition> lookupOnKnownRouters( RoutingTable routingTable,
            ConnectionPool connectionPool, Set<BoltServerAddress> seenServers )
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
                    return lookupOnRouter( address, routingTable, connectionPool )
                            .whenComplete( ( ignore, error ) -> seenServers.add( address ) );
                }
            } );
        }
        return result;
    }

    private CompletionStage<ClusterComposition> lookupOnInitialRouter( RoutingTable routingTable,
            ConnectionPool connectionPool, Set<BoltServerAddress> seenServers )
    {
        Set<BoltServerAddress> addresses = hostNameResolver.resolve( initialRouter );
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
                return lookupOnRouter( address, routingTable, connectionPool );
            } );
        }
        return result;
    }

    private CompletionStage<ClusterComposition> lookupOnRouter( BoltServerAddress routerAddress,
            RoutingTable routingTable, ConnectionPool connectionPool )
    {
        CompletionStage<Connection> connectionStage = connectionPool.acquire( routerAddress );

        return provider.getClusterComposition( connectionStage ).handle( ( response, error ) ->
        {
            Throwable cause = Futures.completionExceptionCause( error );
            if ( cause != null )
            {
                return handleRoutingProcedureError( cause, routingTable, routerAddress );
            }
            else
            {
                return response.clusterComposition();
            }
        } );
    }

    private ClusterComposition handleRoutingProcedureError( Throwable error, RoutingTable routingTable,
            BoltServerAddress routerAddress )
    {
        if ( error instanceof SecurityException )
        {
            // auth error happened, terminate the discovery procedure immediately
            throw new CompletionException( error );
        }
        else
        {
            // connection turned out to be broken
            logger.error( format( "Failed to connect to routing server '%s'.", routerAddress ), error );
            routingTable.forget( routerAddress );
            return null;
        }
    }

}
