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
package org.neo4j.driver.internal.cluster.loadbalancing;

import io.netty.util.concurrent.EventExecutorGroup;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.exceptions.SecurityException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.async.ConnectionContext;
import org.neo4j.driver.internal.async.connection.RoutingConnection;
import org.neo4j.driver.internal.cluster.AddressSet;
import org.neo4j.driver.internal.cluster.ClusterCompositionProvider;
import org.neo4j.driver.internal.cluster.Rediscovery;
import org.neo4j.driver.internal.cluster.RediscoveryImpl;
import org.neo4j.driver.internal.cluster.RoutingProcedureClusterCompositionProvider;
import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.cluster.RoutingTable;
import org.neo4j.driver.internal.cluster.RoutingTableRegistry;
import org.neo4j.driver.internal.cluster.RoutingTableRegistryImpl;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.spi.ConnectionProvider;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.net.ServerAddressResolver;

import static java.lang.String.format;
import static org.neo4j.driver.internal.async.ImmutableConnectionContext.simple;
import static org.neo4j.driver.internal.messaging.request.MultiDatabaseUtil.supportsMultiDatabase;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.internal.util.Futures.completionExceptionCause;
import static org.neo4j.driver.internal.util.Futures.failedFuture;
import static org.neo4j.driver.internal.util.Futures.onErrorContinue;

public class LoadBalancer implements ConnectionProvider
{
    private static final String LOAD_BALANCER_LOG_NAME = "LoadBalancer";
    private final ConnectionPool connectionPool;
    private final RoutingTableRegistry routingTables;
    private final LoadBalancingStrategy loadBalancingStrategy;
    private final EventExecutorGroup eventExecutorGroup;
    private final Logger log;
    private final Rediscovery rediscovery;

    public LoadBalancer( BoltServerAddress initialRouter, RoutingSettings settings, ConnectionPool connectionPool,
            EventExecutorGroup eventExecutorGroup, Clock clock, Logging logging,
            LoadBalancingStrategy loadBalancingStrategy, ServerAddressResolver resolver )
    {
        this( connectionPool, createRediscovery( eventExecutorGroup, initialRouter, resolver, settings, clock, logging ), settings, loadBalancingStrategy,
                eventExecutorGroup, clock, loadBalancerLogger( logging ) );
    }

    private LoadBalancer( ConnectionPool connectionPool, Rediscovery rediscovery, RoutingSettings settings, LoadBalancingStrategy loadBalancingStrategy,
            EventExecutorGroup eventExecutorGroup, Clock clock, Logger log )
    {
        this( connectionPool, createRoutingTables( connectionPool, rediscovery, settings, clock, log ), rediscovery, loadBalancingStrategy, eventExecutorGroup,
                log );
    }

    LoadBalancer( ConnectionPool connectionPool, RoutingTableRegistry routingTables, Rediscovery rediscovery, LoadBalancingStrategy loadBalancingStrategy,
            EventExecutorGroup eventExecutorGroup, Logger log )
    {
        this.connectionPool = connectionPool;
        this.routingTables = routingTables;
        this.rediscovery = rediscovery;
        this.loadBalancingStrategy = loadBalancingStrategy;
        this.eventExecutorGroup = eventExecutorGroup;
        this.log = log;
    }

    @Override
    public CompletionStage<Connection> acquireConnection( ConnectionContext context )
    {
        return routingTables.ensureRoutingTable( context )
                .thenCompose( handler -> acquire( context.mode(), handler.routingTable() )
                        .thenApply( connection -> new RoutingConnection( connection, context.databaseName(), context.mode(), handler ) ) );
    }

    @Override
    public CompletionStage<Void> verifyConnectivity()
    {
        return this.supportsMultiDb().thenCompose( supports -> routingTables.ensureRoutingTable( simple( supports ) ) ).handle( ( ignored, error ) -> {
            if ( error != null )
            {
                Throwable cause = completionExceptionCause( error );
                if ( cause instanceof ServiceUnavailableException )
                {
                    throw Futures.asCompletionException( new ServiceUnavailableException(
                            "Unable to connect to database management service, ensure the database is running and that there is a working network connection to it.",
                            cause ) );
                }
                throw Futures.asCompletionException( cause );
            }
            return null;
        } );
    }

    @Override
    public CompletionStage<Void> close()
    {
        return connectionPool.close();
    }

    @Override
    public CompletionStage<Boolean> supportsMultiDb()
    {
        List<BoltServerAddress> addresses;

        try
        {
            addresses = rediscovery.resolve();
        }
        catch ( Throwable error )
        {
            return failedFuture( error );
        }
        CompletableFuture<Boolean> result = completedWithNull();
        Throwable baseError = new ServiceUnavailableException( "Failed to perform multi-databases feature detection with the following servers: " + addresses );

        for ( BoltServerAddress address : addresses )
        {
            result = onErrorContinue( result, baseError, completionError -> {
                // We fail fast on security errors
                Throwable error = completionExceptionCause( completionError );
                if ( error instanceof SecurityException )
                {
                    return failedFuture( error );
                }
                return supportsMultiDb( address );
            } );
        }
        return onErrorContinue( result, baseError, completionError -> {
            // If we failed with security errors, then we rethrow the security error out, otherwise we throw the chained errors.
            Throwable error = completionExceptionCause( completionError );
            if ( error instanceof SecurityException )
            {
                return failedFuture( error );
            }
            return failedFuture( baseError );
        } );
    }

    private CompletionStage<Boolean> supportsMultiDb( BoltServerAddress address )
    {
        return connectionPool.acquire( address ).thenCompose( conn -> {
            boolean supportsMultiDatabase = supportsMultiDatabase( conn );
            return conn.release().thenApply( ignored -> supportsMultiDatabase );
        } );
    }

    private CompletionStage<Connection> acquire( AccessMode mode, RoutingTable routingTable )
    {
        AddressSet addresses = addressSet( mode, routingTable );
        CompletableFuture<Connection> result = new CompletableFuture<>();
        acquire( mode, routingTable, addresses, result );
        return result;
    }

    private void acquire( AccessMode mode, RoutingTable routingTable, AddressSet addresses, CompletableFuture<Connection> result )
    {
        BoltServerAddress address = selectAddress( mode, addresses );

        if ( address == null )
        {
            result.completeExceptionally( new SessionExpiredException(
                    "Failed to obtain connection towards " + mode + " server. " +
                    "Known routing table is: " + routingTable ) );
            return;
        }

        connectionPool.acquire( address ).whenComplete( ( connection, completionError ) ->
        {
            Throwable error = completionExceptionCause( completionError );
            if ( error != null )
            {
                if ( error instanceof ServiceUnavailableException )
                {
                    SessionExpiredException errorToLog = new SessionExpiredException( format( "Server at %s is no longer available", address ), error );
                    log.warn( "Failed to obtain a connection towards address " + address, errorToLog );
                    routingTable.forget( address );
                    eventExecutorGroup.next().execute( () -> acquire( mode, routingTable, addresses, result ) );
                }
                else
                {
                    result.completeExceptionally( error );
                }
            }
            else
            {
                result.complete( connection );
            }
        } );
    }

    private static AddressSet addressSet( AccessMode mode, RoutingTable routingTable )
    {
        switch ( mode )
        {
        case READ:
            return routingTable.readers();
        case WRITE:
            return routingTable.writers();
        default:
            throw unknownMode( mode );
        }
    }

    private BoltServerAddress selectAddress( AccessMode mode, AddressSet servers )
    {
        BoltServerAddress[] addresses = servers.toArray();

        switch ( mode )
        {
        case READ:
            return loadBalancingStrategy.selectReader( addresses );
        case WRITE:
            return loadBalancingStrategy.selectWriter( addresses );
        default:
            throw unknownMode( mode );
        }
    }

    private static RoutingTableRegistry createRoutingTables( ConnectionPool connectionPool, Rediscovery rediscovery, RoutingSettings settings, Clock clock,
            Logger log )
    {
        return new RoutingTableRegistryImpl( connectionPool, rediscovery, clock, log, settings.routingTablePurgeDelayMs() );
    }

    private static Rediscovery createRediscovery( EventExecutorGroup eventExecutorGroup, BoltServerAddress initialRouter, ServerAddressResolver resolver,
            RoutingSettings settings, Clock clock, Logging logging )
    {
        Logger log = loadBalancerLogger( logging );
        ClusterCompositionProvider clusterCompositionProvider = new RoutingProcedureClusterCompositionProvider( clock, settings.routingContext() );
        return new RediscoveryImpl( initialRouter, settings, clusterCompositionProvider, eventExecutorGroup, resolver, log );
    }

    private static Logger loadBalancerLogger( Logging logging )
    {
        return logging.getLog( LOAD_BALANCER_LOG_NAME );
    }

    private static RuntimeException unknownMode( AccessMode mode )
    {
        return new IllegalArgumentException( "Mode '" + mode + "' is not supported" );
    }
}
