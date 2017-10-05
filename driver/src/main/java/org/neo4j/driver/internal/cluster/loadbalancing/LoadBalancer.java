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
package org.neo4j.driver.internal.cluster.loadbalancing;

import io.netty.util.concurrent.EventExecutorGroup;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.internal.RoutingErrorHandler;
import org.neo4j.driver.internal.async.AsyncConnection;
import org.neo4j.driver.internal.async.RoutingAsyncConnection;
import org.neo4j.driver.internal.async.pool.AsyncConnectionPool;
import org.neo4j.driver.internal.cluster.AddressSet;
import org.neo4j.driver.internal.cluster.ClusterComposition;
import org.neo4j.driver.internal.cluster.ClusterCompositionProvider;
import org.neo4j.driver.internal.cluster.ClusterRoutingTable;
import org.neo4j.driver.internal.cluster.DnsResolver;
import org.neo4j.driver.internal.cluster.Rediscovery;
import org.neo4j.driver.internal.cluster.RoutingPooledConnection;
import org.neo4j.driver.internal.cluster.RoutingProcedureClusterCompositionProvider;
import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.cluster.RoutingTable;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.spi.ConnectionProvider;
import org.neo4j.driver.internal.spi.PooledConnection;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class LoadBalancer implements ConnectionProvider, RoutingErrorHandler
{
    private static final String LOAD_BALANCER_LOG_NAME = "LoadBalancer";

    private final ConnectionPool connections;
    private final AsyncConnectionPool asyncConnectionPool;
    private final RoutingTable routingTable;
    private final Rediscovery rediscovery;
    private final LoadBalancingStrategy loadBalancingStrategy;
    private final EventExecutorGroup eventExecutorGroup;
    private final Logger log;

    private CompletableFuture<RoutingTable> refreshRoutingTableFuture;

    public LoadBalancer( BoltServerAddress initialRouter, RoutingSettings settings, ConnectionPool connections,
            AsyncConnectionPool asyncConnectionPool, EventExecutorGroup eventExecutorGroup, Clock clock,
            Logging logging, LoadBalancingStrategy loadBalancingStrategy )
    {
        this( connections, asyncConnectionPool, new ClusterRoutingTable( clock, initialRouter ),
                createRediscovery( initialRouter, settings, eventExecutorGroup, clock, logging ),
                loadBalancerLogger( logging ), loadBalancingStrategy, eventExecutorGroup );
    }

    // Used only in testing
    public LoadBalancer( ConnectionPool connections, AsyncConnectionPool asyncConnectionPool,
            RoutingTable routingTable, Rediscovery rediscovery, EventExecutorGroup eventExecutorGroup, Logging logging )
    {
        this( connections, asyncConnectionPool, routingTable, rediscovery, loadBalancerLogger( logging ),
                new LeastConnectedLoadBalancingStrategy( connections, asyncConnectionPool, logging ),
                eventExecutorGroup );
    }

    private LoadBalancer( ConnectionPool connections, AsyncConnectionPool asyncConnectionPool,
            RoutingTable routingTable, Rediscovery rediscovery, Logger log,
            LoadBalancingStrategy loadBalancingStrategy, EventExecutorGroup eventExecutorGroup )
    {
        this.connections = connections;
        this.asyncConnectionPool = asyncConnectionPool;
        this.routingTable = routingTable;
        this.rediscovery = rediscovery;
        this.loadBalancingStrategy = loadBalancingStrategy;
        this.eventExecutorGroup = eventExecutorGroup;
        this.log = log;

        if ( connections != null )
        {
            refreshRoutingTable();
        }
    }

    @Override
    public PooledConnection acquireConnection( AccessMode mode )
    {
        AddressSet addressSet = addressSet( mode, routingTable );
        PooledConnection connection = acquireConnection( mode, addressSet );
        return new RoutingPooledConnection( connection, this, mode );
    }

    @Override
    public CompletionStage<AsyncConnection> acquireAsyncConnection( AccessMode mode )
    {
        return freshRoutingTable( mode )
                .thenCompose( routingTable -> acquireAsync( mode, routingTable ) )
                .thenApply( connection -> new RoutingAsyncConnection( connection, mode, this ) );
    }

    @Override
    public void onConnectionFailure( BoltServerAddress address )
    {
        forget( address );
    }

    @Override
    public void onWriteFailure( BoltServerAddress address )
    {
        routingTable.removeWriter( address );
    }

    @Override
    public CompletionStage<Void> close()
    {
        try
        {
            connections.close();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }

        return asyncConnectionPool.close();
    }

    private PooledConnection acquireConnection( AccessMode mode, AddressSet servers )
    {
        ensureRouting( mode );
        for ( BoltServerAddress address; (address = selectAddress( mode, servers )) != null; )
        {
            try
            {
                return connections.acquire( address );
            }
            catch ( ServiceUnavailableException e )
            {
                log.error( "Failed to obtain a connection towards address " + address, e );
                forget( address );
            }
        }
        throw new SessionExpiredException(
                "Failed to obtain connection towards " + mode + " server. Known routing table is: " + routingTable );
    }

    private synchronized void forget( BoltServerAddress address )
    {
        // First remove from the load balancer, to prevent concurrent threads from making connections to them.
        routingTable.forget( address );
        // drop all current connections to the address
        if ( connections != null )
        {
            connections.purge( address );
        }
        asyncConnectionPool.purge( address );
    }

    synchronized void ensureRouting( AccessMode mode )
    {
        if ( routingTable.isStaleFor( mode ) )
        {
            refreshRoutingTable();
        }
    }

    synchronized void refreshRoutingTable()
    {
        log.info( "Routing information is stale. %s", routingTable );

        // get a new routing table
        ClusterComposition cluster = rediscovery.lookupClusterComposition( routingTable, connections );
        Set<BoltServerAddress> removed = routingTable.update( cluster );
        // purge connections to removed addresses
        for ( BoltServerAddress address : removed )
        {
            connections.purge( address );
        }

        log.info( "Refreshed routing information. %s", routingTable );
    }

    private synchronized CompletionStage<RoutingTable> freshRoutingTable( AccessMode mode )
    {
        if ( refreshRoutingTableFuture != null )
        {
            // refresh is already happening concurrently, just use it's result
            return refreshRoutingTableFuture;
        }
        else if ( routingTable.isStaleFor( mode ) )
        {
            // existing routing table is not fresh and should be updated
            log.info( "Routing information is stale. %s", routingTable );

            CompletableFuture<RoutingTable> resultFuture = new CompletableFuture<>();
            refreshRoutingTableFuture = resultFuture;

            rediscovery.lookupClusterCompositionAsync( routingTable, asyncConnectionPool )
                    .whenComplete( ( composition, error ) ->
                    {
                        if ( error != null )
                        {
                            clusterCompositionLookupFailed( error );
                        }
                        else
                        {
                            freshClusterCompositionFetched( composition );
                        }
                    } );

            return resultFuture;
        }
        else
        {
            // existing routing table is fresh, use it
            return completedFuture( routingTable );
        }
    }

    private synchronized void freshClusterCompositionFetched( ClusterComposition composition )
    {
        Set<BoltServerAddress> removed = routingTable.update( composition );

        for ( BoltServerAddress address : removed )
        {
            asyncConnectionPool.purge( address );
        }

        log.info( "Refreshed routing information. %s", routingTable );

        CompletableFuture<RoutingTable> routingTableFuture = refreshRoutingTableFuture;
        refreshRoutingTableFuture = null;
        routingTableFuture.complete( routingTable );
    }

    private synchronized void clusterCompositionLookupFailed( Throwable error )
    {
        CompletableFuture<RoutingTable> routingTableFuture = refreshRoutingTableFuture;
        refreshRoutingTableFuture = null;
        routingTableFuture.completeExceptionally( error );
    }

    private CompletionStage<AsyncConnection> acquireAsync( AccessMode mode, RoutingTable routingTable )
    {
        AddressSet addresses = addressSet( mode, routingTable );
        CompletableFuture<AsyncConnection> result = new CompletableFuture<>();
        acquireAsync( mode, addresses, result );
        return result;
    }

    private void acquireAsync( AccessMode mode, AddressSet addresses, CompletableFuture<AsyncConnection> result )
    {
        BoltServerAddress address = selectAddressAsync( mode, addresses );

        if ( address == null )
        {
            result.completeExceptionally( new SessionExpiredException(
                    "Failed to obtain connection towards " + mode + " server. " +
                    "Known routing table is: " + routingTable ) );
            return;
        }

        asyncConnectionPool.acquire( address ).whenComplete( ( connection, error ) ->
        {
            if ( error != null )
            {
                if ( error instanceof ServiceUnavailableException )
                {
                    log.error( "Failed to obtain a connection towards address " + address, error );
                    forget( address );
                    eventExecutorGroup.next().execute( () -> acquireAsync( mode, addresses, result ) );
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

    private BoltServerAddress selectAddressAsync( AccessMode mode, AddressSet servers )
    {
        BoltServerAddress[] addresses = servers.toArray();

        switch ( mode )
        {
        case READ:
            return loadBalancingStrategy.selectReaderAsync( addresses );
        case WRITE:
            return loadBalancingStrategy.selectWriterAsync( addresses );
        default:
            throw unknownMode( mode );
        }
    }

    private static Rediscovery createRediscovery( BoltServerAddress initialRouter, RoutingSettings settings,
            EventExecutorGroup eventExecutorGroup, Clock clock, Logging logging )
    {
        Logger log = loadBalancerLogger( logging );
        ClusterCompositionProvider clusterCompositionProvider =
                new RoutingProcedureClusterCompositionProvider( clock, log, settings );
        return new Rediscovery( initialRouter, settings, clusterCompositionProvider, eventExecutorGroup,
                new DnsResolver( log ), clock, log );
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
