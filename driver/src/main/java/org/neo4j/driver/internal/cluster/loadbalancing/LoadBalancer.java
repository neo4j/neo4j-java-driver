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

import java.util.Set;

import org.neo4j.driver.internal.RoutingErrorHandler;
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
import org.neo4j.driver.internal.netty.AsyncConnection;
import org.neo4j.driver.internal.netty.EventLoopAwareFuture;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.spi.ConnectionProvider;
import org.neo4j.driver.internal.spi.PooledConnection;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;

public class LoadBalancer implements ConnectionProvider, RoutingErrorHandler, AutoCloseable
{
    private static final String LOAD_BALANCER_LOG_NAME = "LoadBalancer";

    private final ConnectionPool connections;
    private final RoutingTable routingTable;
    private final Rediscovery rediscovery;
    private final LoadBalancingStrategy loadBalancingStrategy;
    private final Logger log;

    public LoadBalancer( BoltServerAddress initialRouter, RoutingSettings settings, ConnectionPool connections,
            Clock clock, Logging logging, LoadBalancingStrategy loadBalancingStrategy )
    {
        this( connections, new ClusterRoutingTable( clock, initialRouter ),
                createRediscovery( initialRouter, settings, clock, logging ), loadBalancerLogger( logging ),
                loadBalancingStrategy );
    }

    // Used only in testing
    public LoadBalancer( ConnectionPool connections, RoutingTable routingTable, Rediscovery rediscovery,
            Logging logging )
    {
        this( connections, routingTable, rediscovery, loadBalancerLogger( logging ),
                new LeastConnectedLoadBalancingStrategy( connections, logging ) );
    }

    private LoadBalancer( ConnectionPool connections, RoutingTable routingTable, Rediscovery rediscovery, Logger log,
            LoadBalancingStrategy loadBalancingStrategy )
    {
        this.connections = connections;
        this.routingTable = routingTable;
        this.rediscovery = rediscovery;
        this.loadBalancingStrategy = loadBalancingStrategy;
        this.log = log;

        refreshRoutingTable();
    }

    @Override
    public PooledConnection acquireConnection( AccessMode mode )
    {
        AddressSet addressSet = addressSetFor( mode );
        PooledConnection connection = acquireConnection( mode, addressSet );
        return new RoutingPooledConnection( connection, this, mode );
    }

    @Override
    public EventLoopAwareFuture<AsyncConnection> acquireAsyncConnection( AccessMode mode )
    {
        throw new UnsupportedOperationException();
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
    public void close() throws Exception
    {
        connections.close();
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
        connections.purge( address );
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

    private AddressSet addressSetFor( AccessMode mode )
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

    private static Rediscovery createRediscovery( BoltServerAddress initialRouter, RoutingSettings settings,
            Clock clock, Logging logging )
    {
        Logger log = loadBalancerLogger( logging );
        ClusterCompositionProvider clusterComposition =
                new RoutingProcedureClusterCompositionProvider( clock, log, settings );
        return new Rediscovery( initialRouter, settings, clock, log, clusterComposition, new DnsResolver( log ) );
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
