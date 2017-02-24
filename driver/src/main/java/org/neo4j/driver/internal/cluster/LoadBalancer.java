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

import java.util.Set;

import org.neo4j.driver.internal.RoutingErrorHandler;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.spi.ConnectionProvider;
import org.neo4j.driver.internal.spi.PooledConnection;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.exceptions.ProtocolException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;

import static java.lang.String.format;

public class LoadBalancer implements ConnectionProvider, RoutingErrorHandler, AutoCloseable
{
    private static final String LOAD_BALANCER_LOG_NAME = "LoadBalancer";

    private final ConnectionPool connections;
    private final RoutingTable routingTable;
    private final Rediscovery rediscovery;
    private final Logger log;

    public LoadBalancer( RoutingSettings settings, ConnectionPool connections, Clock clock, Logging logging,
            BoltServerAddress... routingAddresses )
    {
        this( settings, new ClusterRoutingTable( clock, routingAddresses ), connections, clock,
                logging.getLog( LOAD_BALANCER_LOG_NAME ) );
    }

    private LoadBalancer( RoutingSettings settings, RoutingTable routingTable, ConnectionPool connections,
            Clock clock, Logger log )
    {
        this( routingTable, connections, new Rediscovery( settings, clock, log,
                new GetServersProcedureClusterCompositionProvider( clock, log ) ), log );
    }

    LoadBalancer( RoutingTable routingTable, ConnectionPool connections, Rediscovery rediscovery, Logger log )
    {
        this.log = log;
        this.connections = connections;
        this.routingTable = routingTable;
        this.rediscovery = rediscovery;

        // initialize the routing table
        ensureRouting();
    }

    @Override
    public PooledConnection acquireConnection( AccessMode mode )
    {
        RoundRobinAddressSet addressSet = addressSetFor( mode );
        PooledConnection connection = acquireConnection( addressSet );
        return new RoutingPooledConnection( connection, this, mode );
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

    private PooledConnection acquireConnection( RoundRobinAddressSet servers ) throws ServiceUnavailableException
    {
        for ( ; ; )
        {
            // refresh the routing table if needed
            ensureRouting();
            for ( BoltServerAddress address; (address = servers.next()) != null; )
            {
                try
                {
                    return connections.acquire( address );
                }
                catch ( ServiceUnavailableException e )
                {
                    log.error( format( "Failed to refresh routing information using routing address %s",
                            address ), e );

                    forget( address );
                }
            }
            // if we get here, we failed to connect to any server, so we will rebuild the routing table
        }
    }

    private synchronized void forget( BoltServerAddress address )
    {
        // First remove from the load balancer, to prevent concurrent threads from making connections to them.
        routingTable.forget( address );
        // drop all current connections to the address
        connections.purge( address );
    }

    synchronized void ensureRouting() throws ServiceUnavailableException, ProtocolException
    {
        if ( routingTable.isStale() )
        {
            log.info( "Routing information is stale. %s", routingTable );
            try
            {
                // get a new routing table
                ClusterComposition cluster = rediscovery.lookupRoutingTable( connections, routingTable );
                Set<BoltServerAddress> removed = routingTable.update( cluster );
                // purge connections to removed addresses
                for ( BoltServerAddress address : removed )
                {
                    connections.purge( address );
                }

                log.info( "Refreshed routing information. %s", routingTable );
            }
            catch ( InterruptedException e )
            {
                throw new ServiceUnavailableException( "Thread was interrupted while establishing connection.", e );
            }
        }
    }

    private RoundRobinAddressSet addressSetFor( AccessMode mode )
    {
        switch ( mode )
        {
        case READ:
            return routingTable.readers();
        case WRITE:
            return routingTable.writers();
        default:
            throw new IllegalArgumentException( "Mode '" + mode + "' is not supported" );
        }
    }
}
