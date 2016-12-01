/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.util.HashSet;

import org.neo4j.driver.internal.RoutingErrorHandler;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;

import static java.util.Arrays.asList;

public final class LoadBalancer implements RoutingErrorHandler, AutoCloseable
{
    private static final int MIN_ROUTERS = 1;
    private static final String NO_ROUTERS_AVAILABLE = "Could not perform discovery. No routing servers available.";
    // dependencies
    private final RoutingSettings settings;
    private final Clock clock;
    private final Logger log;
    private final ConnectionPool connections;
    private final ClusterComposition.Provider provider;
    // state
    private long expirationTimeout;
    private final RoundRobinAddressSet readers, writers, routers;

    public LoadBalancer(
            RoutingSettings settings,
            Clock clock,
            Logger log,
            ConnectionPool connections,
            BoltServerAddress... routingAddresses ) throws ServiceUnavailableException
    {
        this( settings, clock, log, connections, new ClusterComposition.Provider.Default( clock ), routingAddresses );
    }

    LoadBalancer(
            RoutingSettings settings,
            Clock clock,
            Logger log,
            ConnectionPool connections,
            ClusterComposition.Provider provider,
            BoltServerAddress... routingAddresses ) throws ServiceUnavailableException
    {
        this.clock = clock;
        this.log = log;
        this.connections = connections;
        this.expirationTimeout = clock.millis() - 1;
        this.provider = provider;
        this.settings = settings;
        this.readers = new RoundRobinAddressSet();
        this.writers = new RoundRobinAddressSet();
        this.routers = new RoundRobinAddressSet();
        routers.update( new HashSet<>( asList( routingAddresses ) ), new HashSet<BoltServerAddress>() );
        // initialize the routing table
        ensureRouting();
    }

    public Connection acquireReadConnection() throws ServiceUnavailableException
    {
        return acquireConnection( readers );
    }

    public Connection acquireWriteConnection() throws ServiceUnavailableException
    {
        return acquireConnection( writers );
    }

    @Override
    public void onConnectionFailure( BoltServerAddress address )
    {
        forget( address );
    }

    @Override
    public void onWriteFailure( BoltServerAddress address )
    {
        writers.remove( address );
    }

    @Override
    public void close() throws Exception
    {
        connections.close();
    }

    private Connection acquireConnection( RoundRobinAddressSet servers ) throws ServiceUnavailableException
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
                    forget( address );
                }
            }
            // if we get here, we failed to connect to any server, so we will rebuild the routing table
        }
    }

    private synchronized void ensureRouting() throws ServiceUnavailableException
    {
        if ( stale() )
        {
            try
            {
                // get a new routing table
                ClusterComposition cluster = lookupRoutingTable();
                expirationTimeout = cluster.expirationTimestamp;
                HashSet<BoltServerAddress> removed = new HashSet<>();
                readers.update( cluster.readers(), removed );
                writers.update( cluster.writers(), removed );
                routers.update( cluster.routers(), removed );
                // purge connections to removed addresses
                for ( BoltServerAddress address : removed )
                {
                    connections.purge( address );
                }
            }
            catch ( InterruptedException e )
            {
                throw new ServiceUnavailableException( "Thread was interrupted while establishing connection.", e );
            }
        }
    }

    private ClusterComposition lookupRoutingTable() throws InterruptedException, ServiceUnavailableException
    {
        int size = routers.size(), failures = 0;
        if ( size == 0 )
        {
            throw new ServiceUnavailableException( NO_ROUTERS_AVAILABLE );
        }
        for ( long start = clock.millis(), delay = 0; ; delay = Math.max( settings.retryTimeoutDelay, delay * 2 ) )
        {
            long waitTime = start + delay - clock.millis();
            if ( waitTime > 0 )
            {
                clock.sleep( waitTime );
            }
            start = clock.millis();
            for ( int i = 0; i < size; i++ )
            {
                BoltServerAddress address = routers.next();
                if ( address == null )
                {
                    throw new ServiceUnavailableException( NO_ROUTERS_AVAILABLE );
                }
                ClusterComposition cluster;
                try ( Connection connection = connections.acquire( address ) )
                {
                    cluster = provider.getClusterComposition( connection );
                }
                catch ( Exception e )
                {
                    log.error( String.format( "Failed to connect to routing server '%s'.", address ), e );
                    continue;
                }
                if ( cluster == null || !cluster.isValid() )
                {
                    log.info(
                            "Server <%s> unable to perform routing capability, dropping from list of routers.",
                            address );
                    routers.remove( address );
                    if ( --size == 0 )
                    {
                        throw new ServiceUnavailableException( NO_ROUTERS_AVAILABLE );
                    }
                }
                else
                {
                    return cluster;
                }
            }
            if ( ++failures > settings.maxRoutingFailures )
            {
                throw new ServiceUnavailableException( NO_ROUTERS_AVAILABLE );
            }
        }
    }

    private synchronized void forget( BoltServerAddress address )
    {
        // First remove from the load balancer, to prevent concurrent threads from making connections to them.
        // Don't remove it from the set of routers, since that might mean we lose our ability to re-discover,
        // just remove it from the set of readers and writers, so that we don't use it for actual work without
        // performing discovery first.
        readers.remove( address );
        writers.remove( address );
        // drop all current connections to the address
        connections.purge( address );
    }

    private boolean stale()
    {
        return expirationTimeout < clock.millis() || // the expiration timeout has been reached
                routers.size() <= MIN_ROUTERS || // we need to discover more routing servers
                readers.size() == 0 || // we need to discover more read servers
                writers.size() == 0; // we need to discover more write servers
    }
}
