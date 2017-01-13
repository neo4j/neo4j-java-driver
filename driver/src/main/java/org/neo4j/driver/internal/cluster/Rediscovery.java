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

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;

import static java.lang.String.format;

public class Rediscovery
{
    private static final String NO_ROUTERS_AVAILABLE = "Could not perform discovery. No routing servers available.";

    // Given a old routing table and connection pool, use the connection composition provider to obtain a new
    // cluster composition, which would be used to update the routing table and connection pool
    public static ClusterComposition lookupRoutingTable(
            RoutingSettings settings, Clock clock, Logger log,
            ConnectionPool connections, RoutingTable routingTable,
            ClusterComposition.Provider provider )
            throws InterruptedException, ServiceUnavailableException
    {
        int size = routingTable.routerSize(), failures = 0;
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
                BoltServerAddress address = routingTable.nextRouter();
                if ( address == null )
                {
                    throw new ServiceUnavailableException( NO_ROUTERS_AVAILABLE );
                }
                ClusterComposition cluster;
                try ( Connection connection = connections.acquire( address ) )
                {
                    cluster = provider.getClusterComposition( connection );
                    log.info( "Got cluster composition %s", cluster );
                }
                catch ( Exception e )
                {
                    log.error( format( "Failed to connect to routing server '%s'.", address ), e );
                    continue;
                }
                if ( cluster == null || !cluster.isValid() )
                {
                    log.info(
                            "Server <%s> unable to perform routing capability, dropping from list of routers.",
                            address );
                    routingTable.removeRouter( address );
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
            if ( ++failures >= settings.maxRoutingFailures )
            {
                throw new ServiceUnavailableException( NO_ROUTERS_AVAILABLE );
            }
        }
    }
}
