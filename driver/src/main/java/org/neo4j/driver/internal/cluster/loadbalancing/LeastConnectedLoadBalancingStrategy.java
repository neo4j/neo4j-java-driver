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

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;

/**
 * Load balancing strategy that finds server with least amount of active (checked out of the pool) connections from
 * given readers or writers. It finds a start index for iteration in a round-robin fashion. This is done to prevent
 * choosing same first address over and over when all addresses have same amount of active connections.
 */
public class LeastConnectedLoadBalancingStrategy implements LoadBalancingStrategy
{
    private static final String LOGGER_NAME = LeastConnectedLoadBalancingStrategy.class.getSimpleName();

    private final RoundRobinArrayIndex readersIndex = new RoundRobinArrayIndex();
    private final RoundRobinArrayIndex writersIndex = new RoundRobinArrayIndex();

    private final ConnectionPool connectionPool;
    private final Logger log;

    public LeastConnectedLoadBalancingStrategy( ConnectionPool connectionPool, Logging logging )
    {
        this.connectionPool = connectionPool;
        this.log = logging.getLog( LOGGER_NAME );
    }

    @Override
    public BoltServerAddress selectReader( BoltServerAddress[] knownReaders )
    {
        return select( knownReaders, readersIndex, "reader" );
    }

    @Override
    public BoltServerAddress selectWriter( BoltServerAddress[] knownWriters )
    {
        return select( knownWriters, writersIndex, "writer" );
    }

    private BoltServerAddress select( BoltServerAddress[] addresses, RoundRobinArrayIndex addressesIndex,
            String addressType )
    {
        int size = addresses.length;
        if ( size == 0 )
        {
            log.trace( "Unable to select %s, no known addresses given", addressType );
            return null;
        }

        // choose start index for iteration in round-robin fashion
        int startIndex = addressesIndex.next( size );
        int index = startIndex;

        BoltServerAddress leastConnectedAddress = null;
        int leastActiveConnections = Integer.MAX_VALUE;

        // iterate over the array to find least connected address
        do
        {
            BoltServerAddress address = addresses[index];
            int activeConnections = connectionPool.inUseConnections( address );

            if ( activeConnections < leastActiveConnections )
            {
                leastConnectedAddress = address;
                leastActiveConnections = activeConnections;
            }

            // loop over to the start of the array when end is reached
            if ( index == size - 1 )
            {
                index = 0;
            }
            else
            {
                index++;
            }
        }
        while ( index != startIndex );

        log.trace( "Selected %s with address: '%s' and active connections: %s",
                addressType, leastConnectedAddress, leastActiveConnections );

        return leastConnectedAddress;
    }
}
