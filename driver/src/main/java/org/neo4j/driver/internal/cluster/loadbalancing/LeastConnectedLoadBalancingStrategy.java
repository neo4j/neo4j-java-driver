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

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.spi.ConnectionPool;

public class LeastConnectedLoadBalancingStrategy implements LoadBalancingStrategy
{
    private final RoundRobinArrayIndex readersIndex = new RoundRobinArrayIndex();
    private final RoundRobinArrayIndex writersIndex = new RoundRobinArrayIndex();

    private final ConnectionPool connectionPool;

    public LeastConnectedLoadBalancingStrategy( ConnectionPool connectionPool )
    {
        this.connectionPool = connectionPool;
    }

    @Override
    public BoltServerAddress selectReader( BoltServerAddress[] knownReaders )
    {
        return select( knownReaders, readersIndex );
    }

    @Override
    public BoltServerAddress selectWriter( BoltServerAddress[] knownWriters )
    {
        return select( knownWriters, writersIndex );
    }

    private BoltServerAddress select( BoltServerAddress[] addresses, RoundRobinArrayIndex addressesIndex )
    {
        int size = addresses.length;
        if ( size == 0 )
        {
            return null;
        }

        int startIndex = addressesIndex.next( size );
        int index = startIndex;

        BoltServerAddress leastConnectedAddress = null;
        int leastActiveConnections = Integer.MAX_VALUE;

        do
        {
            BoltServerAddress address = addresses[index];
            int activeConnections = connectionPool.activeConnections( address );

            if ( activeConnections < leastActiveConnections )
            {
                leastConnectedAddress = address;
                leastActiveConnections = activeConnections;
            }

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

        return leastConnectedAddress;
    }
}
