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

import org.junit.Test;

import org.neo4j.driver.internal.net.BoltServerAddress;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class RoundRobinLoadBalancingStrategyTest
{
    private final RoundRobinLoadBalancingStrategy strategy = new RoundRobinLoadBalancingStrategy();

    @Test
    public void shouldHandleEmptyReadersArray()
    {
        assertNull( strategy.selectReader( new BoltServerAddress[0] ) );
    }

    @Test
    public void shouldHandleEmptyWritersArray()
    {
        assertNull( strategy.selectWriter( new BoltServerAddress[0] ) );
    }

    @Test
    public void shouldHandleSingleReader()
    {
        BoltServerAddress address = new BoltServerAddress( "reader", 9999 );

        assertEquals( address, strategy.selectReader( new BoltServerAddress[]{address} ) );
    }

    @Test
    public void shouldHandleSingleWriter()
    {
        BoltServerAddress address = new BoltServerAddress( "writer", 9999 );

        assertEquals( address, strategy.selectWriter( new BoltServerAddress[]{address} ) );
    }

    @Test
    public void shouldReturnReadersInRoundRobinOrder()
    {
        BoltServerAddress address1 = new BoltServerAddress( "server-1", 1 );
        BoltServerAddress address2 = new BoltServerAddress( "server-2", 2 );
        BoltServerAddress address3 = new BoltServerAddress( "server-3", 3 );
        BoltServerAddress address4 = new BoltServerAddress( "server-4", 4 );

        BoltServerAddress[] readers = {address1, address2, address3, address4};

        assertEquals( address1, strategy.selectReader( readers ) );
        assertEquals( address2, strategy.selectReader( readers ) );
        assertEquals( address3, strategy.selectReader( readers ) );
        assertEquals( address4, strategy.selectReader( readers ) );

        assertEquals( address1, strategy.selectReader( readers ) );
        assertEquals( address2, strategy.selectReader( readers ) );
        assertEquals( address3, strategy.selectReader( readers ) );
        assertEquals( address4, strategy.selectReader( readers ) );
    }

    @Test
    public void shouldReturnWriterInRoundRobinOrder()
    {
        BoltServerAddress address1 = new BoltServerAddress( "server-1", 1 );
        BoltServerAddress address2 = new BoltServerAddress( "server-2", 2 );
        BoltServerAddress address3 = new BoltServerAddress( "server-3", 3 );

        BoltServerAddress[] writers = {address1, address2, address3};

        assertEquals( address1, strategy.selectWriter( writers ) );
        assertEquals( address2, strategy.selectWriter( writers ) );
        assertEquals( address3, strategy.selectWriter( writers ) );

        assertEquals( address1, strategy.selectWriter( writers ) );
        assertEquals( address2, strategy.selectWriter( writers ) );
        assertEquals( address3, strategy.selectWriter( writers ) );
    }
}
