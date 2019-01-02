/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.junit.jupiter.api.Test;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Logging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.cluster.ClusterCompositionUtil.A;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;

class RoundRobinLoadBalancingStrategyTest
{
    private final RoundRobinLoadBalancingStrategy strategy = new RoundRobinLoadBalancingStrategy( DEV_NULL_LOGGING );

    @Test
    void shouldHandleEmptyReadersArray()
    {
        assertNull( strategy.selectReader( new BoltServerAddress[0] ) );
    }

    @Test
    void shouldHandleEmptyWritersArray()
    {
        assertNull( strategy.selectWriter( new BoltServerAddress[0] ) );
    }

    @Test
    void shouldHandleSingleReader()
    {
        BoltServerAddress address = new BoltServerAddress( "reader", 9999 );

        assertEquals( address, strategy.selectReader( new BoltServerAddress[]{address} ) );
    }

    @Test
    void shouldHandleSingleWriter()
    {
        BoltServerAddress address = new BoltServerAddress( "writer", 9999 );

        assertEquals( address, strategy.selectWriter( new BoltServerAddress[]{address} ) );
    }

    @Test
    void shouldReturnReadersInRoundRobinOrder()
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
    void shouldReturnWriterInRoundRobinOrder()
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

    @Test
    void shouldTraceLogWhenNoAddressSelected()
    {
        Logging logging = mock( Logging.class );
        Logger logger = mock( Logger.class );
        when( logging.getLog( anyString() ) ).thenReturn( logger );

        RoundRobinLoadBalancingStrategy strategy = new RoundRobinLoadBalancingStrategy( logging );

        strategy.selectReader( new BoltServerAddress[0] );
        strategy.selectWriter( new BoltServerAddress[0] );

        verify( logger ).trace( startsWith( "Unable to select" ), eq( "reader" ) );
        verify( logger ).trace( startsWith( "Unable to select" ), eq( "writer" ) );
    }

    @Test
    void shouldTraceLogSelectedAddress()
    {
        Logging logging = mock( Logging.class );
        Logger logger = mock( Logger.class );
        when( logging.getLog( anyString() ) ).thenReturn( logger );

        RoundRobinLoadBalancingStrategy strategy = new RoundRobinLoadBalancingStrategy( logging );

        strategy.selectReader( new BoltServerAddress[]{A} );
        strategy.selectWriter( new BoltServerAddress[]{A} );

        verify( logger ).trace( startsWith( "Selected" ), eq( "reader" ), eq( A ) );
        verify( logger ).trace( startsWith( "Selected" ), eq( "writer" ), eq( A ) );
    }
}
