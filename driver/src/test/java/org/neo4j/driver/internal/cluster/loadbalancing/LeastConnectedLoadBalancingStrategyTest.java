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

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.spi.ConnectionPool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class LeastConnectedLoadBalancingStrategyTest
{
    @Mock
    private ConnectionPool connectionPool;
    private LeastConnectedLoadBalancingStrategy strategy;

    @Before
    public void setUp() throws Exception
    {
        initMocks( this );
        strategy = new LeastConnectedLoadBalancingStrategy( connectionPool );
    }

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
    public void shouldHandleSingleReaderWithoutActiveConnections()
    {
        BoltServerAddress address = new BoltServerAddress( "reader", 9999 );

        assertEquals( address, strategy.selectReader( new BoltServerAddress[]{address} ) );
    }

    @Test
    public void shouldHandleSingleWriterWithoutActiveConnections()
    {
        BoltServerAddress address = new BoltServerAddress( "writer", 9999 );

        assertEquals( address, strategy.selectWriter( new BoltServerAddress[]{address} ) );
    }

    @Test
    public void shouldHandleSingleReaderWithActiveConnections()
    {
        BoltServerAddress address = new BoltServerAddress( "reader", 9999 );
        when( connectionPool.activeConnections( address ) ).thenReturn( 42 );

        assertEquals( address, strategy.selectReader( new BoltServerAddress[]{address} ) );
    }

    @Test
    public void shouldHandleSingleWriterWithActiveConnections()
    {
        BoltServerAddress address = new BoltServerAddress( "writer", 9999 );
        when( connectionPool.activeConnections( address ) ).thenReturn( 24 );

        assertEquals( address, strategy.selectWriter( new BoltServerAddress[]{address} ) );
    }

    @Test
    public void shouldHandleMultipleReadersWithActiveConnections()
    {
        BoltServerAddress address1 = new BoltServerAddress( "reader", 1 );
        BoltServerAddress address2 = new BoltServerAddress( "reader", 2 );
        BoltServerAddress address3 = new BoltServerAddress( "reader", 3 );

        when( connectionPool.activeConnections( address1 ) ).thenReturn( 3 );
        when( connectionPool.activeConnections( address2 ) ).thenReturn( 4 );
        when( connectionPool.activeConnections( address3 ) ).thenReturn( 1 );

        assertEquals( address3, strategy.selectReader( new BoltServerAddress[]{address1, address2, address3} ) );
    }

    @Test
    public void shouldHandleMultipleWritersWithActiveConnections()
    {
        BoltServerAddress address1 = new BoltServerAddress( "writer", 1 );
        BoltServerAddress address2 = new BoltServerAddress( "writer", 2 );
        BoltServerAddress address3 = new BoltServerAddress( "writer", 3 );
        BoltServerAddress address4 = new BoltServerAddress( "writer", 4 );

        when( connectionPool.activeConnections( address1 ) ).thenReturn( 5 );
        when( connectionPool.activeConnections( address2 ) ).thenReturn( 6 );
        when( connectionPool.activeConnections( address3 ) ).thenReturn( 0 );
        when( connectionPool.activeConnections( address4 ) ).thenReturn( 1 );

        assertEquals( address3,
                strategy.selectWriter( new BoltServerAddress[]{address1, address2, address3, address4} ) );
    }

    @Test
    public void shouldReturnDifferentReaderOnEveryInvocationWhenNoActiveConnections()
    {
        BoltServerAddress address1 = new BoltServerAddress( "reader", 1 );
        BoltServerAddress address2 = new BoltServerAddress( "reader", 2 );
        BoltServerAddress address3 = new BoltServerAddress( "reader", 3 );

        assertEquals( address1, strategy.selectReader( new BoltServerAddress[]{address1, address2, address3} ) );
        assertEquals( address2, strategy.selectReader( new BoltServerAddress[]{address1, address2, address3} ) );
        assertEquals( address3, strategy.selectReader( new BoltServerAddress[]{address1, address2, address3} ) );

        assertEquals( address1, strategy.selectReader( new BoltServerAddress[]{address1, address2, address3} ) );
        assertEquals( address2, strategy.selectReader( new BoltServerAddress[]{address1, address2, address3} ) );
        assertEquals( address3, strategy.selectReader( new BoltServerAddress[]{address1, address2, address3} ) );
    }

    @Test
    public void shouldReturnDifferentWriterOnEveryInvocationWhenNoActiveConnections()
    {
        BoltServerAddress address1 = new BoltServerAddress( "writer", 1 );
        BoltServerAddress address2 = new BoltServerAddress( "writer", 2 );

        assertEquals( address1, strategy.selectReader( new BoltServerAddress[]{address1, address2} ) );
        assertEquals( address2, strategy.selectReader( new BoltServerAddress[]{address1, address2} ) );

        assertEquals( address1, strategy.selectReader( new BoltServerAddress[]{address1, address2} ) );
        assertEquals( address2, strategy.selectReader( new BoltServerAddress[]{address1, address2} ) );
    }
}
