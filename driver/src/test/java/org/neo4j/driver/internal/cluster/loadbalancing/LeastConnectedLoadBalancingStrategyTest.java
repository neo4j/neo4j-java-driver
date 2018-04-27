/*
 * Copyright (c) 2002-2018 Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Logging;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.neo4j.driver.internal.cluster.ClusterCompositionUtil.A;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;

public class LeastConnectedLoadBalancingStrategyTest
{
    @Mock
    private ConnectionPool connectionPool;
    private LeastConnectedLoadBalancingStrategy strategy;

    @Before
    public void setUp() throws Exception
    {
        initMocks( this );
        strategy = new LeastConnectedLoadBalancingStrategy( connectionPool, DEV_NULL_LOGGING );
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

    @Test
    public void shouldTraceLogWhenNoAddressSelected()
    {
        Logging logging = mock( Logging.class );
        Logger logger = mock( Logger.class );
        when( logging.getLog( anyString() ) ).thenReturn( logger );

        LoadBalancingStrategy strategy = new LeastConnectedLoadBalancingStrategy( connectionPool, logging );

        strategy.selectReader( new BoltServerAddress[0] );
        strategy.selectWriter( new BoltServerAddress[0] );

        verify( logger ).trace( startsWith( "Unable to select" ), eq( "reader" ) );
        verify( logger ).trace( startsWith( "Unable to select" ), eq( "writer" ) );
    }

    @Test
    public void shouldTraceLogSelectedAddress()
    {
        Logging logging = mock( Logging.class );
        Logger logger = mock( Logger.class );
        when( logging.getLog( anyString() ) ).thenReturn( logger );

        when( connectionPool.activeConnections( any( BoltServerAddress.class ) ) ).thenReturn( 42 );

        LoadBalancingStrategy strategy = new LeastConnectedLoadBalancingStrategy( connectionPool, logging );

        strategy.selectReader( new BoltServerAddress[]{A} );
        strategy.selectWriter( new BoltServerAddress[]{A} );

        verify( logger ).trace( startsWith( "Selected" ), eq( "reader" ), eq( A ), eq( 42 ) );
        verify( logger ).trace( startsWith( "Selected" ), eq( "writer" ), eq( A ), eq( 42 ) );
    }
}
