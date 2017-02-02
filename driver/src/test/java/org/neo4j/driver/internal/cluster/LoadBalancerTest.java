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

import org.junit.Test;
import org.mockito.InOrder;

import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.logging.DevNullLogger.DEV_NULL_LOGGER;

public class LoadBalancerTest
{
    @Test
    public void ensureRoutingShouldUpdateRoutingTableAndPurgeConnectionPoolWhenStale() throws Exception
    {
        // given
        ConnectionPool conns = mock( ConnectionPool.class );
        RoutingTable routingTable = mock( RoutingTable.class );
        Rediscovery rediscovery = mock( Rediscovery.class );
        when( routingTable.isStale() ).thenReturn( true );
        HashSet<BoltServerAddress> set = new HashSet<>( asList( new BoltServerAddress( "abc", 12 ) ) );
        when( routingTable.update( any( ClusterComposition.class ) ) ).thenReturn( set );

        // when
        LoadBalancer balancer = new LoadBalancer( DEV_NULL_LOGGER, conns, routingTable, rediscovery );

        // then
        InOrder inOrder = inOrder( rediscovery, routingTable, conns );
        inOrder.verify( rediscovery ).lookupRoutingTable( conns, routingTable );
        inOrder.verify( routingTable ).update( any( ClusterComposition.class ) );
        inOrder.verify( conns ).purge( new BoltServerAddress( "abc", 12 ) );
    }


    @Test
    public void shouldEnsureRoutingOnInitialization() throws Exception
    {
        // given & when
        final AtomicInteger ensureRoutingCounter = new AtomicInteger( 0 );
        LoadBalancer balancer = new LoadBalancer( DEV_NULL_LOGGER, mock( ConnectionPool.class ),
                mock( RoutingTable.class ), mock( Rediscovery.class ) ) {
            @Override
            public void ensureRouting()
            {
                ensureRoutingCounter.incrementAndGet();
            }
        };

        // then
        assertThat( ensureRoutingCounter.get(), equalTo( 1 ) );
    }

    @Test
    public void shouldEnsureRoutingWhenAcquireConn() throws Exception
    {
        // given
        Connection writerConn = mock( Connection.class );
        Connection readConn = mock( Connection.class );
        LoadBalancer balancer = setupLoadBalancer( writerConn, readConn );
        LoadBalancer spy = spy( balancer );

        // when
        Connection connection = spy.acquireReadConnection();

        // then
        verify( spy ).ensureRouting();
        assertThat( connection, equalTo( readConn ) );
    }

    @Test
    public void shouldAcquireReaderOrWriterConn() throws Exception
    {
        Connection writerConn = mock( Connection.class );
        Connection readConn = mock( Connection.class );
        LoadBalancer balancer = setupLoadBalancer( writerConn, readConn );

        // when & then
        assertThat( balancer.acquireReadConnection(), equalTo( readConn ) );
        assertThat( balancer.acquireWriteConnection(), equalTo( writerConn ) );
    }

    private LoadBalancer setupLoadBalancer( Connection writerConn, Connection readConn )
    {
        BoltServerAddress writer = mock( BoltServerAddress.class );
        BoltServerAddress reader = mock( BoltServerAddress.class );

        ConnectionPool connPool = mock( ConnectionPool.class );
        when( connPool.acquire( writer ) ).thenReturn( writerConn );
        when( connPool.acquire( reader ) ).thenReturn( readConn );

        RoundRobinAddressSet writerAddrs = mock( RoundRobinAddressSet.class );
        when( writerAddrs.next() ).thenReturn( writer );

        RoundRobinAddressSet readerAddrs = mock( RoundRobinAddressSet.class );
        when( readerAddrs.next() ).thenReturn( reader );

        RoutingTable routingTable = mock( RoutingTable.class );
        when( routingTable.readers() ).thenReturn( readerAddrs );
        when( routingTable.writers() ).thenReturn( writerAddrs );

        LoadBalancer balancer = new LoadBalancer( DEV_NULL_LOGGER, connPool,
                routingTable, mock( Rediscovery.class ) ) ;

        return balancer;
    }
}
