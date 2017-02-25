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

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.driver.internal.ExplicitTransaction;
import org.neo4j.driver.internal.NetworkSession;
import org.neo4j.driver.internal.SessionResourcesHandler;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.spi.PooledConnection;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.logging.DevNullLogger.DEV_NULL_LOGGER;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.v1.AccessMode.READ;
import static org.neo4j.driver.v1.AccessMode.WRITE;

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
        Set<BoltServerAddress> set = Collections.singleton( new BoltServerAddress( "abc", 12 ) );
        when( routingTable.update( any( ClusterComposition.class ) ) ).thenReturn( set );

        // when
        LoadBalancer balancer = new LoadBalancer( routingTable, conns, rediscovery, DEV_NULL_LOGGER );

        // then
        assertNotNull( balancer );
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
        LoadBalancer balancer = new LoadBalancer( mock( RoutingTable.class ), mock( ConnectionPool.class ),
                mock( Rediscovery.class ), DEV_NULL_LOGGER )
        {
            @Override
            public void ensureRouting()
            {
                ensureRoutingCounter.incrementAndGet();
            }
        };

        // then
        assertNotNull( balancer );
        assertThat( ensureRoutingCounter.get(), equalTo( 1 ) );
    }

    @Test
    public void shouldEnsureRoutingWhenAcquireConn() throws Exception
    {
        // given
        PooledConnection writerConn = mock( PooledConnection.class );
        PooledConnection readConn = mock( PooledConnection.class );
        LoadBalancer balancer = setupLoadBalancer( writerConn, readConn );
        LoadBalancer spy = spy( balancer );

        // when
        Connection connection = spy.acquireConnection( READ );
        connection.init( "Test", Collections.<String,Value>emptyMap() );

        // then
        verify( spy ).ensureRouting();
        verify( readConn ).init( "Test", Collections.<String,Value>emptyMap() );
    }

    @Test
    public void shouldAcquireReaderOrWriterConn() throws Exception
    {
        PooledConnection writerConn = mock( PooledConnection.class );
        PooledConnection readConn = mock( PooledConnection.class );
        LoadBalancer balancer = setupLoadBalancer( writerConn, readConn );

        Connection acquiredReadConn = balancer.acquireConnection( READ );
        acquiredReadConn.init( "TestRead", Collections.<String,Value>emptyMap() );
        verify( readConn ).init( "TestRead", Collections.<String,Value>emptyMap() );

        Connection acquiredWriteConn = balancer.acquireConnection( WRITE );
        acquiredWriteConn.init( "TestWrite", Collections.<String,Value>emptyMap() );
        verify( writerConn ).init( "TestWrite", Collections.<String,Value>emptyMap() );
    }

    @Test
    public void shouldForgetAddressAndItsConnectionsOnServiceUnavailableWhileClosingTx()
    {
        RoutingTable routingTable = mock( RoutingTable.class );
        ConnectionPool connectionPool = mock( ConnectionPool.class );
        Rediscovery rediscovery = mock( Rediscovery.class );
        LoadBalancer loadBalancer = new LoadBalancer( routingTable, connectionPool, rediscovery, DEV_NULL_LOGGER );
        BoltServerAddress address = new BoltServerAddress( "host", 42 );

        PooledConnection connection = newConnectionWithFailingSync( address );
        Connection routingConnection = new RoutingPooledConnection( connection, loadBalancer, AccessMode.WRITE );
        Transaction tx = new ExplicitTransaction( routingConnection, mock( SessionResourcesHandler.class ) );

        try
        {
            tx.close();
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( SessionExpiredException.class ) );
            assertThat( e.getCause(), instanceOf( ServiceUnavailableException.class ) );
        }

        verify( routingTable ).forget( address );
        verify( connectionPool ).purge( address );
    }

    @Test
    public void shouldForgetAddressAndItsConnectionsOnServiceUnavailableWhileClosingSession()
    {
        RoutingTable routingTable = mock( RoutingTable.class, RETURNS_MOCKS );
        ConnectionPool connectionPool = mock( ConnectionPool.class );
        BoltServerAddress address = new BoltServerAddress( "host", 42 );
        PooledConnection connectionWithFailingSync = newConnectionWithFailingSync( address );
        when( connectionPool.acquire( any( BoltServerAddress.class ) ) ).thenReturn( connectionWithFailingSync );
        Rediscovery rediscovery = mock( Rediscovery.class );
        LoadBalancer loadBalancer = new LoadBalancer( routingTable, connectionPool, rediscovery, DEV_NULL_LOGGER );

        NetworkSession session = new NetworkSession( loadBalancer, AccessMode.WRITE, null, DEV_NULL_LOGGING );
        // begin transaction to make session obtain a connection
        session.beginTransaction();

        session.close();

        verify( routingTable ).forget( address );
        verify( connectionPool ).purge( address );
    }

    private LoadBalancer setupLoadBalancer( PooledConnection writerConn, PooledConnection readConn )
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

        return new LoadBalancer( routingTable, connPool, mock( Rediscovery.class ), DEV_NULL_LOGGER );
    }

    private static PooledConnection newConnectionWithFailingSync( BoltServerAddress address )
    {
        PooledConnection connection = mock( PooledConnection.class );
        doReturn( true ).when( connection ).isOpen();
        doReturn( address ).when( connection ).boltServerAddress();
        ServiceUnavailableException closeError = new ServiceUnavailableException( "Oh!" );
        doThrow( closeError ).when( connection ).sync();
        return connection;
    }

}
