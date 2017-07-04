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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.driver.internal.ExplicitTransaction;
import org.neo4j.driver.internal.NetworkSession;
import org.neo4j.driver.internal.SessionResourcesHandler;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.retry.ExponentialBackoffRetryLogic;
import org.neo4j.driver.internal.retry.RetryLogic;
import org.neo4j.driver.internal.retry.RetrySettings;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.spi.PooledConnection;
import org.neo4j.driver.internal.util.SleeplessClock;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.logging.DevNullLogger.DEV_NULL_LOGGER;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.net.BoltServerAddress.LOCAL_DEFAULT;
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
        Set<BoltServerAddress> set = singleton( new BoltServerAddress( "abc", 12 ) );
        when( routingTable.update( any( ClusterComposition.class ) ) ).thenReturn( set );

        // when
        LoadBalancer balancer = new LoadBalancer( conns, routingTable, rediscovery, DEV_NULL_LOGGER );

        // then
        assertNotNull( balancer );
        InOrder inOrder = inOrder( rediscovery, routingTable, conns );
        inOrder.verify( rediscovery ).lookupClusterComposition( routingTable, conns );
        inOrder.verify( routingTable ).update( any( ClusterComposition.class ) );
        inOrder.verify( conns ).purge( new BoltServerAddress( "abc", 12 ) );
    }

    @Test
    public void shouldRefreshRoutingTableOnInitialization() throws Exception
    {
        // given & when
        final AtomicInteger refreshRoutingTableCounter = new AtomicInteger( 0 );
        LoadBalancer balancer = new LoadBalancer( mock( ConnectionPool.class ), mock( RoutingTable.class ),
                mock( Rediscovery.class ), DEV_NULL_LOGGER )
        {
            @Override
            synchronized void refreshRoutingTable()
            {
                refreshRoutingTableCounter.incrementAndGet();
            }
        };

        // then
        assertNotNull( balancer );
        assertThat( refreshRoutingTableCounter.get(), equalTo( 1 ) );
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
        verify( spy ).ensureRouting( READ );
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
        LoadBalancer loadBalancer = new LoadBalancer( connectionPool, routingTable, rediscovery, DEV_NULL_LOGGER );
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
        BoltServerAddress address = new BoltServerAddress( "host", 42 );
        RoutingTable routingTable = mock( RoutingTable.class );
        AddressSet addressSet = mock( AddressSet.class );
        when( addressSet.toArray() ).thenReturn( new BoltServerAddress[]{address} );
        when( routingTable.writers() ).thenReturn( addressSet );
        ConnectionPool connectionPool = mock( ConnectionPool.class );
        PooledConnection connectionWithFailingSync = newConnectionWithFailingSync( address );
        when( connectionPool.acquire( any( BoltServerAddress.class ) ) ).thenReturn( connectionWithFailingSync );
        Rediscovery rediscovery = mock( Rediscovery.class );
        LoadBalancer loadBalancer = new LoadBalancer( connectionPool, routingTable, rediscovery, DEV_NULL_LOGGER );

        Session session = newSession( loadBalancer );
        // begin transaction to make session obtain a connection
        session.beginTransaction();

        session.close();

        verify( routingTable ).forget( address );
        verify( connectionPool ).purge( address );
    }

    @Test
    public void shouldRediscoverOnReadWhenRoutingTableIsStaleForReads()
    {
        testRediscoveryWhenStale( READ );
    }

    @Test
    public void shouldRediscoverOnWriteWhenRoutingTableIsStaleForWrites()
    {
        testRediscoveryWhenStale( WRITE );
    }

    @Test
    public void shouldNotRediscoverOnReadWhenRoutingTableIsStaleForWritesButNotReads()
    {
        testNoRediscoveryWhenNotStale( WRITE, READ );
    }

    @Test
    public void shouldNotRediscoverOnWriteWhenRoutingTableIsStaleForReadsButNotWrites()
    {
        testNoRediscoveryWhenNotStale( READ, WRITE );
    }

    @Test
    public void shouldThrowWhenRediscoveryReturnsNoSuitableServers()
    {
        ConnectionPool connections = mock( ConnectionPool.class );
        RoutingTable routingTable = mock( RoutingTable.class );
        when( routingTable.isStaleFor( any( AccessMode.class ) ) ).thenReturn( true );
        Rediscovery rediscovery = mock( Rediscovery.class );
        when( routingTable.readers() ).thenReturn( new AddressSet() );
        when( routingTable.writers() ).thenReturn( new AddressSet() );

        LoadBalancer loadBalancer = new LoadBalancer( connections, routingTable, rediscovery, DEV_NULL_LOGGER );

        try
        {
            loadBalancer.acquireConnection( READ );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( SessionExpiredException.class ) );
            assertThat( e.getMessage(), startsWith( "Failed to obtain connection towards READ server" ) );
        }

        try
        {
            loadBalancer.acquireConnection( WRITE );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( SessionExpiredException.class ) );
            assertThat( e.getMessage(), startsWith( "Failed to obtain connection towards WRITE server" ) );
        }
    }

    private void testRediscoveryWhenStale( AccessMode mode )
    {
        ConnectionPool connections = mock( ConnectionPool.class );
        when( connections.acquire( LOCAL_DEFAULT ) ).thenReturn( mock( PooledConnection.class ) );

        RoutingTable routingTable = newStaleRoutingTableMock( mode );
        Rediscovery rediscovery = newRediscoveryMock();

        LoadBalancer loadBalancer = new LoadBalancer( connections, routingTable, rediscovery, DEV_NULL_LOGGER );
        verify( rediscovery ).lookupClusterComposition( routingTable, connections );

        assertNotNull( loadBalancer.acquireConnection( mode ) );
        verify( routingTable ).isStaleFor( mode );
        verify( rediscovery, times( 2 ) ).lookupClusterComposition( routingTable, connections );
    }

    private void testNoRediscoveryWhenNotStale( AccessMode staleMode, AccessMode notStaleMode )
    {
        ConnectionPool connections = mock( ConnectionPool.class );
        when( connections.acquire( LOCAL_DEFAULT ) ).thenReturn( mock( PooledConnection.class ) );

        RoutingTable routingTable = newStaleRoutingTableMock( staleMode );
        Rediscovery rediscovery = newRediscoveryMock();

        LoadBalancer loadBalancer = new LoadBalancer( connections, routingTable, rediscovery, DEV_NULL_LOGGER );
        verify( rediscovery ).lookupClusterComposition( routingTable, connections );

        assertNotNull( loadBalancer.acquireConnection( notStaleMode ) );
        verify( routingTable ).isStaleFor( notStaleMode );
        verify( rediscovery ).lookupClusterComposition( routingTable, connections );
    }

    private LoadBalancer setupLoadBalancer( PooledConnection writerConn, PooledConnection readConn )
    {
        return setupLoadBalancer( writerConn, readConn, mock( Rediscovery.class ) );
    }

    private LoadBalancer setupLoadBalancer( PooledConnection writerConn, PooledConnection readConn,
            Rediscovery rediscovery )
    {
        BoltServerAddress writer = mock( BoltServerAddress.class );
        BoltServerAddress reader = mock( BoltServerAddress.class );

        ConnectionPool connPool = mock( ConnectionPool.class );
        when( connPool.acquire( writer ) ).thenReturn( writerConn );
        when( connPool.acquire( reader ) ).thenReturn( readConn );

        AddressSet writerAddrs = mock( AddressSet.class );
        when( writerAddrs.toArray() ).thenReturn( new BoltServerAddress[]{writer} );

        AddressSet readerAddrs = mock( AddressSet.class );
        when( readerAddrs.toArray() ).thenReturn( new BoltServerAddress[]{reader} );

        RoutingTable routingTable = mock( RoutingTable.class );
        when( routingTable.readers() ).thenReturn( readerAddrs );
        when( routingTable.writers() ).thenReturn( writerAddrs );

        return new LoadBalancer( connPool, routingTable, rediscovery, DEV_NULL_LOGGER );
    }

    private static Session newSession( LoadBalancer loadBalancer )
    {
        SleeplessClock clock = new SleeplessClock();
        RetryLogic retryLogic = new ExponentialBackoffRetryLogic( RetrySettings.DEFAULT, clock, DEV_NULL_LOGGING );
        return new NetworkSession( loadBalancer, AccessMode.WRITE, retryLogic, DEV_NULL_LOGGING );
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

    private static RoutingTable newStaleRoutingTableMock( AccessMode mode )
    {
        RoutingTable routingTable = mock( RoutingTable.class );
        when( routingTable.isStaleFor( mode ) ).thenReturn( true );
        when( routingTable.update( any( ClusterComposition.class ) ) ).thenReturn( new HashSet<BoltServerAddress>() );

        AddressSet addresses = new AddressSet();
        addresses.update( new HashSet<>( singletonList( LOCAL_DEFAULT ) ), new HashSet<BoltServerAddress>() );
        when( routingTable.readers() ).thenReturn( addresses );
        when( routingTable.writers() ).thenReturn( addresses );

        return routingTable;
    }

    private static Rediscovery newRediscoveryMock()
    {
        Rediscovery rediscovery = mock( Rediscovery.class );
        Set<BoltServerAddress> noServers = Collections.<BoltServerAddress>emptySet();
        ClusterComposition clusterComposition = new ClusterComposition( 1, noServers, noServers, noServers );
        when( rediscovery.lookupClusterComposition( any( RoutingTable.class ), any( ConnectionPool.class ) ) )
                .thenReturn( clusterComposition );
        return rediscovery;
    }
}
