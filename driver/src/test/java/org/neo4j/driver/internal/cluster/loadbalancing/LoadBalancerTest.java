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

import io.netty.util.concurrent.GlobalEventExecutor;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import org.neo4j.driver.internal.async.BoltServerAddress;
import org.neo4j.driver.internal.cluster.AddressSet;
import org.neo4j.driver.internal.cluster.ClusterComposition;
import org.neo4j.driver.internal.cluster.ClusterRoutingTable;
import org.neo4j.driver.internal.cluster.Rediscovery;
import org.neo4j.driver.internal.cluster.RoutingTable;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.util.FakeClock;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;

import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.async.BoltServerAddress.LOCAL_DEFAULT;
import static org.neo4j.driver.internal.cluster.ClusterCompositionUtil.A;
import static org.neo4j.driver.internal.cluster.ClusterCompositionUtil.B;
import static org.neo4j.driver.internal.cluster.ClusterCompositionUtil.C;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.util.Futures.getBlocking;
import static org.neo4j.driver.v1.AccessMode.READ;
import static org.neo4j.driver.v1.AccessMode.WRITE;
import static org.neo4j.driver.v1.util.TestUtil.asOrderedSet;

public class LoadBalancerTest
{
    @Test
    public void acquireShouldUpdateRoutingTableWhenKnownRoutingTableIsStale()
    {
        BoltServerAddress initialRouter = new BoltServerAddress( "initialRouter", 1 );
        BoltServerAddress reader1 = new BoltServerAddress( "reader-1", 2 );
        BoltServerAddress reader2 = new BoltServerAddress( "reader-1", 3 );
        BoltServerAddress writer1 = new BoltServerAddress( "writer-1", 4 );
        BoltServerAddress router1 = new BoltServerAddress( "router-1", 5 );

        ConnectionPool connectionPool = newAsyncConnectionPoolMock();
        ClusterRoutingTable routingTable = new ClusterRoutingTable( new FakeClock(), initialRouter );

        Set<BoltServerAddress> readers = new LinkedHashSet<>( Arrays.asList( reader1, reader2 ) );
        Set<BoltServerAddress> writers = new LinkedHashSet<>( singletonList( writer1 ) );
        Set<BoltServerAddress> routers = new LinkedHashSet<>( singletonList( router1 ) );
        ClusterComposition clusterComposition = new ClusterComposition( 42, readers, writers, routers );
        Rediscovery rediscovery = mock( Rediscovery.class );
        when( rediscovery.lookupClusterComposition( routingTable, connectionPool ) )
                .thenReturn( completedFuture( clusterComposition ) );

        LoadBalancer loadBalancer = new LoadBalancer( connectionPool, routingTable, rediscovery,
                GlobalEventExecutor.INSTANCE, DEV_NULL_LOGGING );

        assertNotNull( getBlocking( loadBalancer.acquireConnection( READ ) ) );

        verify( rediscovery ).lookupClusterComposition( routingTable, connectionPool );
        assertArrayEquals( new BoltServerAddress[]{reader1, reader2}, routingTable.readers().toArray() );
        assertArrayEquals( new BoltServerAddress[]{writer1}, routingTable.writers().toArray() );
        assertArrayEquals( new BoltServerAddress[]{router1}, routingTable.routers().toArray() );
    }

    @Test
    public void acquireShouldPurgeConnectionsWhenKnownRoutingTableIsStale()
    {
        BoltServerAddress initialRouter1 = new BoltServerAddress( "initialRouter-1", 1 );
        BoltServerAddress initialRouter2 = new BoltServerAddress( "initialRouter-2", 1 );
        BoltServerAddress reader = new BoltServerAddress( "reader", 2 );
        BoltServerAddress writer = new BoltServerAddress( "writer", 3 );
        BoltServerAddress router = new BoltServerAddress( "router", 4 );

        ConnectionPool connectionPool = newAsyncConnectionPoolMock();
        ClusterRoutingTable routingTable = new ClusterRoutingTable( new FakeClock(), initialRouter1, initialRouter2 );

        Set<BoltServerAddress> readers = new HashSet<>( singletonList( reader ) );
        Set<BoltServerAddress> writers = new HashSet<>( singletonList( writer ) );
        Set<BoltServerAddress> routers = new HashSet<>( singletonList( router ) );
        ClusterComposition clusterComposition = new ClusterComposition( 42, readers, writers, routers );
        Rediscovery rediscovery = mock( Rediscovery.class );
        when( rediscovery.lookupClusterComposition( routingTable, connectionPool ) )
                .thenReturn( completedFuture( clusterComposition ) );

        LoadBalancer loadBalancer = new LoadBalancer( connectionPool, routingTable, rediscovery,
                GlobalEventExecutor.INSTANCE, DEV_NULL_LOGGING );

        assertNotNull( getBlocking( loadBalancer.acquireConnection( READ ) ) );

        verify( rediscovery ).lookupClusterComposition( routingTable, connectionPool );
        verify( connectionPool ).purge( initialRouter1 );
        verify( connectionPool ).purge( initialRouter2 );
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
        ConnectionPool connectionPool = newAsyncConnectionPoolMock();
        RoutingTable routingTable = mock( RoutingTable.class );
        when( routingTable.isStaleFor( any( AccessMode.class ) ) ).thenReturn( true );
        Rediscovery rediscovery = mock( Rediscovery.class );
        ClusterComposition emptyClusterComposition = new ClusterComposition( 42, emptySet(), emptySet(), emptySet() );
        when( rediscovery.lookupClusterComposition( routingTable, connectionPool ) )
                .thenReturn( completedFuture( emptyClusterComposition ) );
        when( routingTable.readers() ).thenReturn( new AddressSet() );
        when( routingTable.writers() ).thenReturn( new AddressSet() );

        LoadBalancer loadBalancer = new LoadBalancer( connectionPool, routingTable, rediscovery,
                GlobalEventExecutor.INSTANCE, DEV_NULL_LOGGING );

        try
        {
            getBlocking( loadBalancer.acquireConnection( READ ) );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( SessionExpiredException.class ) );
            assertThat( e.getMessage(), startsWith( "Failed to obtain connection towards READ server" ) );
        }

        try
        {
            getBlocking( loadBalancer.acquireConnection( WRITE ) );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( SessionExpiredException.class ) );
            assertThat( e.getMessage(), startsWith( "Failed to obtain connection towards WRITE server" ) );
        }
    }

    @Test
    public void shouldSelectLeastConnectedAddress()
    {
        ConnectionPool connectionPool = newAsyncConnectionPoolMock();

        when( connectionPool.activeConnections( A ) ).thenReturn( 0 );
        when( connectionPool.activeConnections( B ) ).thenReturn( 20 );
        when( connectionPool.activeConnections( C ) ).thenReturn( 0 );

        RoutingTable routingTable = mock( RoutingTable.class );
        AddressSet readerAddresses = mock( AddressSet.class );
        when( readerAddresses.toArray() ).thenReturn( new BoltServerAddress[]{A, B, C} );
        when( routingTable.readers() ).thenReturn( readerAddresses );

        Rediscovery rediscovery = mock( Rediscovery.class );

        LoadBalancer loadBalancer = new LoadBalancer( connectionPool, routingTable, rediscovery,
                GlobalEventExecutor.INSTANCE, DEV_NULL_LOGGING );

        Set<BoltServerAddress> seenAddresses = new HashSet<>();
        for ( int i = 0; i < 10; i++ )
        {
            Connection connection = getBlocking( loadBalancer.acquireConnection( READ ) );
            seenAddresses.add( connection.serverAddress() );
        }

        // server B should never be selected because it has many active connections
        assertEquals( 2, seenAddresses.size() );
        assertTrue( seenAddresses.containsAll( Arrays.asList( A, C ) ) );
    }

    @Test
    public void shouldRoundRobinWhenNoActiveConnections()
    {
        ConnectionPool connectionPool = newAsyncConnectionPoolMock();

        RoutingTable routingTable = mock( RoutingTable.class );
        AddressSet readerAddresses = mock( AddressSet.class );
        when( readerAddresses.toArray() ).thenReturn( new BoltServerAddress[]{A, B, C} );
        when( routingTable.readers() ).thenReturn( readerAddresses );

        Rediscovery rediscovery = mock( Rediscovery.class );

        LoadBalancer loadBalancer = new LoadBalancer( connectionPool, routingTable, rediscovery,
                GlobalEventExecutor.INSTANCE, DEV_NULL_LOGGING );

        Set<BoltServerAddress> seenAddresses = new HashSet<>();
        for ( int i = 0; i < 10; i++ )
        {
            Connection connection = getBlocking( loadBalancer.acquireConnection( READ ) );
            seenAddresses.add( connection.serverAddress() );
        }

        assertEquals( 3, seenAddresses.size() );
        assertTrue( seenAddresses.containsAll( Arrays.asList( A, B, C ) ) );
    }

    @Test
    public void shouldTryMultipleServersAfterRediscovery()
    {
        Set<BoltServerAddress> unavailableAddresses = asOrderedSet( A );
        ConnectionPool connectionPool = newConnectionPoolMockWithFailures( unavailableAddresses );

        ClusterRoutingTable routingTable = new ClusterRoutingTable( new FakeClock(), A );
        Rediscovery rediscovery = mock( Rediscovery.class );
        ClusterComposition clusterComposition = new ClusterComposition( 42,
                asOrderedSet( A, B ), asOrderedSet( A, B ), asOrderedSet( A, B ) );
        when( rediscovery.lookupClusterComposition( any(), any() ) )
                .thenReturn( completedFuture( clusterComposition ) );

        LoadBalancer loadBalancer = new LoadBalancer( connectionPool, routingTable, rediscovery,
                GlobalEventExecutor.INSTANCE, DEV_NULL_LOGGING );

        Connection connection = getBlocking( loadBalancer.acquireConnection( READ ) );

        assertNotNull( connection );
        assertEquals( B, connection.serverAddress() );
        // routing table should've forgotten A
        assertArrayEquals( new BoltServerAddress[]{B}, routingTable.readers().toArray() );
    }

    private void testRediscoveryWhenStale( AccessMode mode )
    {
        ConnectionPool connectionPool = mock( ConnectionPool.class );
        when( connectionPool.acquire( LOCAL_DEFAULT ) )
                .thenReturn( completedFuture( mock( Connection.class ) ) );

        RoutingTable routingTable = newStaleRoutingTableMock( mode );
        Rediscovery rediscovery = newRediscoveryMock();

        LoadBalancer loadBalancer = new LoadBalancer( connectionPool, routingTable, rediscovery,
                GlobalEventExecutor.INSTANCE, DEV_NULL_LOGGING );
        Connection connection = getBlocking( loadBalancer.acquireConnection( mode ) );
        assertNotNull( connection );

        verify( routingTable ).isStaleFor( mode );
        verify( rediscovery ).lookupClusterComposition( routingTable, connectionPool );
    }

    private void testNoRediscoveryWhenNotStale( AccessMode staleMode, AccessMode notStaleMode )
    {
        ConnectionPool connectionPool = mock( ConnectionPool.class );
        when( connectionPool.acquire( LOCAL_DEFAULT ) )
                .thenReturn( completedFuture( mock( Connection.class ) ) );

        RoutingTable routingTable = newStaleRoutingTableMock( staleMode );
        Rediscovery rediscovery = newRediscoveryMock();

        LoadBalancer loadBalancer = new LoadBalancer( connectionPool, routingTable, rediscovery,
                GlobalEventExecutor.INSTANCE, DEV_NULL_LOGGING );

        assertNotNull( getBlocking( loadBalancer.acquireConnection( notStaleMode ) ) );
        verify( routingTable ).isStaleFor( notStaleMode );
        verify( rediscovery, never() ).lookupClusterComposition( routingTable, connectionPool );
    }

    private static RoutingTable newStaleRoutingTableMock( AccessMode mode )
    {
        RoutingTable routingTable = mock( RoutingTable.class );
        when( routingTable.isStaleFor( mode ) ).thenReturn( true );
        when( routingTable.update( any( ClusterComposition.class ) ) ).thenReturn( new HashSet<>() );

        AddressSet addresses = new AddressSet();
        addresses.update( new HashSet<>( singletonList( LOCAL_DEFAULT ) ), new HashSet<>() );
        when( routingTable.readers() ).thenReturn( addresses );
        when( routingTable.writers() ).thenReturn( addresses );

        return routingTable;
    }

    private static Rediscovery newRediscoveryMock()
    {
        Rediscovery rediscovery = mock( Rediscovery.class );
        Set<BoltServerAddress> noServers = Collections.emptySet();
        ClusterComposition clusterComposition = new ClusterComposition( 1, noServers, noServers, noServers );
        when( rediscovery.lookupClusterComposition( any( RoutingTable.class ), any( ConnectionPool.class ) ) )
                .thenReturn( completedFuture( clusterComposition ) );
        return rediscovery;
    }

    private static ConnectionPool newAsyncConnectionPoolMock()
    {
        return newConnectionPoolMockWithFailures( emptySet() );
    }

    private static ConnectionPool newConnectionPoolMockWithFailures(
            Set<BoltServerAddress> unavailableAddresses )
    {
        ConnectionPool pool = mock( ConnectionPool.class );
        when( pool.acquire( any( BoltServerAddress.class ) ) ).then( invocation ->
        {
            BoltServerAddress requestedAddress = invocation.getArgumentAt( 0, BoltServerAddress.class );
            if ( unavailableAddresses.contains( requestedAddress ) )
            {
                return Futures.failedFuture( new ServiceUnavailableException( requestedAddress + " is unavailable!" ) );
            }
            Connection connection = mock( Connection.class );
            when( connection.serverAddress() ).thenReturn( requestedAddress );
            return completedFuture( connection );
        } );
        return pool;
    }
}
