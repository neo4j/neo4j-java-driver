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

import io.netty.util.concurrent.GlobalEventExecutor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.async.AccessModeConnection;
import org.neo4j.driver.internal.cluster.AddressSet;
import org.neo4j.driver.internal.cluster.ClusterComposition;
import org.neo4j.driver.internal.cluster.ClusterRoutingTable;
import org.neo4j.driver.internal.cluster.Rediscovery;
import org.neo4j.driver.internal.cluster.RoutingTable;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.util.FakeClock;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.SessionExpiredException;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.BoltServerAddress.LOCAL_DEFAULT;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.A;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.B;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.C;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.D;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.E;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.F;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.AccessMode.READ;
import static org.neo4j.driver.AccessMode.WRITE;
import static org.neo4j.driver.util.TestUtil.asOrderedSet;
import static org.neo4j.driver.util.TestUtil.await;

class LoadBalancerTest
{
    @ParameterizedTest
    @EnumSource( AccessMode.class )
    void returnsCorrectAccessMode( AccessMode mode )
    {
        ConnectionPool connectionPool = newConnectionPoolMock();
        RoutingTable routingTable = mock( RoutingTable.class );
        AddressSet readerAddresses = mock( AddressSet.class );
        AddressSet writerAddresses = mock( AddressSet.class );
        when( readerAddresses.toArray() ).thenReturn( new BoltServerAddress[]{A} );
        when( writerAddresses.toArray() ).thenReturn( new BoltServerAddress[]{B} );
        when( routingTable.readers() ).thenReturn( readerAddresses );
        when( routingTable.writers() ).thenReturn( writerAddresses );
        Rediscovery rediscovery = mock( Rediscovery.class );

        LoadBalancer loadBalancer = new LoadBalancer( connectionPool, routingTable, rediscovery,
                GlobalEventExecutor.INSTANCE, DEV_NULL_LOGGING );

        Connection acquired = await( loadBalancer.acquireConnection( mode ) );

        assertThat( acquired, instanceOf( AccessModeConnection.class ) );
        assertThat( acquired.mode(), equalTo( mode ) );
    }

    @Test
    void acquireShouldUpdateRoutingTableWhenKnownRoutingTableIsStale()
    {
        BoltServerAddress initialRouter = new BoltServerAddress( "initialRouter", 1 );
        BoltServerAddress reader1 = new BoltServerAddress( "reader-1", 2 );
        BoltServerAddress reader2 = new BoltServerAddress( "reader-1", 3 );
        BoltServerAddress writer1 = new BoltServerAddress( "writer-1", 4 );
        BoltServerAddress router1 = new BoltServerAddress( "router-1", 5 );

        ConnectionPool connectionPool = newConnectionPoolMock();
        ClusterRoutingTable routingTable = new ClusterRoutingTable( new FakeClock(), initialRouter );

        Set<BoltServerAddress> readers = new LinkedHashSet<>( asList( reader1, reader2 ) );
        Set<BoltServerAddress> writers = new LinkedHashSet<>( singletonList( writer1 ) );
        Set<BoltServerAddress> routers = new LinkedHashSet<>( singletonList( router1 ) );
        ClusterComposition clusterComposition = new ClusterComposition( 42, readers, writers, routers );
        Rediscovery rediscovery = mock( Rediscovery.class );
        when( rediscovery.lookupClusterComposition( routingTable, connectionPool ) )
                .thenReturn( completedFuture( clusterComposition ) );

        LoadBalancer loadBalancer = new LoadBalancer( connectionPool, routingTable, rediscovery,
                GlobalEventExecutor.INSTANCE, DEV_NULL_LOGGING );

        assertNotNull( await( loadBalancer.acquireConnection( READ ) ) );

        verify( rediscovery ).lookupClusterComposition( routingTable, connectionPool );
        assertArrayEquals( new BoltServerAddress[]{reader1, reader2}, routingTable.readers().toArray() );
        assertArrayEquals( new BoltServerAddress[]{writer1}, routingTable.writers().toArray() );
        assertArrayEquals( new BoltServerAddress[]{router1}, routingTable.routers().toArray() );
    }

    @Test
    void shouldRediscoverOnReadWhenRoutingTableIsStaleForReads()
    {
        testRediscoveryWhenStale( READ );
    }

    @Test
    void shouldRediscoverOnWriteWhenRoutingTableIsStaleForWrites()
    {
        testRediscoveryWhenStale( WRITE );
    }

    @Test
    void shouldNotRediscoverOnReadWhenRoutingTableIsStaleForWritesButNotReads()
    {
        testNoRediscoveryWhenNotStale( WRITE, READ );
    }

    @Test
    void shouldNotRediscoverOnWriteWhenRoutingTableIsStaleForReadsButNotWrites()
    {
        testNoRediscoveryWhenNotStale( READ, WRITE );
    }

    @Test
    void shouldThrowWhenRediscoveryReturnsNoSuitableServers()
    {
        ConnectionPool connectionPool = newConnectionPoolMock();
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

        SessionExpiredException error1 = assertThrows( SessionExpiredException.class, () -> await( loadBalancer.acquireConnection( READ ) ) );
        assertThat( error1.getMessage(), startsWith( "Failed to obtain connection towards READ server" ) );

        SessionExpiredException error2 = assertThrows( SessionExpiredException.class, () -> await( loadBalancer.acquireConnection( WRITE ) ) );
        assertThat( error2.getMessage(), startsWith( "Failed to obtain connection towards WRITE server" ) );
    }

    @Test
    void shouldSelectLeastConnectedAddress()
    {
        ConnectionPool connectionPool = newConnectionPoolMock();

        when( connectionPool.inUseConnections( A ) ).thenReturn( 0 );
        when( connectionPool.inUseConnections( B ) ).thenReturn( 20 );
        when( connectionPool.inUseConnections( C ) ).thenReturn( 0 );

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
            Connection connection = await( loadBalancer.acquireConnection( READ ) );
            seenAddresses.add( connection.serverAddress() );
        }

        // server B should never be selected because it has many active connections
        assertEquals( 2, seenAddresses.size() );
        assertTrue( seenAddresses.containsAll( asList( A, C ) ) );
    }

    @Test
    void shouldRoundRobinWhenNoActiveConnections()
    {
        ConnectionPool connectionPool = newConnectionPoolMock();

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
            Connection connection = await( loadBalancer.acquireConnection( READ ) );
            seenAddresses.add( connection.serverAddress() );
        }

        assertEquals( 3, seenAddresses.size() );
        assertTrue( seenAddresses.containsAll( asList( A, B, C ) ) );
    }

    @Test
    void shouldTryMultipleServersAfterRediscovery()
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

        Connection connection = await( loadBalancer.acquireConnection( READ ) );

        assertNotNull( connection );
        assertEquals( B, connection.serverAddress() );
        // routing table should've forgotten A
        assertArrayEquals( new BoltServerAddress[]{B}, routingTable.readers().toArray() );
    }

    @Test
    void shouldRemoveAddressFromRoutingTableOnConnectionFailure()
    {
        RoutingTable routingTable = new ClusterRoutingTable( new FakeClock() );
        routingTable.update( new ClusterComposition(
                42, asOrderedSet( A, B, C ), asOrderedSet( A, C, E ), asOrderedSet( B, D, F ) ) );

        LoadBalancer loadBalancer = new LoadBalancer( newConnectionPoolMock(), routingTable, newRediscoveryMock(),
                GlobalEventExecutor.INSTANCE, DEV_NULL_LOGGING );

        loadBalancer.onConnectionFailure( B );

        assertArrayEquals( new BoltServerAddress[]{A, C}, routingTable.readers().toArray() );
        assertArrayEquals( new BoltServerAddress[]{A, C, E}, routingTable.writers().toArray() );
        assertArrayEquals( new BoltServerAddress[]{D, F}, routingTable.routers().toArray() );

        loadBalancer.onConnectionFailure( A );

        assertArrayEquals( new BoltServerAddress[]{C}, routingTable.readers().toArray() );
        assertArrayEquals( new BoltServerAddress[]{C, E}, routingTable.writers().toArray() );
        assertArrayEquals( new BoltServerAddress[]{D, F}, routingTable.routers().toArray() );
    }

    @Test
    void shouldRetainAllFetchedAddressesInConnectionPoolAfterFetchingOfRoutingTable()
    {
        RoutingTable routingTable = new ClusterRoutingTable( new FakeClock() );
        routingTable.update( new ClusterComposition(
                42, asOrderedSet(), asOrderedSet( B, C ), asOrderedSet( D, E ) ) );

        ConnectionPool connectionPool = newConnectionPoolMock();

        Rediscovery rediscovery = newRediscoveryMock();
        when( rediscovery.lookupClusterComposition( any(), any() ) ).thenReturn( completedFuture(
                new ClusterComposition( 42, asOrderedSet( A, B ), asOrderedSet( B, C ), asOrderedSet( A, C ) ) ) );

        LoadBalancer loadBalancer = new LoadBalancer( connectionPool, routingTable, rediscovery,
                GlobalEventExecutor.INSTANCE, DEV_NULL_LOGGING );

        Connection connection = await( loadBalancer.acquireConnection( READ ) );
        assertNotNull( connection );

        verify( connectionPool ).retainAll( new HashSet<>( asList( A, B, C ) ) );
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
        Connection connection = await( loadBalancer.acquireConnection( mode ) );
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

        assertNotNull( await( loadBalancer.acquireConnection( notStaleMode ) ) );
        verify( routingTable ).isStaleFor( notStaleMode );
        verify( rediscovery, never() ).lookupClusterComposition( routingTable, connectionPool );
    }

    private static RoutingTable newStaleRoutingTableMock( AccessMode mode )
    {
        RoutingTable routingTable = mock( RoutingTable.class );
        when( routingTable.isStaleFor( mode ) ).thenReturn( true );

        AddressSet addresses = new AddressSet();
        addresses.update( new HashSet<>( singletonList( LOCAL_DEFAULT ) ) );
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

    private static ConnectionPool newConnectionPoolMock()
    {
        return newConnectionPoolMockWithFailures( emptySet() );
    }

    private static ConnectionPool newConnectionPoolMockWithFailures(
            Set<BoltServerAddress> unavailableAddresses )
    {
        ConnectionPool pool = mock( ConnectionPool.class );
        when( pool.acquire( any( BoltServerAddress.class ) ) ).then( invocation ->
        {
            BoltServerAddress requestedAddress = invocation.getArgument( 0 );
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
