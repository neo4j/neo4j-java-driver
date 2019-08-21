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
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.async.ConnectionContext;
import org.neo4j.driver.internal.async.connection.RoutingConnection;
import org.neo4j.driver.internal.cluster.AddressSet;
import org.neo4j.driver.internal.cluster.ClusterComposition;
import org.neo4j.driver.internal.cluster.ClusterRoutingTable;
import org.neo4j.driver.internal.cluster.RoutingTable;
import org.neo4j.driver.internal.cluster.RoutingTableHandler;
import org.neo4j.driver.internal.cluster.RoutingTableRegistry;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.util.FakeClock;
import org.neo4j.driver.internal.util.Futures;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.AccessMode.READ;
import static org.neo4j.driver.AccessMode.WRITE;
import static org.neo4j.driver.internal.async.ImmutableConnectionContext.simple;
import static org.neo4j.driver.internal.cluster.RediscoveryUtils.contextWithDatabase;
import static org.neo4j.driver.internal.cluster.RediscoveryUtils.contextWithMode;
import static org.neo4j.driver.internal.logging.DevNullLogger.DEV_NULL_LOGGER;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.messaging.request.MultiDatabaseUtil.ABSENT_DB_NAME;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.A;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.B;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.C;
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

        LoadBalancer loadBalancer = newLoadBalancer( connectionPool, routingTable );

        Connection acquired = await( loadBalancer.acquireConnection( contextWithMode( mode ) ) );

        assertThat( acquired, instanceOf( RoutingConnection.class ) );
        assertThat( acquired.mode(), equalTo( mode ) );
    }

    @ParameterizedTest
    @ValueSource( strings = {"", "foo", "data", ABSENT_DB_NAME} )
    void returnsCorrectDatabaseName( String databaseName )
    {
        ConnectionPool connectionPool = newConnectionPoolMock();
        RoutingTable routingTable = mock( RoutingTable.class );
        AddressSet writerAddresses = mock( AddressSet.class );
        when( writerAddresses.toArray() ).thenReturn( new BoltServerAddress[]{A} );
        when( routingTable.writers() ).thenReturn( writerAddresses );

        LoadBalancer loadBalancer = newLoadBalancer( connectionPool, routingTable );

        Connection acquired = await( loadBalancer.acquireConnection( contextWithDatabase( databaseName ) ) );

        assertThat( acquired, instanceOf( RoutingConnection.class ) );
        assertThat( acquired.databaseName(), equalTo( databaseName ) );
        verify( connectionPool ).acquire( A );
    }

    @Test
    void shouldThrowWhenRediscoveryReturnsNoSuitableServers()
    {
        ConnectionPool connectionPool = newConnectionPoolMock();
        RoutingTable routingTable = mock( RoutingTable.class );
        when( routingTable.readers() ).thenReturn( new AddressSet() );
        when( routingTable.writers() ).thenReturn( new AddressSet() );

        LoadBalancer loadBalancer = newLoadBalancer( connectionPool, routingTable );

        SessionExpiredException error1 = assertThrows( SessionExpiredException.class, () -> await( loadBalancer.acquireConnection( contextWithMode( READ ) ) ) );
        assertThat( error1.getMessage(), startsWith( "Failed to obtain connection towards READ server" ) );

        SessionExpiredException error2 = assertThrows( SessionExpiredException.class, () -> await( loadBalancer.acquireConnection( contextWithMode( WRITE ) ) ) );
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


        LoadBalancer loadBalancer = newLoadBalancer( connectionPool, routingTable );

        Set<BoltServerAddress> seenAddresses = new HashSet<>();
        for ( int i = 0; i < 10; i++ )
        {
            Connection connection = await( loadBalancer.acquireConnection( simple() ) );
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

        LoadBalancer loadBalancer = newLoadBalancer( connectionPool, routingTable );

        Set<BoltServerAddress> seenAddresses = new HashSet<>();
        for ( int i = 0; i < 10; i++ )
        {
            Connection connection = await( loadBalancer.acquireConnection( simple() ) );
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

        RoutingTable routingTable = new ClusterRoutingTable( ABSENT_DB_NAME, new FakeClock() );
        routingTable.update( new ClusterComposition( -1, new LinkedHashSet<>( Arrays.asList( A, B ) ), emptySet(), emptySet() ) );

        LoadBalancer loadBalancer = newLoadBalancer( connectionPool, routingTable );

        Connection connection = await( loadBalancer.acquireConnection( simple() ) );

        assertNotNull( connection );
        assertEquals( B, connection.serverAddress() );
        // routing table should've forgotten A
        assertArrayEquals( new BoltServerAddress[]{B}, routingTable.readers().toArray() );
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

    private static LoadBalancer newLoadBalancer( ConnectionPool connectionPool, RoutingTable routingTable )
    {
        // Used only in testing
        RoutingTableRegistry routingTables = mock( RoutingTableRegistry.class );
        RoutingTableHandler handler = mock( RoutingTableHandler.class );
        when( handler.routingTable() ).thenReturn( routingTable );
        when( routingTables.refreshRoutingTable( any( ConnectionContext.class ) ) ).thenReturn( CompletableFuture.completedFuture( handler ) );
        return new LoadBalancer( connectionPool, routingTables, DEV_NULL_LOGGER, new LeastConnectedLoadBalancingStrategy( connectionPool, DEV_NULL_LOGGING ),
                GlobalEventExecutor.INSTANCE );
    }
}
