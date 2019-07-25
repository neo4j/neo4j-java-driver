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
package org.neo4j.driver.internal.cluster;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.InternalBookmark;
import org.neo4j.driver.internal.async.ImmutableConnectionContext;
import org.neo4j.driver.internal.cluster.RoutingTableRegistryImpl.RoutingTableHandlerFactory;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.util.Clock;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.cluster.RoutingSettings.STALE_ROUTING_TABLE_PURGE_DELAY_MS;
import static org.neo4j.driver.internal.logging.DevNullLogger.DEV_NULL_LOGGER;
import static org.neo4j.driver.internal.messaging.request.MultiDatabaseUtil.ABSENT_DB_NAME;
import static org.neo4j.driver.internal.messaging.request.MultiDatabaseUtil.SYSTEM_DB_NAME;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.A;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.B;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.C;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.D;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.E;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.F;
import static org.neo4j.driver.util.TestUtil.await;

class RoutingTableRegistryImplTest
{
    @Test
    void factoryShouldCreateARoutingTableWithSameDatabaseName() throws Throwable
    {
        Clock clock = Clock.SYSTEM;
        RoutingTableHandlerFactory factory =
                new RoutingTableHandlerFactory( mock( ConnectionPool.class ), mock( RediscoveryImpl.class ), clock, DEV_NULL_LOGGER,
                        STALE_ROUTING_TABLE_PURGE_DELAY_MS );

        RoutingTableHandler handler = factory.newInstance( "Molly", null );
        RoutingTable table = handler.routingTable();

        assertThat( table.database(), equalTo( "Molly" ) );

        assertThat( table.routers().size(), equalTo( 0 ) );
        assertThat( table.readers().size(), equalTo( 0 ) );
        assertThat( table.writers().size(), equalTo( 0 ) );

        assertTrue( table.isStaleFor( AccessMode.READ ) );
        assertTrue( table.isStaleFor( AccessMode.WRITE ) );
    }

    @ParameterizedTest
    @ValueSource( strings = {ABSENT_DB_NAME, SYSTEM_DB_NAME, "", "database", " molly "} )
    void shouldCreateRoutingTableHandlerIfAbsentWhenFreshRoutingTable( String databaseName ) throws Throwable
    {
        // Given
        ConcurrentMap<String,RoutingTableHandler> map = new ConcurrentHashMap<>();
        RoutingTableHandlerFactory factory = mockedHandlerFactory();
        RoutingTableRegistryImpl routingTables = newRoutingTables( map, factory );

        // When
        routingTables.refreshRoutingTable( new ImmutableConnectionContext( databaseName, InternalBookmark.empty(), AccessMode.READ ) );

        // Then
        assertTrue( map.containsKey( databaseName ) );
        verify( factory ).newInstance( eq( databaseName ), eq( routingTables ) );
    }

    @ParameterizedTest
    @ValueSource( strings = {ABSENT_DB_NAME, SYSTEM_DB_NAME, "", "database", " molly "} )
    void shouldReturnExistingRoutingTableHandlerWhenFreshRoutingTable( String databaseName ) throws Throwable
    {
        // Given
        ConcurrentMap<String,RoutingTableHandler> map = new ConcurrentHashMap<>();
        RoutingTableHandler handler = mockedRoutingTableHandler();
        map.put( databaseName, handler );

        RoutingTableHandlerFactory factory = mockedHandlerFactory();
        RoutingTableRegistryImpl routingTables = newRoutingTables( map, factory );
        ImmutableConnectionContext context = new ImmutableConnectionContext( databaseName, InternalBookmark.empty(), AccessMode.READ );

        // When
        RoutingTableHandler actual = await( routingTables.refreshRoutingTable( context ) );

        // Then it is the one we put in map that is picked up.
        verify( handler ).refreshRoutingTable( context );
        // Then it is the one we put in map that is picked up.
        assertEquals( handler, actual );
    }

    @ParameterizedTest
    @EnumSource( AccessMode.class )
    void shouldReturnFreshRoutingTable( AccessMode mode ) throws Throwable
    {
        // Given
        ConcurrentMap<String,RoutingTableHandler> map = new ConcurrentHashMap<>();
        RoutingTableHandler handler = mockedRoutingTableHandler();
        RoutingTableHandlerFactory factory = mockedHandlerFactory( handler );
        RoutingTableRegistryImpl routingTables = new RoutingTableRegistryImpl( map, factory, DEV_NULL_LOGGER );

        ImmutableConnectionContext context = new ImmutableConnectionContext( ABSENT_DB_NAME, InternalBookmark.empty(), mode );
        // When
        routingTables.refreshRoutingTable( context );

        // Then
        verify( handler ).refreshRoutingTable( context );
    }

    @Test
    void shouldReturnServersInAllRoutingTables() throws Throwable
    {
        // Given
        ConcurrentMap<String,RoutingTableHandler> map = new ConcurrentHashMap<>();
        map.put( "Apple", mockedRoutingTableHandler( A, B, C ) );
        map.put( "Banana", mockedRoutingTableHandler( B, C, D ) );
        map.put( "Orange", mockedRoutingTableHandler( E, F, C ) );
        RoutingTableHandlerFactory factory = mockedHandlerFactory();
        RoutingTableRegistryImpl routingTables = new RoutingTableRegistryImpl( map, factory, DEV_NULL_LOGGER );

        // When
        Set<BoltServerAddress> servers = routingTables.allServers();

        // Then
        assertThat( servers, containsInAnyOrder( A, B, C, D, E, F ) );
    }

    @Test
    void shouldRemoveRoutingTableHandler() throws Throwable
    {
        // Given
        ConcurrentMap<String,RoutingTableHandler> map = new ConcurrentHashMap<>();
        map.put( "Apple", mockedRoutingTableHandler( A ) );
        map.put( "Banana", mockedRoutingTableHandler( B ) );
        map.put( "Orange", mockedRoutingTableHandler( C ) );

        RoutingTableHandlerFactory factory = mockedHandlerFactory();
        RoutingTableRegistryImpl routingTables = newRoutingTables( map, factory );

        // When
        routingTables.remove( "Apple" );
        routingTables.remove( "Banana" );
        // Then
        assertThat( routingTables.allServers(), contains( C ) );
    }

    @Test
    void shouldRemoveStatleRoutingTableHandlers() throws Throwable
    {
        ConcurrentMap<String,RoutingTableHandler> map = new ConcurrentHashMap<>();
        map.put( "Apple", mockedRoutingTableHandler( A ) );
        map.put( "Banana", mockedRoutingTableHandler( B ) );
        map.put( "Orange", mockedRoutingTableHandler( C ) );

        RoutingTableHandlerFactory factory = mockedHandlerFactory();
        RoutingTableRegistryImpl routingTables = newRoutingTables( map, factory );

        // When
        routingTables.purgeAged();
        // Then
        assertThat( routingTables.allServers(), empty() );
    }

    private RoutingTableHandler mockedRoutingTableHandler( BoltServerAddress... servers )
    {
        RoutingTableHandler handler = mock( RoutingTableHandler.class );
        when( handler.servers() ).thenReturn( new HashSet<>( Arrays.asList( servers ) ) );
        when( handler.isRoutingTableAged() ).thenReturn( true );
        return handler;
    }

    private RoutingTableRegistryImpl newRoutingTables( ConcurrentMap<String,RoutingTableHandler> handlers, RoutingTableHandlerFactory factory )
    {
        return new RoutingTableRegistryImpl( handlers, factory, DEV_NULL_LOGGER );
    }

    private RoutingTableHandlerFactory mockedHandlerFactory( RoutingTableHandler handler )
    {
        RoutingTableHandlerFactory factory = mock( RoutingTableHandlerFactory.class );
        when( factory.newInstance( any(), any() ) ).thenReturn( handler );
        return factory;
    }

    private RoutingTableHandlerFactory mockedHandlerFactory()
    {
        return mockedHandlerFactory( mockedRoutingTableHandler() );
    }

    private RoutingTableHandler mockedRoutingTableHandler()
    {
        RoutingTableHandler handler = mock( RoutingTableHandler.class );
        when( handler.refreshRoutingTable( any() ) ).thenReturn( completedFuture( mock( RoutingTable.class ) ) );
        return handler;
    }
}
