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
import org.neo4j.driver.internal.RoutingErrorHandler;
import org.neo4j.driver.internal.cluster.RoutingTablesImpl.RoutingTableHandlerFactory;
import org.neo4j.driver.internal.spi.ConnectionPool;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.logging.DevNullLogger.DEV_NULL_LOGGER;
import static org.neo4j.driver.internal.messaging.request.MultiDatabaseUtil.ABSENT_DB_NAME;
import static org.neo4j.driver.internal.messaging.request.MultiDatabaseUtil.SYSTEM_DB_NAME;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.A;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.B;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.C;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.D;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.E;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.F;

class RoutingTablesImplTest
{
    @Test
    void factoryShouldCreateARoutingTableWithSameDatabaseName() throws Throwable
    {
        RoutingTableFactory routingTableFactory = db -> {
            assertThat( db, equalTo( "Molly" ) );
            RoutingTable table = mock( RoutingTable.class );
            when( table.database() ).thenReturn( db );
            return table;
        };

        RoutingTableHandlerFactory factory =
                new RoutingTableHandlerFactory( mock( ConnectionPool.class ), routingTableFactory, mock( Rediscovery.class ), DEV_NULL_LOGGER );

        factory.newInstance( "Molly", null );
    }

    @ParameterizedTest
    @ValueSource( strings = {ABSENT_DB_NAME, SYSTEM_DB_NAME, "", "database", " molly "} )
    void shouldCreateRoutingTableHandlerIfAbsentWhenFreshRoutingTable( String databaseName ) throws Throwable
    {
        // Given
        ConcurrentMap<String,RoutingTableHandler> map = new ConcurrentHashMap<>();
        RoutingTableHandlerFactory factory = mockedHandlerFactory();
        RoutingTablesImpl routingTables = newRoutingTables( map, factory );

        // When
        routingTables.freshRoutingTable( databaseName, AccessMode.READ );

        // Then
        assertTrue( map.containsKey( databaseName ) );
        verify( factory ).newInstance( eq( databaseName ), eq( routingTables ) );
    }

    @ParameterizedTest
    @ValueSource( strings = {ABSENT_DB_NAME, SYSTEM_DB_NAME, "", "database", " molly "} )
    void shouldCreateRoutingTableHandlerIfAbsentWhenGetRoutingErrorHandler( String databaseName ) throws Throwable
    {
        // Given
        ConcurrentMap<String,RoutingTableHandler> map = new ConcurrentHashMap<>();
        RoutingTableHandlerFactory factory = mockedHandlerFactory();
        RoutingTablesImpl routingTables = newRoutingTables( map, factory );

        // When
        routingTables.routingErrorHandler( databaseName );

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
        RoutingTablesImpl routingTables = newRoutingTables( map, factory );

        // When
        routingTables.freshRoutingTable( databaseName, AccessMode.READ );

        // Then it is the one we put in map that is picked up.
        verify( handler ).freshRoutingTable( AccessMode.READ );
    }

    @ParameterizedTest
    @ValueSource( strings = {ABSENT_DB_NAME, SYSTEM_DB_NAME, "", "database", " molly "} )
    void shouldReturnExistingRoutingTableHandlerWhenGetRoutingErrorHandler( String databaseName ) throws Throwable
    {
        // Given
        ConcurrentMap<String,RoutingTableHandler> map = new ConcurrentHashMap<>();
        RoutingTableHandler handler = mockedRoutingTableHandler();
        map.put( databaseName, handler );

        RoutingTableHandlerFactory factory = mockedHandlerFactory();
        RoutingTablesImpl routingTables = newRoutingTables( map, factory );

        // When
        RoutingErrorHandler actual = routingTables.routingErrorHandler( databaseName );

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
        RoutingTablesImpl routingTables = new RoutingTablesImpl( map, factory );

        // When
        routingTables.freshRoutingTable( ABSENT_DB_NAME, mode );

        // Then
        verify( handler ).freshRoutingTable( mode );
    }

    @Test
    void shouldReturnServersInAllRoutingTables() throws Throwable
    {
        // Given
        ConcurrentMap<String,RoutingTableHandler> map = new ConcurrentHashMap<>();
        map.put( "Apple", mockedRoutingTable( A, B, C ) );
        map.put( "Banana", mockedRoutingTable( B, C, D ) );
        map.put( "Orange", mockedRoutingTable( E, F, C ) );
        RoutingTableHandlerFactory factory = mockedHandlerFactory();
        RoutingTablesImpl routingTables = new RoutingTablesImpl( map, factory );

        // When
        Set<BoltServerAddress> servers = routingTables.allServers();

        // Then
        assertThat( servers, containsInAnyOrder( A, B, C, D, E, F ) );
    }

    private RoutingTableHandler mockedRoutingTable( BoltServerAddress... servers )
    {
        RoutingTableHandler handler = mock( RoutingTableHandler.class );
        when( handler.servers() ).thenReturn( new HashSet<>( Arrays.asList( servers ) ) );
        return handler;
    }

    private RoutingTablesImpl newRoutingTables( ConcurrentMap<String,RoutingTableHandler> handlers, RoutingTableHandlerFactory factory )
    {
        return new RoutingTablesImpl( handlers, factory );
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
        when( handler.freshRoutingTable( any() ) ).thenReturn( completedFuture( mock( RoutingTable.class ) ) );
        return handler;
    }
}
