/*
 * Copyright (c) "Neo4j"
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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.DatabaseNameUtil.SYSTEM_DATABASE_NAME;
import static org.neo4j.driver.internal.DatabaseNameUtil.database;
import static org.neo4j.driver.internal.DatabaseNameUtil.defaultDatabase;
import static org.neo4j.driver.internal.cluster.RoutingSettings.STALE_ROUTING_TABLE_PURGE_DELAY_MS;
import static org.neo4j.driver.internal.cluster.RoutingTableRegistryImpl.TABLE_ACQUISITION_TIMEOUT_MESSAGE;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.A;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.B;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.C;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.D;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.E;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.F;
import static org.neo4j.driver.util.TestUtil.await;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.DatabaseName;
import org.neo4j.driver.internal.async.ImmutableConnectionContext;
import org.neo4j.driver.internal.cluster.RoutingTableRegistryImpl.RoutingTableHandlerFactory;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.util.Clock;

class RoutingTableRegistryImplTest {
    @Test
    void factoryShouldCreateARoutingTableWithSameDatabaseName() throws Throwable {
        Clock clock = Clock.SYSTEM;
        RoutingTableHandlerFactory factory = new RoutingTableHandlerFactory(
                mock(ConnectionPool.class),
                mock(RediscoveryImpl.class),
                clock,
                DEV_NULL_LOGGING,
                STALE_ROUTING_TABLE_PURGE_DELAY_MS);

        RoutingTableHandler handler = factory.newInstance(database("Molly"), null);
        RoutingTable table = handler.routingTable();

        assertThat(table.database().description(), equalTo("Molly"));

        assertThat(table.routers().size(), equalTo(0));
        assertThat(table.readers().size(), equalTo(0));
        assertThat(table.writers().size(), equalTo(0));

        assertTrue(table.isStaleFor(AccessMode.READ));
        assertTrue(table.isStaleFor(AccessMode.WRITE));
    }

    @ParameterizedTest
    @ValueSource(strings = {SYSTEM_DATABASE_NAME, "", "database", " molly "})
    void shouldCreateRoutingTableHandlerIfAbsentWhenFreshRoutingTable(String databaseName) throws Throwable {
        // Given
        ConcurrentMap<DatabaseName, RoutingTableHandler> map = new ConcurrentHashMap<>();
        RoutingTableHandlerFactory factory = mockedHandlerFactory();
        RoutingTableRegistryImpl routingTables = newRoutingTables(map, factory);

        // When
        DatabaseName database = database(databaseName);
        routingTables.ensureRoutingTable(
                new ImmutableConnectionContext(database, Collections.emptySet(), AccessMode.READ));

        // Then
        assertTrue(map.containsKey(database));
        verify(factory).newInstance(eq(database), eq(routingTables));
    }

    @ParameterizedTest
    @ValueSource(strings = {SYSTEM_DATABASE_NAME, "", "database", " molly "})
    void shouldReturnExistingRoutingTableHandlerWhenFreshRoutingTable(String databaseName) throws Throwable {
        // Given
        ConcurrentMap<DatabaseName, RoutingTableHandler> map = new ConcurrentHashMap<>();
        RoutingTableHandler handler = mockedRoutingTableHandler();
        DatabaseName database = database(databaseName);
        map.put(database, handler);

        RoutingTableHandlerFactory factory = mockedHandlerFactory();
        RoutingTableRegistryImpl routingTables = newRoutingTables(map, factory);
        ImmutableConnectionContext context =
                new ImmutableConnectionContext(database, Collections.emptySet(), AccessMode.READ);

        // When
        RoutingTableHandler actual = await(routingTables.ensureRoutingTable(context));

        // Then it is the one we put in map that is picked up.
        verify(handler).ensureRoutingTable(context);
        // Then it is the one we put in map that is picked up.
        assertEquals(handler, actual);
    }

    @ParameterizedTest
    @EnumSource(AccessMode.class)
    void shouldReturnFreshRoutingTable(AccessMode mode) throws Throwable {
        // Given
        ConcurrentMap<DatabaseName, RoutingTableHandler> map = new ConcurrentHashMap<>();
        RoutingTableHandler handler = mockedRoutingTableHandler();
        RoutingTableHandlerFactory factory = mockedHandlerFactory(handler);
        RoutingTableRegistryImpl routingTables =
                new RoutingTableRegistryImpl(map, factory, Long.MAX_VALUE, null, null, null, DEV_NULL_LOGGING);

        ImmutableConnectionContext context =
                new ImmutableConnectionContext(defaultDatabase(), Collections.emptySet(), mode);
        // When
        routingTables.ensureRoutingTable(context);

        // Then
        verify(handler).ensureRoutingTable(context);
    }

    @Test
    void shouldReturnServersInAllRoutingTables() throws Throwable {
        // Given
        ConcurrentMap<DatabaseName, RoutingTableHandler> map = new ConcurrentHashMap<>();
        map.put(database("Apple"), mockedRoutingTableHandler(A, B, C));
        map.put(database("Banana"), mockedRoutingTableHandler(B, C, D));
        map.put(database("Orange"), mockedRoutingTableHandler(E, F, C));
        RoutingTableHandlerFactory factory = mockedHandlerFactory();
        RoutingTableRegistryImpl routingTables =
                new RoutingTableRegistryImpl(map, factory, Long.MAX_VALUE, null, null, null, DEV_NULL_LOGGING);

        // When
        Set<BoltServerAddress> servers = routingTables.allServers();

        // Then
        assertThat(servers, containsInAnyOrder(A, B, C, D, E, F));
    }

    @Test
    void shouldRemoveRoutingTableHandler() throws Throwable {
        // Given
        ConcurrentMap<DatabaseName, RoutingTableHandler> map = new ConcurrentHashMap<>();
        map.put(database("Apple"), mockedRoutingTableHandler(A));
        map.put(database("Banana"), mockedRoutingTableHandler(B));
        map.put(database("Orange"), mockedRoutingTableHandler(C));

        RoutingTableHandlerFactory factory = mockedHandlerFactory();
        RoutingTableRegistryImpl routingTables = newRoutingTables(map, factory);

        // When
        routingTables.remove(database("Apple"));
        routingTables.remove(database("Banana"));
        // Then
        assertThat(routingTables.allServers(), contains(C));
    }

    @Test
    void shouldRemoveStaleRoutingTableHandlers() throws Throwable {
        ConcurrentMap<DatabaseName, RoutingTableHandler> map = new ConcurrentHashMap<>();
        map.put(database("Apple"), mockedRoutingTableHandler(A));
        map.put(database("Banana"), mockedRoutingTableHandler(B));
        map.put(database("Orange"), mockedRoutingTableHandler(C));

        RoutingTableHandlerFactory factory = mockedHandlerFactory();
        RoutingTableRegistryImpl routingTables = newRoutingTables(map, factory);

        // When
        routingTables.removeAged();
        // Then
        assertThat(routingTables.allServers(), empty());
    }

    @Test
    void shouldReturnExistingRoutingTableHandlerWhenFreshRoutingTables() throws Throwable {
        // Given
        var map = new ConcurrentHashMap<DatabaseName, RoutingTableHandler>();
        var handler = mock(RoutingTableHandler.class);
        given(handler.ensureRoutingTable(any())).willReturn(new CompletableFuture<>());
        var database = database("neo4j");
        map.put(database, handler);

        var factory = mockedHandlerFactory();
        var routingTables = new RoutingTableRegistryImpl(map, factory, 250, null, null, null, DEV_NULL_LOGGING);
        var context = new ImmutableConnectionContext(database, Collections.emptySet(), AccessMode.READ);

        // When & Then
        var actual =
                assertThrows(ServiceUnavailableException.class, () -> await(routingTables.ensureRoutingTable(context)));
        assertEquals(TABLE_ACQUISITION_TIMEOUT_MESSAGE, actual.getMessage());
        assertInstanceOf(TimeoutException.class, actual.getCause());
    }

    private RoutingTableHandler mockedRoutingTableHandler(BoltServerAddress... servers) {
        RoutingTableHandler handler = mock(RoutingTableHandler.class);
        when(handler.servers()).thenReturn(new HashSet<>(Arrays.asList(servers)));
        when(handler.isRoutingTableAged()).thenReturn(true);
        return handler;
    }

    private RoutingTableRegistryImpl newRoutingTables(
            ConcurrentMap<DatabaseName, RoutingTableHandler> handlers, RoutingTableHandlerFactory factory) {
        return new RoutingTableRegistryImpl(handlers, factory, Long.MAX_VALUE, null, null, null, DEV_NULL_LOGGING);
    }

    private RoutingTableHandlerFactory mockedHandlerFactory(RoutingTableHandler handler) {
        RoutingTableHandlerFactory factory = mock(RoutingTableHandlerFactory.class);
        when(factory.newInstance(any(), any())).thenReturn(handler);
        return factory;
    }

    private RoutingTableHandlerFactory mockedHandlerFactory() {
        return mockedHandlerFactory(mockedRoutingTableHandler());
    }

    private RoutingTableHandler mockedRoutingTableHandler() {
        RoutingTableHandler handler = mock(RoutingTableHandler.class);
        when(handler.ensureRoutingTable(any())).thenReturn(completedFuture(mock(RoutingTable.class)));
        return handler;
    }
}
