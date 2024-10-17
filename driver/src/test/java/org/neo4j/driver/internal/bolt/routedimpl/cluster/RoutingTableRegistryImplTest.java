/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.driver.internal.bolt.routedimpl.cluster;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.RoutingSettings.STALE_ROUTING_TABLE_PURGE_DELAY_MS;
import static org.neo4j.driver.internal.bolt.api.DatabaseNameUtil.SYSTEM_DATABASE_NAME;
import static org.neo4j.driver.internal.bolt.api.DatabaseNameUtil.database;
import static org.neo4j.driver.internal.bolt.api.DatabaseNameUtil.defaultDatabase;
import static org.neo4j.driver.internal.bolt.api.util.ClusterCompositionUtil.A;
import static org.neo4j.driver.internal.bolt.api.util.ClusterCompositionUtil.B;
import static org.neo4j.driver.internal.bolt.api.util.ClusterCompositionUtil.C;
import static org.neo4j.driver.internal.bolt.api.util.ClusterCompositionUtil.D;
import static org.neo4j.driver.internal.bolt.api.util.ClusterCompositionUtil.E;
import static org.neo4j.driver.internal.bolt.api.util.ClusterCompositionUtil.F;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.bolt.NoopLoggingProvider;
import org.neo4j.driver.internal.bolt.api.AccessMode;
import org.neo4j.driver.internal.bolt.api.BoltProtocolVersion;
import org.neo4j.driver.internal.bolt.api.BoltServerAddress;
import org.neo4j.driver.internal.bolt.api.DatabaseName;
import org.neo4j.driver.internal.bolt.api.SecurityPlan;

class RoutingTableRegistryImplTest {
    @Test
    void factoryShouldCreateARoutingTableWithSameDatabaseName() {
        var clock = Clock.systemUTC();
        var factory = new RoutingTableRegistryImpl.RoutingTableHandlerFactory(
                mock(),
                Mockito.mock(RediscoveryImpl.class),
                clock,
                NoopLoggingProvider.INSTANCE,
                STALE_ROUTING_TABLE_PURGE_DELAY_MS,
                ignored -> {});

        var handler = factory.newInstance(database("Molly"), null);
        var table = handler.routingTable();

        assertThat(table.database().description(), equalTo("Molly"));

        assertThat(table.routers().size(), equalTo(0));
        assertThat(table.readers().size(), equalTo(0));
        assertThat(table.writers().size(), equalTo(0));

        assertTrue(table.isStaleFor(AccessMode.READ));
        assertTrue(table.isStaleFor(AccessMode.WRITE));
    }

    @ParameterizedTest
    @ValueSource(strings = {SYSTEM_DATABASE_NAME, "", "database", " molly "})
    void shouldCreateRoutingTableHandlerIfAbsentWhenFreshRoutingTable(String databaseName) {
        // Given
        ConcurrentMap<DatabaseName, RoutingTableHandler> map = new ConcurrentHashMap<>();
        var factory = mockedHandlerFactory();
        var routingTables = newRoutingTables(map, factory);

        // When
        var database = database(databaseName);
        routingTables.ensureRoutingTable(
                SecurityPlan.INSECURE,
                CompletableFuture.completedFuture(database),
                AccessMode.READ,
                Collections.emptySet(),
                null,
                () -> CompletableFuture.completedStage(Collections.emptyMap()),
                new BoltProtocolVersion(4, 1));

        // Then
        assertTrue(map.containsKey(database));
        verify(factory).newInstance(eq(database), eq(routingTables));
    }

    @ParameterizedTest
    @ValueSource(strings = {SYSTEM_DATABASE_NAME, "", "database", " molly "})
    void shouldReturnExistingRoutingTableHandlerWhenFreshRoutingTable(String databaseName) {
        // Given
        ConcurrentMap<DatabaseName, RoutingTableHandler> map = new ConcurrentHashMap<>();
        var handler = mockedRoutingTableHandler();
        var database = database(databaseName);
        map.put(database, handler);

        var factory = mockedHandlerFactory();
        var routingTables = newRoutingTables(map, factory);
        Supplier<CompletionStage<Map<String, Value>>> authStageSupplier =
                () -> CompletableFuture.completedStage(Collections.emptyMap());

        // When
        var actual = routingTables
                .ensureRoutingTable(
                        SecurityPlan.INSECURE,
                        CompletableFuture.completedFuture(database),
                        AccessMode.READ,
                        Collections.emptySet(),
                        null,
                        authStageSupplier,
                        new BoltProtocolVersion(4, 1))
                .toCompletableFuture()
                .join();

        // Then it is the one we put in map that is picked up.
        verify(handler)
                .ensureRoutingTable(
                        SecurityPlan.INSECURE,
                        AccessMode.READ,
                        Collections.emptySet(),
                        authStageSupplier,
                        new BoltProtocolVersion(4, 1));
        // Then it is the one we put in map that is picked up.
        assertEquals(handler, actual);
    }

    @ParameterizedTest
    @EnumSource(AccessMode.class)
    void shouldReturnFreshRoutingTable(AccessMode mode) {
        // Given
        ConcurrentMap<DatabaseName, RoutingTableHandler> map = new ConcurrentHashMap<>();
        var handler = mockedRoutingTableHandler();
        var factory = mockedHandlerFactory(handler);
        var routingTables = new RoutingTableRegistryImpl(
                map, factory, null, null, Mockito.mock(Rediscovery.class), NoopLoggingProvider.INSTANCE);
        Supplier<CompletionStage<Map<String, Value>>> authStageSupplier =
                () -> CompletableFuture.completedStage(Collections.emptyMap());

        // When
        routingTables
                .ensureRoutingTable(
                        SecurityPlan.INSECURE,
                        CompletableFuture.completedFuture(defaultDatabase()),
                        mode,
                        Collections.emptySet(),
                        null,
                        authStageSupplier,
                        new BoltProtocolVersion(4, 1))
                .toCompletableFuture()
                .join();

        // Then
        verify(handler)
                .ensureRoutingTable(
                        SecurityPlan.INSECURE,
                        mode,
                        Collections.emptySet(),
                        authStageSupplier,
                        new BoltProtocolVersion(4, 1));
    }

    @Test
    void shouldReturnServersInAllRoutingTables() {
        // Given
        ConcurrentMap<DatabaseName, RoutingTableHandler> map = new ConcurrentHashMap<>();
        map.put(database("Apple"), mockedRoutingTableHandler(A, B, C));
        map.put(database("Banana"), mockedRoutingTableHandler(B, C, D));
        map.put(database("Orange"), mockedRoutingTableHandler(E, F, C));
        var factory = mockedHandlerFactory();
        var routingTables = new RoutingTableRegistryImpl(
                map, factory, null, null, Mockito.mock(Rediscovery.class), NoopLoggingProvider.INSTANCE);

        // When
        var servers = routingTables.allServers();

        // Then
        assertThat(servers, containsInAnyOrder(A, B, C, D, E, F));
    }

    @Test
    void shouldRemoveRoutingTableHandler() {
        // Given
        ConcurrentMap<DatabaseName, RoutingTableHandler> map = new ConcurrentHashMap<>();
        map.put(database("Apple"), mockedRoutingTableHandler(A));
        map.put(database("Banana"), mockedRoutingTableHandler(B));
        map.put(database("Orange"), mockedRoutingTableHandler(C));

        var factory = mockedHandlerFactory();
        var routingTables = newRoutingTables(map, factory);

        // When
        routingTables.remove(database("Apple"));
        routingTables.remove(database("Banana"));
        // Then
        assertThat(routingTables.allServers(), contains(C));
    }

    @Test
    void shouldRemoveStaleRoutingTableHandlers() {
        ConcurrentMap<DatabaseName, RoutingTableHandler> map = new ConcurrentHashMap<>();
        map.put(database("Apple"), mockedRoutingTableHandler(A));
        map.put(database("Banana"), mockedRoutingTableHandler(B));
        map.put(database("Orange"), mockedRoutingTableHandler(C));

        var factory = mockedHandlerFactory();
        var routingTables = newRoutingTables(map, factory);

        // When
        routingTables.removeAged();
        // Then
        assertThat(routingTables.allServers(), empty());
    }

    @Test
    void shouldNotAcceptNullRediscovery() {
        // GIVEN
        var factory = mockedHandlerFactory();
        var clock = mock(Clock.class);

        // WHEN & THEN
        assertThrows(
                NullPointerException.class,
                () -> new RoutingTableRegistryImpl(
                        new ConcurrentHashMap<>(), factory, clock, mock(), null, NoopLoggingProvider.INSTANCE));
    }

    private RoutingTableHandler mockedRoutingTableHandler(BoltServerAddress... servers) {
        var handler = Mockito.mock(RoutingTableHandler.class);
        when(handler.servers()).thenReturn(new HashSet<>(Arrays.asList(servers)));
        when(handler.isRoutingTableAged()).thenReturn(true);
        return handler;
    }

    private RoutingTableRegistryImpl newRoutingTables(
            ConcurrentMap<DatabaseName, RoutingTableHandler> handlers,
            RoutingTableRegistryImpl.RoutingTableHandlerFactory factory) {
        return new RoutingTableRegistryImpl(
                handlers, factory, null, null, Mockito.mock(Rediscovery.class), NoopLoggingProvider.INSTANCE);
    }

    private RoutingTableRegistryImpl.RoutingTableHandlerFactory mockedHandlerFactory(RoutingTableHandler handler) {
        var factory = mock(RoutingTableRegistryImpl.RoutingTableHandlerFactory.class);
        when(factory.newInstance(any(), any())).thenReturn(handler);
        return factory;
    }

    private RoutingTableRegistryImpl.RoutingTableHandlerFactory mockedHandlerFactory() {
        return mockedHandlerFactory(mockedRoutingTableHandler());
    }

    private RoutingTableHandler mockedRoutingTableHandler() {
        var handler = Mockito.mock(RoutingTableHandler.class);
        when(handler.ensureRoutingTable(any(), any(), any(), any(), any()))
                .thenReturn(completedFuture(Mockito.mock(RoutingTable.class)));
        return handler;
    }
}
