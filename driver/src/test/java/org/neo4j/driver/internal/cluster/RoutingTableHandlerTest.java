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
package org.neo4j.driver.internal.cluster;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.AccessMode.READ;
import static org.neo4j.driver.AccessMode.WRITE;
import static org.neo4j.driver.internal.BoltServerAddress.LOCAL_DEFAULT;
import static org.neo4j.driver.internal.DatabaseNameUtil.defaultDatabase;
import static org.neo4j.driver.internal.async.ImmutableConnectionContext.simple;
import static org.neo4j.driver.internal.cluster.RediscoveryUtil.contextWithMode;
import static org.neo4j.driver.internal.cluster.RoutingSettings.STALE_ROUTING_TABLE_PURGE_DELAY_MS;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.A;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.B;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.C;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.D;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.E;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.F;
import static org.neo4j.driver.testutil.TestUtil.asOrderedSet;
import static org.neo4j.driver.testutil.TestUtil.await;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.DatabaseName;
import org.neo4j.driver.internal.async.ConnectionContext;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.util.FakeClock;
import org.neo4j.driver.internal.util.Futures;

class RoutingTableHandlerTest {
    @Test
    void shouldRemoveAddressFromRoutingTableOnConnectionFailure() {
        RoutingTable routingTable = new ClusterRoutingTable(defaultDatabase(), new FakeClock());
        routingTable.update(
                new ClusterComposition(42, asOrderedSet(A, B, C), asOrderedSet(A, C, E), asOrderedSet(B, D, F), null));

        var handler = newRoutingTableHandler(routingTable, newRediscoveryMock(), newConnectionPoolMock());

        handler.onConnectionFailure(B);

        assertArrayEquals(new BoltServerAddress[] {A, C}, routingTable.readers().toArray());
        assertArrayEquals(
                new BoltServerAddress[] {A, C, E}, routingTable.writers().toArray());
        assertArrayEquals(new BoltServerAddress[] {D, F}, routingTable.routers().toArray());

        handler.onConnectionFailure(A);

        assertArrayEquals(new BoltServerAddress[] {C}, routingTable.readers().toArray());
        assertArrayEquals(new BoltServerAddress[] {C, E}, routingTable.writers().toArray());
        assertArrayEquals(new BoltServerAddress[] {D, F}, routingTable.routers().toArray());
    }

    @Test
    void acquireShouldUpdateRoutingTableWhenKnownRoutingTableIsStale() {
        var initialRouter = new BoltServerAddress("initialRouter", 1);
        var reader1 = new BoltServerAddress("reader-1", 2);
        var reader2 = new BoltServerAddress("reader-1", 3);
        var writer1 = new BoltServerAddress("writer-1", 4);
        var router1 = new BoltServerAddress("router-1", 5);

        var connectionPool = newConnectionPoolMock();
        var routingTable = new ClusterRoutingTable(defaultDatabase(), new FakeClock(), initialRouter);

        Set<BoltServerAddress> readers = new LinkedHashSet<>(asList(reader1, reader2));
        Set<BoltServerAddress> writers = new LinkedHashSet<>(singletonList(writer1));
        Set<BoltServerAddress> routers = new LinkedHashSet<>(singletonList(router1));
        var clusterComposition = new ClusterComposition(42, readers, writers, routers, null);
        Rediscovery rediscovery = mock(RediscoveryImpl.class);
        when(rediscovery.lookupClusterComposition(eq(routingTable), eq(connectionPool), any(), any(), any()))
                .thenReturn(completedFuture(new ClusterCompositionLookupResult(clusterComposition)));

        var handler = newRoutingTableHandler(routingTable, rediscovery, connectionPool);

        assertNotNull(await(handler.ensureRoutingTable(simple(false))));

        verify(rediscovery).lookupClusterComposition(eq(routingTable), eq(connectionPool), any(), any(), any());
        assertArrayEquals(
                new BoltServerAddress[] {reader1, reader2},
                routingTable.readers().toArray());
        assertArrayEquals(
                new BoltServerAddress[] {writer1}, routingTable.writers().toArray());
        assertArrayEquals(
                new BoltServerAddress[] {router1}, routingTable.routers().toArray());
    }

    @Test
    void shouldRediscoverOnReadWhenRoutingTableIsStaleForReads() {
        testRediscoveryWhenStale(READ);
    }

    @Test
    void shouldRediscoverOnWriteWhenRoutingTableIsStaleForWrites() {
        testRediscoveryWhenStale(WRITE);
    }

    @Test
    void shouldNotRediscoverOnReadWhenRoutingTableIsStaleForWritesButNotReads() {
        testNoRediscoveryWhenNotStale(WRITE, READ);
    }

    @Test
    void shouldNotRediscoverOnWriteWhenRoutingTableIsStaleForReadsButNotWrites() {
        testNoRediscoveryWhenNotStale(READ, WRITE);
    }

    @Test
    void shouldRetainAllFetchedAddressesInConnectionPoolAfterFetchingOfRoutingTable() {
        RoutingTable routingTable = new ClusterRoutingTable(defaultDatabase(), new FakeClock());
        routingTable.update(new ClusterComposition(42, asOrderedSet(), asOrderedSet(B, C), asOrderedSet(D, E), null));

        var connectionPool = newConnectionPoolMock();

        var rediscovery = newRediscoveryMock();
        when(rediscovery.lookupClusterComposition(any(), any(), any(), any(), any()))
                .thenReturn(completedFuture(new ClusterCompositionLookupResult(
                        new ClusterComposition(42, asOrderedSet(A, B), asOrderedSet(B, C), asOrderedSet(A, C), null))));

        var registry = new RoutingTableRegistry() {
            @Override
            public CompletionStage<RoutingTableHandler> ensureRoutingTable(ConnectionContext context) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Set<BoltServerAddress> allServers() {
                return routingTable.servers();
            }

            @Override
            public void remove(DatabaseName databaseName) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void removeAged() {}

            @Override
            public Optional<RoutingTableHandler> getRoutingTableHandler(DatabaseName databaseName) {
                return Optional.empty();
            }
        };

        var handler = newRoutingTableHandler(routingTable, rediscovery, connectionPool, registry);

        var actual = await(handler.ensureRoutingTable(simple(false)));
        assertEquals(routingTable, actual);

        verify(connectionPool).retainAll(new HashSet<>(asList(A, B, C)));
    }

    @Test
    void shouldRemoveRoutingTableHandlerIfFailedToLookup() {
        // Given
        RoutingTable routingTable = new ClusterRoutingTable(defaultDatabase(), new FakeClock());

        var rediscovery = newRediscoveryMock();
        when(rediscovery.lookupClusterComposition(any(), any(), any(), any(), any()))
                .thenReturn(Futures.failedFuture(new RuntimeException("Bang!")));

        var connectionPool = newConnectionPoolMock();
        var registry = newRoutingTableRegistryMock();
        // When

        var handler = newRoutingTableHandler(routingTable, rediscovery, connectionPool, registry);
        assertThrows(RuntimeException.class, () -> await(handler.ensureRoutingTable(simple(false))));

        // Then
        verify(registry).remove(defaultDatabase());
    }

    private void testRediscoveryWhenStale(AccessMode mode) {
        var connectionPool = mock(ConnectionPool.class);
        when(connectionPool.acquire(LOCAL_DEFAULT, null)).thenReturn(completedFuture(mock(Connection.class)));

        var routingTable = newStaleRoutingTableMock(mode);
        var rediscovery = newRediscoveryMock();

        var handler = newRoutingTableHandler(routingTable, rediscovery, connectionPool);
        var actual = await(handler.ensureRoutingTable(contextWithMode(mode)));
        assertEquals(routingTable, actual);

        verify(routingTable).isStaleFor(mode);
        verify(rediscovery).lookupClusterComposition(eq(routingTable), eq(connectionPool), any(), any(), any());
    }

    private void testNoRediscoveryWhenNotStale(AccessMode staleMode, AccessMode notStaleMode) {
        var connectionPool = mock(ConnectionPool.class);
        when(connectionPool.acquire(LOCAL_DEFAULT, null)).thenReturn(completedFuture(mock(Connection.class)));

        var routingTable = newStaleRoutingTableMock(staleMode);
        var rediscovery = newRediscoveryMock();

        var handler = newRoutingTableHandler(routingTable, rediscovery, connectionPool);

        assertNotNull(await(handler.ensureRoutingTable(contextWithMode(notStaleMode))));
        verify(routingTable).isStaleFor(notStaleMode);
        verify(rediscovery, never())
                .lookupClusterComposition(eq(routingTable), eq(connectionPool), any(), any(), any());
    }

    private static RoutingTable newStaleRoutingTableMock(AccessMode mode) {
        var routingTable = mock(RoutingTable.class);
        when(routingTable.isStaleFor(mode)).thenReturn(true);

        var addresses = singletonList(LOCAL_DEFAULT);
        when(routingTable.readers()).thenReturn(addresses);
        when(routingTable.writers()).thenReturn(addresses);
        when(routingTable.database()).thenReturn(defaultDatabase());

        return routingTable;
    }

    private static RoutingTableRegistry newRoutingTableRegistryMock() {
        return mock(RoutingTableRegistry.class);
    }

    private static Rediscovery newRediscoveryMock() {
        Rediscovery rediscovery = mock(RediscoveryImpl.class);
        Set<BoltServerAddress> noServers = Collections.emptySet();
        var clusterComposition = new ClusterComposition(1, noServers, noServers, noServers, null);
        when(rediscovery.lookupClusterComposition(
                        any(RoutingTable.class), any(ConnectionPool.class), any(), any(), any()))
                .thenReturn(completedFuture(new ClusterCompositionLookupResult(clusterComposition)));
        return rediscovery;
    }

    private static ConnectionPool newConnectionPoolMock() {
        return newConnectionPoolMockWithFailures(emptySet());
    }

    private static ConnectionPool newConnectionPoolMockWithFailures(Set<BoltServerAddress> unavailableAddresses) {
        var pool = mock(ConnectionPool.class);
        when(pool.acquire(any(BoltServerAddress.class), any())).then(invocation -> {
            BoltServerAddress requestedAddress = invocation.getArgument(0);
            if (unavailableAddresses.contains(requestedAddress)) {
                return Futures.failedFuture(new ServiceUnavailableException(requestedAddress + " is unavailable!"));
            }
            var connection = mock(Connection.class);
            when(connection.serverAddress()).thenReturn(requestedAddress);
            return completedFuture(connection);
        });
        return pool;
    }

    private static RoutingTableHandler newRoutingTableHandler(
            RoutingTable routingTable, Rediscovery rediscovery, ConnectionPool connectionPool) {
        return new RoutingTableHandlerImpl(
                routingTable,
                rediscovery,
                connectionPool,
                newRoutingTableRegistryMock(),
                DEV_NULL_LOGGING,
                STALE_ROUTING_TABLE_PURGE_DELAY_MS);
    }

    private static RoutingTableHandler newRoutingTableHandler(
            RoutingTable routingTable,
            Rediscovery rediscovery,
            ConnectionPool connectionPool,
            RoutingTableRegistry routingTableRegistry) {
        return new RoutingTableHandlerImpl(
                routingTable,
                rediscovery,
                connectionPool,
                routingTableRegistry,
                DEV_NULL_LOGGING,
                STALE_ROUTING_TABLE_PURGE_DELAY_MS);
    }
}
