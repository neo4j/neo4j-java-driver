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
package org.neo4j.driver.internal.cluster.loadbalancing;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.AccessMode.READ;
import static org.neo4j.driver.AccessMode.WRITE;
import static org.neo4j.driver.internal.DatabaseNameUtil.defaultDatabase;
import static org.neo4j.driver.internal.async.ImmutableConnectionContext.simple;
import static org.neo4j.driver.internal.cluster.RediscoveryUtil.contextWithDatabase;
import static org.neo4j.driver.internal.cluster.RediscoveryUtil.contextWithMode;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.A;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.B;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.C;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.D;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.testutil.TestUtil.asOrderedSet;
import static org.neo4j.driver.testutil.TestUtil.await;

import io.netty.util.concurrent.GlobalEventExecutor;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.exceptions.AuthenticationException;
import org.neo4j.driver.exceptions.SecurityException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.DatabaseName;
import org.neo4j.driver.internal.DatabaseNameUtil;
import org.neo4j.driver.internal.async.ConnectionContext;
import org.neo4j.driver.internal.async.connection.RoutingConnection;
import org.neo4j.driver.internal.cluster.ClusterComposition;
import org.neo4j.driver.internal.cluster.ClusterRoutingTable;
import org.neo4j.driver.internal.cluster.Rediscovery;
import org.neo4j.driver.internal.cluster.RoutingTable;
import org.neo4j.driver.internal.cluster.RoutingTableHandler;
import org.neo4j.driver.internal.cluster.RoutingTableRegistry;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.messaging.v42.BoltProtocolV42;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.util.FakeClock;
import org.neo4j.driver.internal.util.Futures;

class LoadBalancerTest {
    @ParameterizedTest
    @EnumSource(AccessMode.class)
    void returnsCorrectAccessMode(AccessMode mode) {
        var connectionPool = newConnectionPoolMock();
        var routingTable = mock(RoutingTable.class);
        var readerAddresses = Collections.singletonList(A);
        var writerAddresses = Collections.singletonList(B);
        when(routingTable.readers()).thenReturn(readerAddresses);
        when(routingTable.writers()).thenReturn(writerAddresses);

        var loadBalancer = newLoadBalancer(connectionPool, routingTable);

        var acquired = await(loadBalancer.acquireConnection(contextWithMode(mode)));

        assertThat(acquired, instanceOf(RoutingConnection.class));
        assertThat(acquired.mode(), equalTo(mode));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "foo", "data"})
    void returnsCorrectDatabaseName(String databaseName) {
        var connectionPool = newConnectionPoolMock();
        var routingTable = mock(RoutingTable.class);
        var writerAddresses = Collections.singletonList(A);
        when(routingTable.writers()).thenReturn(writerAddresses);

        var loadBalancer = newLoadBalancer(connectionPool, routingTable);

        var acquired = await(loadBalancer.acquireConnection(contextWithDatabase(databaseName)));

        assertThat(acquired, instanceOf(RoutingConnection.class));
        assertThat(acquired.databaseName().description(), equalTo(databaseName));
        verify(connectionPool).acquire(A, null);
    }

    @Test
    void shouldThrowWhenRediscoveryReturnsNoSuitableServers() {
        var connectionPool = newConnectionPoolMock();
        var routingTable = mock(RoutingTable.class);
        when(routingTable.readers()).thenReturn(Collections.emptyList());
        when(routingTable.writers()).thenReturn(Collections.emptyList());

        var loadBalancer = newLoadBalancer(connectionPool, routingTable);

        var error1 = assertThrows(
                SessionExpiredException.class, () -> await(loadBalancer.acquireConnection(contextWithMode(READ))));
        assertThat(error1.getMessage(), startsWith("Failed to obtain connection towards READ server"));

        var error2 = assertThrows(
                SessionExpiredException.class, () -> await(loadBalancer.acquireConnection(contextWithMode(WRITE))));
        assertThat(error2.getMessage(), startsWith("Failed to obtain connection towards WRITE server"));
    }

    @Test
    void shouldSelectLeastConnectedAddress() {
        var connectionPool = newConnectionPoolMock();

        when(connectionPool.inUseConnections(A)).thenReturn(0);
        when(connectionPool.inUseConnections(B)).thenReturn(20);
        when(connectionPool.inUseConnections(C)).thenReturn(0);

        var routingTable = mock(RoutingTable.class);
        var readerAddresses = Arrays.asList(A, B, C);
        when(routingTable.readers()).thenReturn(readerAddresses);

        var loadBalancer = newLoadBalancer(connectionPool, routingTable);

        var seenAddresses = IntStream.range(0, 10)
                .mapToObj(i -> await(loadBalancer.acquireConnection(newBoltV4ConnectionContext())))
                .map(Connection::serverAddress)
                .collect(Collectors.toSet());

        // server B should never be selected because it has many active connections
        assertEquals(2, seenAddresses.size());
        assertTrue(seenAddresses.containsAll(asList(A, C)));
    }

    @Test
    void shouldRoundRobinWhenNoActiveConnections() {
        var connectionPool = newConnectionPoolMock();

        var routingTable = mock(RoutingTable.class);
        var readerAddresses = Arrays.asList(A, B, C);
        when(routingTable.readers()).thenReturn(readerAddresses);

        var loadBalancer = newLoadBalancer(connectionPool, routingTable);

        var seenAddresses = IntStream.range(0, 10)
                .mapToObj(i -> await(loadBalancer.acquireConnection(newBoltV4ConnectionContext())))
                .map(Connection::serverAddress)
                .collect(Collectors.toSet());

        assertEquals(3, seenAddresses.size());
        assertTrue(seenAddresses.containsAll(asList(A, B, C)));
    }

    @Test
    void shouldTryMultipleServersAfterRediscovery() {
        var unavailableAddresses = asOrderedSet(A);
        var connectionPool = newConnectionPoolMockWithFailures(unavailableAddresses);

        RoutingTable routingTable = new ClusterRoutingTable(defaultDatabase(), new FakeClock());
        routingTable.update(
                new ClusterComposition(-1, new LinkedHashSet<>(Arrays.asList(A, B)), emptySet(), emptySet(), null));

        var loadBalancer = newLoadBalancer(connectionPool, routingTable);

        var connection = await(loadBalancer.acquireConnection(newBoltV4ConnectionContext()));

        assertNotNull(connection);
        assertEquals(B, connection.serverAddress());
        // routing table should've forgotten A
        assertArrayEquals(new BoltServerAddress[] {B}, routingTable.readers().toArray());
    }

    @Test
    void shouldFailWithResolverError() throws Throwable {
        var pool = mock(ConnectionPool.class);
        var rediscovery = mock(Rediscovery.class);
        when(rediscovery.resolve()).thenThrow(new RuntimeException("hi there"));

        var loadBalancer = newLoadBalancer(pool, rediscovery);

        var exception = assertThrows(RuntimeException.class, () -> await(loadBalancer.supportsMultiDb()));
        assertThat(exception.getMessage(), equalTo("hi there"));
    }

    @Test
    void shouldFailAfterTryingAllServers() throws Throwable {
        var unavailableAddresses = asOrderedSet(A, B);
        var connectionPool = newConnectionPoolMockWithFailures(unavailableAddresses);

        var rediscovery = mock(Rediscovery.class);
        when(rediscovery.resolve()).thenReturn(Arrays.asList(A, B));

        var loadBalancer = newLoadBalancer(connectionPool, rediscovery);

        var exception = assertThrows(ServiceUnavailableException.class, () -> await(loadBalancer.supportsMultiDb()));
        var suppressed = exception.getSuppressed();
        assertThat(suppressed.length, equalTo(2)); // one for A, one for B
        assertThat(suppressed[0].getMessage(), containsString(A.toString()));
        assertThat(suppressed[1].getMessage(), containsString(B.toString()));
        verify(connectionPool, times(2)).acquire(any(), any());
    }

    @Test
    void shouldFailEarlyOnSecurityError() throws Throwable {
        var unavailableAddresses = asOrderedSet(A, B);
        var connectionPool = newConnectionPoolMockWithFailures(
                unavailableAddresses, address -> new SecurityException("code", "hi there"));

        var rediscovery = mock(Rediscovery.class);
        when(rediscovery.resolve()).thenReturn(Arrays.asList(A, B));

        var loadBalancer = newLoadBalancer(connectionPool, rediscovery);

        var exception = assertThrows(SecurityException.class, () -> await(loadBalancer.supportsMultiDb()));
        assertThat(exception.getMessage(), startsWith("hi there"));
        verify(connectionPool, times(1)).acquire(any(), any());
    }

    @Test
    void shouldSuccessOnFirstSuccessfulServer() throws Throwable {
        var unavailableAddresses = asOrderedSet(A, B);
        var connectionPool = newConnectionPoolMockWithFailures(unavailableAddresses);

        var rediscovery = mock(Rediscovery.class);
        when(rediscovery.resolve()).thenReturn(Arrays.asList(A, B, C, D));

        var loadBalancer = newLoadBalancer(connectionPool, rediscovery);

        assertTrue(await(loadBalancer.supportsMultiDb()));
        verify(connectionPool, times(3)).acquire(any(), any());
    }

    @Test
    void shouldThrowModifiedErrorWhenSupportMultiDbTestFails() throws Throwable {
        var unavailableAddresses = asOrderedSet(A, B);
        var connectionPool = newConnectionPoolMockWithFailures(unavailableAddresses);

        var rediscovery = mock(Rediscovery.class);
        when(rediscovery.resolve()).thenReturn(Arrays.asList(A, B));

        var loadBalancer = newLoadBalancer(connectionPool, rediscovery);

        var exception = assertThrows(ServiceUnavailableException.class, () -> await(loadBalancer.verifyConnectivity()));
        assertThat(exception.getMessage(), startsWith("Unable to connect to database management service,"));
    }

    @Test
    void shouldFailEarlyOnSecurityErrorWhenSupportMultiDbTestFails() throws Throwable {
        var unavailableAddresses = asOrderedSet(A, B);
        var connectionPool = newConnectionPoolMockWithFailures(
                unavailableAddresses, address -> new AuthenticationException("code", "error"));

        var rediscovery = mock(Rediscovery.class);
        when(rediscovery.resolve()).thenReturn(Arrays.asList(A, B));

        var loadBalancer = newLoadBalancer(connectionPool, rediscovery);

        var exception = assertThrows(AuthenticationException.class, () -> await(loadBalancer.verifyConnectivity()));
        assertThat(exception.getMessage(), startsWith("error"));
    }

    @Test
    void shouldThrowModifiedErrorWhenRefreshRoutingTableFails() throws Throwable {
        var connectionPool = newConnectionPoolMock();

        var rediscovery = mock(Rediscovery.class);
        when(rediscovery.resolve()).thenReturn(Arrays.asList(A, B));

        var routingTables = mock(RoutingTableRegistry.class);
        when(routingTables.ensureRoutingTable(any(ConnectionContext.class)))
                .thenThrow(new ServiceUnavailableException("boooo"));

        var loadBalancer = newLoadBalancer(connectionPool, routingTables, rediscovery);

        var exception = assertThrows(ServiceUnavailableException.class, () -> await(loadBalancer.verifyConnectivity()));
        assertThat(exception.getMessage(), startsWith("Unable to connect to database management service,"));
        verify(routingTables).ensureRoutingTable(any(ConnectionContext.class));
    }

    @Test
    void shouldThrowOriginalErrorWhenRefreshRoutingTableFails() throws Throwable {
        var connectionPool = newConnectionPoolMock();

        var rediscovery = mock(Rediscovery.class);
        when(rediscovery.resolve()).thenReturn(Arrays.asList(A, B));

        var routingTables = mock(RoutingTableRegistry.class);
        when(routingTables.ensureRoutingTable(any(ConnectionContext.class))).thenThrow(new RuntimeException("boo"));

        var loadBalancer = newLoadBalancer(connectionPool, routingTables, rediscovery);

        var exception = assertThrows(RuntimeException.class, () -> await(loadBalancer.verifyConnectivity()));
        assertThat(exception.getMessage(), startsWith("boo"));
        verify(routingTables).ensureRoutingTable(any(ConnectionContext.class));
    }

    @Test
    void shouldReturnSuccessVerifyConnectivity() throws Throwable {
        var connectionPool = newConnectionPoolMock();

        var rediscovery = mock(Rediscovery.class);
        when(rediscovery.resolve()).thenReturn(Arrays.asList(A, B));

        var routingTables = mock(RoutingTableRegistry.class);
        when(routingTables.ensureRoutingTable(any(ConnectionContext.class))).thenReturn(Futures.completedWithNull());

        var loadBalancer = newLoadBalancer(connectionPool, routingTables, rediscovery);

        await(loadBalancer.verifyConnectivity());
        verify(routingTables).ensureRoutingTable(any(ConnectionContext.class));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void expectsCompetedDatabaseNameAfterRoutingTableRegistry(boolean completed) throws Throwable {
        var connectionPool = newConnectionPoolMock();
        var routingTable = mock(RoutingTable.class);
        var readerAddresses = Collections.singletonList(A);
        var writerAddresses = Collections.singletonList(B);
        when(routingTable.readers()).thenReturn(readerAddresses);
        when(routingTable.writers()).thenReturn(writerAddresses);
        var routingTables = mock(RoutingTableRegistry.class);
        var handler = mock(RoutingTableHandler.class);
        when(handler.routingTable()).thenReturn(routingTable);
        when(routingTables.ensureRoutingTable(any(ConnectionContext.class)))
                .thenReturn(CompletableFuture.completedFuture(handler));
        var rediscovery = mock(Rediscovery.class);
        var loadBalancer = new LoadBalancer(
                connectionPool,
                routingTables,
                rediscovery,
                new LeastConnectedLoadBalancingStrategy(connectionPool, DEV_NULL_LOGGING),
                GlobalEventExecutor.INSTANCE,
                DEV_NULL_LOGGING);
        var context = mock(ConnectionContext.class);
        CompletableFuture<DatabaseName> databaseNameFuture = spy(new CompletableFuture<>());
        if (completed) {
            databaseNameFuture.complete(DatabaseNameUtil.systemDatabase());
        }
        when(context.databaseNameFuture()).thenReturn(databaseNameFuture);
        when(context.mode()).thenReturn(WRITE);

        Executable action = () -> await(loadBalancer.acquireConnection(context));
        if (completed) {
            action.execute();
        } else {
            assertThrows(
                    IllegalStateException.class,
                    action,
                    ConnectionContext.PENDING_DATABASE_NAME_EXCEPTION_SUPPLIER
                            .get()
                            .getMessage());
        }

        var inOrder = inOrder(routingTables, context, databaseNameFuture);
        inOrder.verify(routingTables).ensureRoutingTable(context);
        inOrder.verify(context).databaseNameFuture();
        inOrder.verify(databaseNameFuture).isDone();
        if (completed) {
            inOrder.verify(databaseNameFuture).join();
        }
    }

    @Test
    void shouldNotAcceptNullRediscovery() {
        // GIVEN
        var connectionPool = mock(ConnectionPool.class);
        var routingTables = mock(RoutingTableRegistry.class);

        // WHEN & THEN
        assertThrows(
                NullPointerException.class,
                () -> new LoadBalancer(
                        connectionPool,
                        routingTables,
                        null,
                        new LeastConnectedLoadBalancingStrategy(connectionPool, DEV_NULL_LOGGING),
                        GlobalEventExecutor.INSTANCE,
                        DEV_NULL_LOGGING));
    }

    private static ConnectionPool newConnectionPoolMock() {
        return newConnectionPoolMockWithFailures(emptySet());
    }

    private static ConnectionPool newConnectionPoolMockWithFailures(Set<BoltServerAddress> unavailableAddresses) {
        return newConnectionPoolMockWithFailures(
                unavailableAddresses, address -> new ServiceUnavailableException(address + " is unavailable!"));
    }

    private static ConnectionPool newConnectionPoolMockWithFailures(
            Set<BoltServerAddress> unavailableAddresses, Function<BoltServerAddress, Throwable> errorAction) {
        var pool = mock(ConnectionPool.class);
        when(pool.acquire(any(BoltServerAddress.class), any())).then(invocation -> {
            BoltServerAddress requestedAddress = invocation.getArgument(0);
            if (unavailableAddresses.contains(requestedAddress)) {
                return Futures.failedFuture(errorAction.apply(requestedAddress));
            }

            return completedFuture(newBoltV4Connection(requestedAddress));
        });
        return pool;
    }

    private static Connection newBoltV4Connection(BoltServerAddress address) {
        var connection = mock(Connection.class);
        when(connection.serverAddress()).thenReturn(address);
        when(connection.protocol()).thenReturn(BoltProtocol.forVersion(BoltProtocolV42.VERSION));
        when(connection.release()).thenReturn(completedWithNull());
        return connection;
    }

    private static ConnectionContext newBoltV4ConnectionContext() {
        return simple(true);
    }

    private static LoadBalancer newLoadBalancer(ConnectionPool connectionPool, RoutingTable routingTable) {
        // Used only in testing
        var routingTables = mock(RoutingTableRegistry.class);
        var handler = mock(RoutingTableHandler.class);
        when(handler.routingTable()).thenReturn(routingTable);
        when(routingTables.ensureRoutingTable(any(ConnectionContext.class)))
                .thenReturn(CompletableFuture.completedFuture(handler));
        var rediscovery = mock(Rediscovery.class);
        return new LoadBalancer(
                connectionPool,
                routingTables,
                rediscovery,
                new LeastConnectedLoadBalancingStrategy(connectionPool, DEV_NULL_LOGGING),
                GlobalEventExecutor.INSTANCE,
                DEV_NULL_LOGGING);
    }

    private static LoadBalancer newLoadBalancer(ConnectionPool connectionPool, Rediscovery rediscovery) {
        // Used only in testing
        var routingTables = mock(RoutingTableRegistry.class);
        return newLoadBalancer(connectionPool, routingTables, rediscovery);
    }

    private static LoadBalancer newLoadBalancer(
            ConnectionPool connectionPool, RoutingTableRegistry routingTables, Rediscovery rediscovery) {
        // Used only in testing
        return new LoadBalancer(
                connectionPool,
                routingTables,
                rediscovery,
                new LeastConnectedLoadBalancingStrategy(connectionPool, DEV_NULL_LOGGING),
                GlobalEventExecutor.INSTANCE,
                DEV_NULL_LOGGING);
    }
}
