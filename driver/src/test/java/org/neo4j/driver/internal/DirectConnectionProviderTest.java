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
package org.neo4j.driver.internal;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.AccessMode.READ;
import static org.neo4j.driver.AccessMode.WRITE;
import static org.neo4j.driver.internal.cluster.RediscoveryUtil.contextWithDatabase;
import static org.neo4j.driver.internal.cluster.RediscoveryUtil.contextWithMode;
import static org.neo4j.driver.testutil.TestUtil.await;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.internal.async.ConnectionContext;
import org.neo4j.driver.internal.async.connection.DirectConnection;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;

class DirectConnectionProviderTest {
    @Test
    void acquiresConnectionsFromThePool() {
        var address = BoltServerAddress.LOCAL_DEFAULT;
        var connection1 = mock(Connection.class);
        var connection2 = mock(Connection.class);

        var pool = poolMock(address, connection1, connection2);
        var provider = new DirectConnectionProvider(address, pool);

        var acquired1 = await(provider.acquireConnection(contextWithMode(READ)));
        assertThat(acquired1, instanceOf(DirectConnection.class));
        assertSame(connection1, ((DirectConnection) acquired1).connection());

        var acquired2 = await(provider.acquireConnection(contextWithMode(WRITE)));
        assertThat(acquired2, instanceOf(DirectConnection.class));
        assertSame(connection2, ((DirectConnection) acquired2).connection());
    }

    @ParameterizedTest
    @EnumSource(AccessMode.class)
    void returnsCorrectAccessMode(AccessMode mode) {
        var address = BoltServerAddress.LOCAL_DEFAULT;
        var pool = poolMock(address, mock(Connection.class));
        var provider = new DirectConnectionProvider(address, pool);

        var acquired = await(provider.acquireConnection(contextWithMode(mode)));

        assertEquals(mode, acquired.mode());
    }

    @Test
    void closesPool() {
        var address = BoltServerAddress.LOCAL_DEFAULT;
        var pool = poolMock(address, mock(Connection.class));
        var provider = new DirectConnectionProvider(address, pool);

        provider.close();

        verify(pool).close();
    }

    @Test
    void returnsCorrectAddress() {
        var address = new BoltServerAddress("server-1", 25000);

        var provider = new DirectConnectionProvider(address, mock(ConnectionPool.class));

        assertEquals(address, provider.getAddress());
    }

    @Test
    void shouldIgnoreDatabaseNameAndAccessModeWhenObtainConnectionFromPool() {
        var address = BoltServerAddress.LOCAL_DEFAULT;
        var connection = mock(Connection.class);

        var pool = poolMock(address, connection);
        var provider = new DirectConnectionProvider(address, pool);

        var acquired1 = await(provider.acquireConnection(contextWithMode(READ)));
        assertThat(acquired1, instanceOf(DirectConnection.class));
        assertSame(connection, ((DirectConnection) acquired1).connection());

        verify(pool).acquire(address, null);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "foo", "data"})
    void shouldObtainDatabaseNameOnConnection(String databaseName) {
        var address = BoltServerAddress.LOCAL_DEFAULT;
        var pool = poolMock(address, mock(Connection.class));
        var provider = new DirectConnectionProvider(address, pool);

        var acquired = await(provider.acquireConnection(contextWithDatabase(databaseName)));

        assertEquals(databaseName, acquired.databaseName().description());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void ensuresCompletedDatabaseNameBeforeAccessingValue(boolean completed) {
        var address = BoltServerAddress.LOCAL_DEFAULT;
        var pool = poolMock(address, mock(Connection.class));
        var provider = new DirectConnectionProvider(address, pool);
        var context = mock(ConnectionContext.class);
        CompletableFuture<DatabaseName> databaseNameFuture = spy(
                completed
                        ? CompletableFuture.completedFuture(DatabaseNameUtil.systemDatabase())
                        : new CompletableFuture<>());
        when(context.databaseNameFuture()).thenReturn(databaseNameFuture);
        when(context.mode()).thenReturn(WRITE);

        await(provider.acquireConnection(context));

        var inOrder = inOrder(context, databaseNameFuture);
        inOrder.verify(context).databaseNameFuture();
        inOrder.verify(databaseNameFuture).complete(DatabaseNameUtil.defaultDatabase());
        inOrder.verify(databaseNameFuture).isDone();
        inOrder.verify(databaseNameFuture).join();
    }

    @SuppressWarnings("unchecked")
    private static ConnectionPool poolMock(
            BoltServerAddress address, Connection connection, Connection... otherConnections) {
        var pool = mock(ConnectionPool.class);
        CompletableFuture<Connection>[] otherConnectionFutures = Stream.of(otherConnections)
                .map(CompletableFuture::completedFuture)
                .toArray(CompletableFuture[]::new);
        when(pool.acquire(address, null)).thenReturn(completedFuture(connection), otherConnectionFutures);
        return pool;
    }
}
