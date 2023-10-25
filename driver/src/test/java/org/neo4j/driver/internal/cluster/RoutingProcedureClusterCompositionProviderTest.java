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
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.internal.DatabaseNameUtil.defaultDatabase;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.internal.util.Futures.failedFuture;
import static org.neo4j.driver.testutil.TestUtil.await;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ProtocolException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.DatabaseName;
import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.messaging.v3.BoltProtocolV3;
import org.neo4j.driver.internal.messaging.v4.BoltProtocolV4;
import org.neo4j.driver.internal.messaging.v43.BoltProtocolV43;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.value.StringValue;

class RoutingProcedureClusterCompositionProviderTest {
    @Test
    void shouldProtocolErrorWhenNoRecord() {
        // Given
        var mockedRunner = newProcedureRunnerMock();
        var connection = mock(Connection.class);
        ClusterCompositionProvider provider = newClusterCompositionProvider(mockedRunner, connection);

        var noRecordsResponse = newRoutingResponse();
        when(mockedRunner.run(eq(connection), any(DatabaseName.class), any(), any()))
                .thenReturn(completedFuture(noRecordsResponse));

        // When & Then
        var error = assertThrows(
                ProtocolException.class,
                () -> await(
                        provider.getClusterComposition(connection, defaultDatabase(), Collections.emptySet(), null)));
        assertThat(error.getMessage(), containsString("records received '0' is too few or too many."));
    }

    @Test
    void shouldProtocolErrorWhenMoreThanOneRecord() {
        // Given
        var mockedRunner = newProcedureRunnerMock();
        var connection = mock(Connection.class);
        ClusterCompositionProvider provider = newClusterCompositionProvider(mockedRunner, connection);

        Record aRecord = new InternalRecord(asList("key1", "key2"), new Value[] {new StringValue("a value")});
        var routingResponse = newRoutingResponse(aRecord, aRecord);
        when(mockedRunner.run(eq(connection), any(DatabaseName.class), any(), any()))
                .thenReturn(completedFuture(routingResponse));

        // When
        var error = assertThrows(
                ProtocolException.class,
                () -> await(
                        provider.getClusterComposition(connection, defaultDatabase(), Collections.emptySet(), null)));
        assertThat(error.getMessage(), containsString("records received '2' is too few or too many."));
    }

    @Test
    void shouldProtocolErrorWhenUnparsableRecord() {
        // Given
        var mockedRunner = newProcedureRunnerMock();
        var connection = mock(Connection.class);
        ClusterCompositionProvider provider = newClusterCompositionProvider(mockedRunner, connection);

        Record aRecord = new InternalRecord(asList("key1", "key2"), new Value[] {new StringValue("a value")});
        var routingResponse = newRoutingResponse(aRecord);
        when(mockedRunner.run(eq(connection), any(DatabaseName.class), any(), any()))
                .thenReturn(completedFuture(routingResponse));

        // When
        var error = assertThrows(
                ProtocolException.class,
                () -> await(
                        provider.getClusterComposition(connection, defaultDatabase(), Collections.emptySet(), null)));
        assertThat(error.getMessage(), containsString("unparsable record received."));
    }

    @Test
    void shouldProtocolErrorWhenNoRouters() {
        // Given
        var mockedRunner = newMultiDBProcedureRunnerMock();
        var connection = mock(Connection.class);
        var mockedClock = mock(Clock.class);
        ClusterCompositionProvider provider = newClusterCompositionProvider(mockedRunner, connection, mockedClock);

        Record record = new InternalRecord(asList("ttl", "servers"), new Value[] {
            value(100), value(asList(serverInfo("READ", "one:1337", "two:1337"), serverInfo("WRITE", "one:1337")))
        });
        var routingResponse = newRoutingResponse(record);
        when(mockedRunner.run(eq(connection), any(DatabaseName.class), any(), any()))
                .thenReturn(completedFuture(routingResponse));
        when(mockedClock.millis()).thenReturn(12345L);

        // When
        var error = assertThrows(
                ProtocolException.class,
                () -> await(
                        provider.getClusterComposition(connection, defaultDatabase(), Collections.emptySet(), null)));
        assertThat(error.getMessage(), containsString("no router or reader found in response."));
    }

    @Test
    void routeMessageRoutingProcedureShouldProtocolErrorWhenNoRouters() {
        // Given
        var mockedRunner = newRouteMessageRoutingProcedureRunnerMock();
        var connection = mock(Connection.class);
        var mockedClock = mock(Clock.class);
        ClusterCompositionProvider provider = newClusterCompositionProvider(mockedRunner, connection, mockedClock);

        Record record = new InternalRecord(asList("ttl", "servers"), new Value[] {
            value(100), value(asList(serverInfo("READ", "one:1337", "two:1337"), serverInfo("WRITE", "one:1337")))
        });
        var routingResponse = newRoutingResponse(record);
        when(mockedRunner.run(eq(connection), any(DatabaseName.class), any(), any()))
                .thenReturn(completedFuture(routingResponse));
        when(mockedClock.millis()).thenReturn(12345L);

        // When
        var error = assertThrows(
                ProtocolException.class,
                () -> await(
                        provider.getClusterComposition(connection, defaultDatabase(), Collections.emptySet(), null)));
        assertThat(error.getMessage(), containsString("no router or reader found in response."));
    }

    @Test
    void shouldProtocolErrorWhenNoReaders() {
        // Given
        var mockedRunner = newMultiDBProcedureRunnerMock();
        var connection = mock(Connection.class);
        var mockedClock = mock(Clock.class);
        ClusterCompositionProvider provider = newClusterCompositionProvider(mockedRunner, connection, mockedClock);

        Record record = new InternalRecord(asList("ttl", "servers"), new Value[] {
            value(100), value(asList(serverInfo("WRITE", "one:1337"), serverInfo("ROUTE", "one:1337", "two:1337")))
        });
        var routingResponse = newRoutingResponse(record);
        when(mockedRunner.run(eq(connection), any(DatabaseName.class), any(), any()))
                .thenReturn(completedFuture(routingResponse));
        when(mockedClock.millis()).thenReturn(12345L);

        // When
        var error = assertThrows(
                ProtocolException.class,
                () -> await(
                        provider.getClusterComposition(connection, defaultDatabase(), Collections.emptySet(), null)));
        assertThat(error.getMessage(), containsString("no router or reader found in response."));
    }

    @Test
    void routeMessageRoutingProcedureShouldProtocolErrorWhenNoReaders() {
        // Given
        var mockedRunner = newRouteMessageRoutingProcedureRunnerMock();
        var connection = mock(Connection.class);
        var mockedClock = mock(Clock.class);
        ClusterCompositionProvider provider = newClusterCompositionProvider(mockedRunner, connection, mockedClock);

        Record record = new InternalRecord(asList("ttl", "servers"), new Value[] {
            value(100), value(asList(serverInfo("WRITE", "one:1337"), serverInfo("ROUTE", "one:1337", "two:1337")))
        });
        var routingResponse = newRoutingResponse(record);
        when(mockedRunner.run(eq(connection), any(DatabaseName.class), any(), any()))
                .thenReturn(completedFuture(routingResponse));
        when(mockedClock.millis()).thenReturn(12345L);

        // When
        var error = assertThrows(
                ProtocolException.class,
                () -> await(
                        provider.getClusterComposition(connection, defaultDatabase(), Collections.emptySet(), null)));
        assertThat(error.getMessage(), containsString("no router or reader found in response."));
    }

    @Test
    void shouldPropagateConnectionFailureExceptions() {
        // Given
        var mockedRunner = newProcedureRunnerMock();
        var connection = mock(Connection.class);
        ClusterCompositionProvider provider = newClusterCompositionProvider(mockedRunner, connection);

        when(mockedRunner.run(eq(connection), any(DatabaseName.class), any(), any()))
                .thenReturn(failedFuture(new ServiceUnavailableException("Connection breaks during cypher execution")));

        // When & Then
        var e = assertThrows(
                ServiceUnavailableException.class,
                () -> await(
                        provider.getClusterComposition(connection, defaultDatabase(), Collections.emptySet(), null)));
        assertThat(e.getMessage(), containsString("Connection breaks during cypher execution"));
    }

    @Test
    void shouldReturnSuccessResultWhenNoError() {
        // Given
        var mockedClock = mock(Clock.class);
        var connection = mock(Connection.class);
        var mockedRunner = newMultiDBProcedureRunnerMock();
        ClusterCompositionProvider provider = newClusterCompositionProvider(mockedRunner, connection, mockedClock);

        Record record = new InternalRecord(asList("ttl", "servers"), new Value[] {
            value(100),
            value(asList(
                    serverInfo("READ", "one:1337", "two:1337"),
                    serverInfo("WRITE", "one:1337"),
                    serverInfo("ROUTE", "one:1337", "two:1337")))
        });
        var routingResponse = newRoutingResponse(record);
        when(mockedRunner.run(eq(connection), any(DatabaseName.class), any(), any()))
                .thenReturn(completedFuture(routingResponse));
        when(mockedClock.millis()).thenReturn(12345L);

        // When
        var cluster =
                await(provider.getClusterComposition(connection, defaultDatabase(), Collections.emptySet(), null));

        // Then
        assertEquals(12345 + 100_000, cluster.expirationTimestamp());
        assertEquals(serverSet("one:1337", "two:1337"), cluster.readers());
        assertEquals(serverSet("one:1337"), cluster.writers());
        assertEquals(serverSet("one:1337", "two:1337"), cluster.routers());
    }

    @Test
    void routeMessageRoutingProcedureShouldReturnSuccessResultWhenNoError() {
        // Given
        var mockedClock = mock(Clock.class);
        var connection = mock(Connection.class);
        var mockedRunner = newRouteMessageRoutingProcedureRunnerMock();
        ClusterCompositionProvider provider = newClusterCompositionProvider(mockedRunner, connection, mockedClock);

        Record record = new InternalRecord(asList("ttl", "servers"), new Value[] {
            value(100),
            value(asList(
                    serverInfo("READ", "one:1337", "two:1337"),
                    serverInfo("WRITE", "one:1337"),
                    serverInfo("ROUTE", "one:1337", "two:1337")))
        });
        var routingResponse = newRoutingResponse(record);
        when(mockedRunner.run(eq(connection), any(DatabaseName.class), any(), any()))
                .thenReturn(completedFuture(routingResponse));
        when(mockedClock.millis()).thenReturn(12345L);

        // When
        var cluster =
                await(provider.getClusterComposition(connection, defaultDatabase(), Collections.emptySet(), null));

        // Then
        assertEquals(12345 + 100_000, cluster.expirationTimestamp());
        assertEquals(serverSet("one:1337", "two:1337"), cluster.readers());
        assertEquals(serverSet("one:1337"), cluster.writers());
        assertEquals(serverSet("one:1337", "two:1337"), cluster.routers());
    }

    @Test
    void shouldReturnFailureWhenProcedureRunnerFails() {
        var procedureRunner = newProcedureRunnerMock();
        var connection = mock(Connection.class);

        var error = new RuntimeException("hi");
        when(procedureRunner.run(eq(connection), any(DatabaseName.class), any(), any()))
                .thenReturn(completedFuture(newRoutingResponse(error)));

        var provider = newClusterCompositionProvider(procedureRunner, connection);

        var e = assertThrows(
                RuntimeException.class,
                () -> await(
                        provider.getClusterComposition(connection, defaultDatabase(), Collections.emptySet(), null)));
        assertEquals(error, e);
    }

    @Test
    void shouldUseMultiDBProcedureRunnerWhenConnectingWith40Server() {
        var procedureRunner = newMultiDBProcedureRunnerMock();
        var connection = mock(Connection.class);

        var provider = newClusterCompositionProvider(procedureRunner, connection);

        when(procedureRunner.run(eq(connection), any(DatabaseName.class), any(), any()))
                .thenReturn(completedWithNull());
        provider.getClusterComposition(connection, defaultDatabase(), Collections.emptySet(), null);

        verify(procedureRunner).run(eq(connection), any(DatabaseName.class), any(), any());
    }

    @Test
    void shouldUseProcedureRunnerWhenConnectingWith35AndPreviousServers() {
        var procedureRunner = newProcedureRunnerMock();
        var connection = mock(Connection.class);

        var provider = newClusterCompositionProvider(procedureRunner, connection);

        when(procedureRunner.run(eq(connection), any(DatabaseName.class), any(), any()))
                .thenReturn(completedWithNull());
        provider.getClusterComposition(connection, defaultDatabase(), Collections.emptySet(), null);

        verify(procedureRunner).run(eq(connection), any(DatabaseName.class), any(), any());
    }

    @Test
    void shouldUseRouteMessageProcedureRunnerWhenConnectingWithProtocol43() {
        var procedureRunner = newRouteMessageRoutingProcedureRunnerMock();
        var connection = mock(Connection.class);

        var provider = newClusterCompositionProvider(procedureRunner, connection);

        when(procedureRunner.run(eq(connection), any(DatabaseName.class), any(), any()))
                .thenReturn(completedWithNull());
        provider.getClusterComposition(connection, defaultDatabase(), Collections.emptySet(), null);

        verify(procedureRunner).run(eq(connection), any(DatabaseName.class), any(), any());
    }

    private static Map<String, Object> serverInfo(String role, String... addresses) {
        Map<String, Object> map = new HashMap<>();
        map.put("role", role);
        map.put("addresses", asList(addresses));
        return map;
    }

    private static Set<BoltServerAddress> serverSet(String... addresses) {
        return Arrays.stream(addresses).map(BoltServerAddress::new).collect(Collectors.toSet());
    }

    private static SingleDatabaseRoutingProcedureRunner newProcedureRunnerMock() {
        return mock(SingleDatabaseRoutingProcedureRunner.class);
    }

    private static MultiDatabasesRoutingProcedureRunner newMultiDBProcedureRunnerMock() {
        return mock(MultiDatabasesRoutingProcedureRunner.class);
    }

    private static RouteMessageRoutingProcedureRunner newRouteMessageRoutingProcedureRunnerMock() {
        return mock(RouteMessageRoutingProcedureRunner.class);
    }

    private static RoutingProcedureResponse newRoutingResponse(Record... records) {
        return new RoutingProcedureResponse(new Query("procedure"), asList(records));
    }

    private static RoutingProcedureResponse newRoutingResponse(Throwable error) {
        return new RoutingProcedureResponse(new Query("procedure"), error);
    }

    private static RoutingProcedureClusterCompositionProvider newClusterCompositionProvider(
            SingleDatabaseRoutingProcedureRunner runner, Connection connection) {
        when(connection.protocol()).thenReturn(BoltProtocolV3.INSTANCE);
        return new RoutingProcedureClusterCompositionProvider(
                mock(Clock.class),
                runner,
                newMultiDBProcedureRunnerMock(),
                newRouteMessageRoutingProcedureRunnerMock());
    }

    private static RoutingProcedureClusterCompositionProvider newClusterCompositionProvider(
            MultiDatabasesRoutingProcedureRunner runner, Connection connection) {
        when(connection.protocol()).thenReturn(BoltProtocolV4.INSTANCE);
        return new RoutingProcedureClusterCompositionProvider(
                mock(Clock.class), newProcedureRunnerMock(), runner, newRouteMessageRoutingProcedureRunnerMock());
    }

    private static RoutingProcedureClusterCompositionProvider newClusterCompositionProvider(
            MultiDatabasesRoutingProcedureRunner runner, Connection connection, Clock clock) {
        when(connection.protocol()).thenReturn(BoltProtocolV4.INSTANCE);
        return new RoutingProcedureClusterCompositionProvider(
                clock, newProcedureRunnerMock(), runner, newRouteMessageRoutingProcedureRunnerMock());
    }

    private static RoutingProcedureClusterCompositionProvider newClusterCompositionProvider(
            RouteMessageRoutingProcedureRunner runner, Connection connection) {

        return newClusterCompositionProvider(runner, connection, mock(Clock.class));
    }

    private static RoutingProcedureClusterCompositionProvider newClusterCompositionProvider(
            RouteMessageRoutingProcedureRunner runner, Connection connection, Clock clock) {
        when(connection.protocol()).thenReturn(BoltProtocolV43.INSTANCE);
        return new RoutingProcedureClusterCompositionProvider(
                clock, newProcedureRunnerMock(), newMultiDBProcedureRunnerMock(), runner);
    }
}
