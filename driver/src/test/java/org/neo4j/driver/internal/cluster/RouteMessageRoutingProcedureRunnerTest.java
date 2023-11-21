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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.DatabaseName;
import org.neo4j.driver.internal.DatabaseNameUtil;
import org.neo4j.driver.internal.handlers.RouteMessageResponseHandler;
import org.neo4j.driver.internal.messaging.request.RouteMessage;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.testutil.TestUtil;

class RouteMessageRoutingProcedureRunnerTest {

    private static Stream<Arguments> shouldRequestRoutingTableForAllValidInputScenarios() {
        return Stream.of(
                Arguments.arguments(RoutingContext.EMPTY, DatabaseNameUtil.defaultDatabase()),
                Arguments.arguments(RoutingContext.EMPTY, DatabaseNameUtil.systemDatabase()),
                Arguments.arguments(RoutingContext.EMPTY, DatabaseNameUtil.database("neo4j")),
                Arguments.arguments(
                        new RoutingContext(URI.create("localhost:17601")), DatabaseNameUtil.defaultDatabase()),
                Arguments.arguments(
                        new RoutingContext(URI.create("localhost:17602")), DatabaseNameUtil.systemDatabase()),
                Arguments.arguments(
                        new RoutingContext(URI.create("localhost:17603")), DatabaseNameUtil.database("neo4j")));
    }

    @ParameterizedTest
    @MethodSource
    void shouldRequestRoutingTableForAllValidInputScenarios(RoutingContext routingContext, DatabaseName databaseName) {
        var routingTable = getRoutingTable();
        var completableFuture = CompletableFuture.completedFuture(routingTable);
        var runner = new RouteMessageRoutingProcedureRunner(routingContext, () -> completableFuture);
        var connection = mock(Connection.class);
        CompletableFuture<Void> releaseConnectionFuture = CompletableFuture.completedFuture(null);
        doReturn(releaseConnectionFuture).when(connection).release();

        var response = TestUtil.await(runner.run(connection, databaseName, null, null));

        assertNotNull(response);
        assertTrue(response.isSuccess());
        assertNotNull(response.procedure());
        assertEquals(1, response.records().size());
        assertNotNull(response.records().get(0));

        var record = response.records().get(0);
        assertEquals(routingTable.get("ttl"), record.get("ttl"));
        assertEquals(routingTable.get("servers"), record.get("servers"));

        verifyMessageWasWrittenAndFlushed(connection, completableFuture, routingContext, databaseName);
        verify(connection).release();
    }

    @Test
    void shouldReturnFailureWhenSomethingHappensGettingTheRoutingTable() {
        Throwable reason = new RuntimeException("Some error");
        var completableFuture = new CompletableFuture<Map<String, Value>>();
        completableFuture.completeExceptionally(reason);
        var runner = new RouteMessageRoutingProcedureRunner(RoutingContext.EMPTY, () -> completableFuture);
        var connection = mock(Connection.class);
        CompletableFuture<Void> releaseConnectionFuture = CompletableFuture.completedFuture(null);
        doReturn(releaseConnectionFuture).when(connection).release();

        var response = TestUtil.await(runner.run(connection, DatabaseNameUtil.defaultDatabase(), null, null));

        assertNotNull(response);
        assertFalse(response.isSuccess());
        assertNotNull(response.procedure());
        assertEquals(reason, response.error());
        assertThrows(IllegalStateException.class, () -> response.records().size());

        verifyMessageWasWrittenAndFlushed(
                connection, completableFuture, RoutingContext.EMPTY, DatabaseNameUtil.defaultDatabase());
        verify(connection).release();
    }

    private void verifyMessageWasWrittenAndFlushed(
            Connection connection,
            CompletableFuture<Map<String, Value>> completableFuture,
            RoutingContext routingContext,
            DatabaseName databaseName) {
        var context = routingContext.toMap().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> Values.value(entry.getValue())));

        verify(connection)
                .writeAndFlush(
                        eq(new RouteMessage(
                                context, null, databaseName.databaseName().orElse(null), null)),
                        eq(new RouteMessageResponseHandler(completableFuture)));
    }

    private Map<String, Value> getRoutingTable() {
        Map<String, Value> routingTable = new HashMap<>();
        routingTable.put("ttl", Values.value(300));
        routingTable.put("servers", Values.value(getServers()));
        return routingTable;
    }

    private List<Map<String, Value>> getServers() {
        List<Map<String, Value>> servers = new ArrayList<>();
        servers.add(getServer("WRITE", "localhost:17601"));
        servers.add(getServer("READ", "localhost:17601", "localhost:17602", "localhost:17603"));
        servers.add(getServer("ROUTE", "localhost:17601", "localhost:17602", "localhost:17603"));
        return servers;
    }

    private Map<String, Value> getServer(String role, String... addresses) {
        Map<String, Value> server = new HashMap<>();
        server.put("role", Values.value(role));
        server.put("addresses", Values.value(addresses));
        return server;
    }
}
