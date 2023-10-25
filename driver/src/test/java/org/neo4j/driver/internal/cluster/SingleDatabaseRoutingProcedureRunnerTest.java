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

import static java.util.Collections.singletonList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.internal.DatabaseNameUtil.SYSTEM_DATABASE_NAME;
import static org.neo4j.driver.internal.DatabaseNameUtil.database;
import static org.neo4j.driver.internal.DatabaseNameUtil.defaultDatabase;
import static org.neo4j.driver.internal.cluster.SingleDatabaseRoutingProcedureRunner.GET_ROUTING_TABLE;
import static org.neo4j.driver.internal.cluster.SingleDatabaseRoutingProcedureRunner.ROUTING_CONTEXT;
import static org.neo4j.driver.testutil.TestUtil.await;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.exceptions.FatalDiscoveryException;
import org.neo4j.driver.internal.spi.Connection;

class SingleDatabaseRoutingProcedureRunnerTest extends AbstractRoutingProcedureRunnerTest {
    @Test
    void shouldCallGetRoutingTableWithEmptyMap() {
        var runner = new TestRoutingProcedureRunner(RoutingContext.EMPTY);
        var response = await(runner.run(connection(), defaultDatabase(), Collections.emptySet(), null));

        assertTrue(response.isSuccess());
        assertEquals(1, response.records().size());

        assertThat(runner.bookmarks, equalTo(Collections.emptySet()));
        assertThat(runner.connection.databaseName(), equalTo(defaultDatabase()));
        assertThat(runner.connection.mode(), equalTo(AccessMode.WRITE));

        var query = generateRoutingQuery(Collections.emptyMap());
        assertThat(runner.procedure, equalTo(query));
    }

    @Test
    void shouldCallGetRoutingTableWithParam() {
        var uri = URI.create("neo4j://localhost/?key1=value1&key2=value2");
        var context = new RoutingContext(uri);

        var runner = new TestRoutingProcedureRunner(context);
        var response = await(runner.run(connection(), defaultDatabase(), Collections.emptySet(), null));

        assertTrue(response.isSuccess());
        assertEquals(1, response.records().size());

        assertThat(runner.bookmarks, equalTo(Collections.emptySet()));
        assertThat(runner.connection.databaseName(), equalTo(defaultDatabase()));
        assertThat(runner.connection.mode(), equalTo(AccessMode.WRITE));

        var query = generateRoutingQuery(context.toMap());
        assertThat(response.procedure(), equalTo(query));
        assertThat(runner.procedure, equalTo(query));
    }

    @ParameterizedTest
    @MethodSource("invalidDatabaseNames")
    void shouldErrorWhenDatabaseIsNotAbsent(String db) {
        var runner = new TestRoutingProcedureRunner(RoutingContext.EMPTY);
        assertThrows(
                FatalDiscoveryException.class,
                () -> await(runner.run(connection(), database(db), Collections.emptySet(), null)));
    }

    SingleDatabaseRoutingProcedureRunner singleDatabaseRoutingProcedureRunner() {
        return new TestRoutingProcedureRunner(RoutingContext.EMPTY);
    }

    SingleDatabaseRoutingProcedureRunner singleDatabaseRoutingProcedureRunner(
            CompletionStage<List<Record>> runProcedureResult) {
        return new TestRoutingProcedureRunner(RoutingContext.EMPTY, runProcedureResult);
    }

    private static Stream<String> invalidDatabaseNames() {
        return Stream.of(SYSTEM_DATABASE_NAME, "This is a string", "null");
    }

    private static Query generateRoutingQuery(Map<String, String> context) {
        var parameters = parameters(ROUTING_CONTEXT, context);
        return new Query(GET_ROUTING_TABLE, parameters);
    }

    private static class TestRoutingProcedureRunner extends SingleDatabaseRoutingProcedureRunner {
        final CompletionStage<List<Record>> runProcedureResult;
        private Connection connection;
        private Query procedure;
        private Set<Bookmark> bookmarks;

        TestRoutingProcedureRunner(RoutingContext context) {
            this(context, completedFuture(singletonList(mock(Record.class))));
        }

        TestRoutingProcedureRunner(RoutingContext context, CompletionStage<List<Record>> runProcedureResult) {
            super(context, Logging.none());
            this.runProcedureResult = runProcedureResult;
        }

        @Override
        CompletionStage<List<Record>> runProcedure(Connection connection, Query procedure, Set<Bookmark> bookmarks) {
            this.connection = connection;
            this.procedure = procedure;
            this.bookmarks = bookmarks;
            return runProcedureResult;
        }
    }
}
