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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.neo4j.bolt.connection.TelemetryApi;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.BookmarkManager;
import org.neo4j.driver.Driver;
import org.neo4j.driver.ExecutableQuery;
import org.neo4j.driver.Query;
import org.neo4j.driver.QueryConfig;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.RoutingControl;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.TransactionCallback;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.TransactionContext;
import org.neo4j.driver.summary.ResultSummary;

class InternalExecutableQueryTest {
    @Test
    void shouldNotAcceptNullDriverOnInstantiation() {
        assertThrows(
                NullPointerException.class,
                () -> new InternalExecutableQuery(null, new Query("string"), QueryConfig.defaultConfig(), null));
    }

    @Test
    void shouldNotAcceptNullQueryOnInstantiation() {
        assertThrows(
                NullPointerException.class,
                () -> new InternalExecutableQuery(mock(Driver.class), null, QueryConfig.defaultConfig(), null));
    }

    @Test
    void shouldNotAcceptNullConfigOnInstantiation() {
        assertThrows(
                NullPointerException.class,
                () -> new InternalExecutableQuery(mock(Driver.class), new Query("string"), null, null));
    }

    @Test
    void shouldNotAcceptNullParameters() {
        var executableQuery =
                new InternalExecutableQuery(mock(Driver.class), new Query("string"), QueryConfig.defaultConfig(), null);
        assertThrows(NullPointerException.class, () -> executableQuery.withParameters(null));
    }

    @Test
    void shouldUpdateParameters() {
        // GIVEN
        var query = new Query("string");
        var params = Map.<String, Object>of("$param", "value");
        var executableQuery = new InternalExecutableQuery(mock(Driver.class), query, QueryConfig.defaultConfig(), null);

        // WHEN
        executableQuery = (InternalExecutableQuery) executableQuery.withParameters(params);

        // THEN
        assertEquals(params, executableQuery.parameters());
    }

    @Test
    void shouldNotAcceptNullConfig() {
        var executableQuery =
                new InternalExecutableQuery(mock(Driver.class), new Query("string"), QueryConfig.defaultConfig(), null);
        assertThrows(NullPointerException.class, () -> executableQuery.withConfig(null));
    }

    @Test
    void shouldUpdateConfig() {
        // GIVEN
        var query = new Query("string");
        var executableQuery = new InternalExecutableQuery(mock(Driver.class), query, QueryConfig.defaultConfig(), null);
        var config = QueryConfig.builder().withDatabase("database").build();

        // WHEN
        executableQuery = (InternalExecutableQuery) executableQuery.withConfig(config);

        // THEN
        assertEquals(config, executableQuery.config());
    }

    static List<RoutingControl> routingControls() {
        return List.of(RoutingControl.READ, RoutingControl.WRITE);
    }

    @ParameterizedTest
    @MethodSource("routingControls")
    @SuppressWarnings({"unchecked", "resource"})
    void shouldExecuteAndReturnResult(RoutingControl routingControl) {
        // GIVEN
        var driver = mock(Driver.class);
        var bookmarkManager = mock(BookmarkManager.class);
        given(driver.executableQueryBookmarkManager()).willReturn(bookmarkManager);
        var session = mock(InternalSession.class);
        given(driver.session(eq(Session.class), any(SessionConfig.class), eq(null)))
                .willReturn(session);
        var txContext = mock(TransactionContext.class);
        var accessMode = routingControl.equals(RoutingControl.WRITE) ? AccessMode.WRITE : AccessMode.READ;
        given(session.execute(
                        eq(accessMode),
                        any(),
                        eq(TransactionConfig.empty()),
                        eq(TelemetryApi.EXECUTABLE_QUERY),
                        eq(false)))
                .willAnswer(answer -> {
                    TransactionCallback<?> txCallback = answer.getArgument(1);
                    return txCallback.execute(txContext);
                });
        var result = mock(Result.class);
        given(txContext.run(any(Query.class))).willReturn(result);
        var keys = List.of("key");
        given(result.keys()).willReturn(keys);
        given(result.hasNext()).willReturn(true, false);
        var record = mock(Record.class);
        given(result.next()).willReturn(record);
        var summary = mock(ResultSummary.class);
        given(result.consume()).willReturn(summary);
        var query = new Query("string");
        var params = Map.<String, Object>of("$param", "value");
        var config = QueryConfig.builder()
                .withDatabase("db")
                .withImpersonatedUser("user")
                .withRouting(routingControl)
                .build();
        Collector<Record, Object, String> recordCollector = mock(Collector.class);
        var resultContainer = new Object();
        given(recordCollector.supplier()).willReturn(() -> resultContainer);
        BiConsumer<Object, Record> accumulator = mock(BiConsumer.class);
        given(recordCollector.accumulator()).willReturn(accumulator);
        var collectorResult = "0";
        Function<Object, String> finisher = mock(Function.class);
        given(finisher.apply(resultContainer)).willReturn(collectorResult);
        given(recordCollector.finisher()).willReturn(finisher);
        ExecutableQuery.ResultFinisher<String, String> finisherWithSummary = mock(ExecutableQuery.ResultFinisher.class);
        var expectedExecuteResult = "1";
        given(finisherWithSummary.finish(any(List.class), any(String.class), any(ResultSummary.class)))
                .willReturn(expectedExecuteResult);
        var executableQuery = new InternalExecutableQuery(driver, query, config, null).withParameters(params);

        // WHEN
        var executeResult = executableQuery.execute(recordCollector, finisherWithSummary);

        // THEN
        var sessionConfigCapture = ArgumentCaptor.forClass(SessionConfig.class);
        then(driver).should().session(eq(Session.class), sessionConfigCapture.capture(), eq(null));
        var sessionConfig = sessionConfigCapture.getValue();
        @SuppressWarnings("OptionalGetWithoutIsPresent")
        var expectedSessionConfig = SessionConfig.builder()
                .withDatabase(config.database().get())
                .withImpersonatedUser(config.impersonatedUser().get())
                .withBookmarkManager(bookmarkManager)
                .build();
        assertEquals(expectedSessionConfig, sessionConfig);
        then(session)
                .should()
                .execute(
                        eq(accessMode),
                        any(),
                        eq(TransactionConfig.empty()),
                        eq(TelemetryApi.EXECUTABLE_QUERY),
                        eq(false));
        then(txContext).should().run(query.withParameters(params));
        then(result).should(times(2)).hasNext();
        then(result).should().next();
        then(result).should().consume();
        then(recordCollector).should().supplier();
        then(recordCollector).should().accumulator();
        then(accumulator).should().accept(resultContainer, record);
        then(recordCollector).should().finisher();
        then(finisher).should().apply(resultContainer);
        then(finisherWithSummary).should().finish(keys, collectorResult, summary);
        assertEquals(expectedExecuteResult, executeResult);
    }

    @Test
    void shouldAllowNullAuthToken() {
        var executableQuery =
                new InternalExecutableQuery(mock(Driver.class), new Query("string"), QueryConfig.defaultConfig(), null);

        executableQuery.withAuthToken(null);

        assertNull(executableQuery.authToken());
    }

    @Test
    void shouldUpdateAuthToken() {
        var executableQuery =
                new InternalExecutableQuery(mock(Driver.class), new Query("string"), QueryConfig.defaultConfig(), null);
        var authToken = AuthTokens.basic("user", "password");

        executableQuery = (InternalExecutableQuery) executableQuery.withAuthToken(authToken);

        assertEquals(authToken, executableQuery.authToken());
    }
}
