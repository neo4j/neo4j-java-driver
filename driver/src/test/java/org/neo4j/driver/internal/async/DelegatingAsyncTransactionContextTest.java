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
package org.neo4j.driver.internal.async;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.async.AsyncTransaction;
import org.neo4j.driver.async.ResultCursor;

public class DelegatingAsyncTransactionContextTest {
    AsyncTransaction transaction;
    DelegatingAsyncTransactionContext context;

    @BeforeEach
    void beforeEach() {
        transaction = mock(AsyncTransaction.class);
        context = new DelegatingAsyncTransactionContext(transaction);
    }

    @Test
    void shouldDelegateRunWithValueParams() {
        // GIVEN
        var query = "something";
        var params = mock(Value.class);
        CompletionStage<ResultCursor> expected = CompletableFuture.completedFuture(null);
        given(transaction.runAsync(query, params)).willReturn(expected);

        // WHEN
        var actual = context.runAsync(query, params);

        // THEN
        assertEquals(expected, actual);
        then(transaction).should().runAsync(query, params);
    }

    @Test
    void shouldDelegateRunWithMapParams() {
        // GIVEN
        var query = "something";
        Map<String, Object> params = Collections.emptyMap();
        CompletionStage<ResultCursor> expected = CompletableFuture.completedFuture(null);
        given(transaction.runAsync(query, params)).willReturn(expected);

        // WHEN
        var actual = context.runAsync(query, params);

        // THEN
        assertEquals(expected, actual);
        then(transaction).should().runAsync(query, params);
    }

    @Test
    void shouldDelegateRunWithRecordParams() {
        // GIVEN
        var query = "something";
        var params = mock(Record.class);
        CompletionStage<ResultCursor> expected = CompletableFuture.completedFuture(null);
        given(transaction.runAsync(query, params)).willReturn(expected);

        // WHEN
        var actual = context.runAsync(query, params);

        // THEN
        assertEquals(expected, actual);
        then(transaction).should().runAsync(query, params);
    }

    @Test
    void shouldDelegateRun() {
        // GIVEN
        var query = "something";
        CompletionStage<ResultCursor> expected = CompletableFuture.completedFuture(null);
        given(transaction.runAsync(query)).willReturn(expected);

        // WHEN
        var actual = context.runAsync(query);

        // THEN
        assertEquals(expected, actual);
        then(transaction).should().runAsync(query);
    }

    @Test
    void shouldDelegateRunWithQueryType() {
        // GIVEN
        var query = mock(Query.class);
        CompletionStage<ResultCursor> expected = CompletableFuture.completedFuture(null);
        given(transaction.runAsync(query)).willReturn(expected);

        // WHEN
        var actual = context.runAsync(query);

        // THEN
        assertEquals(expected, actual);
        then(transaction).should().runAsync(query);
    }
}
