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
package org.neo4j.driver.internal.reactive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static reactor.adapter.JdkFlowAdapter.publisherToFlowPublisher;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Flow.Publisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.reactive.ReactiveResult;
import org.neo4j.driver.reactive.ReactiveTransaction;
import reactor.core.publisher.Mono;

public class DelegatingReactiveTransactionContextTest {
    ReactiveTransaction transaction;
    DelegatingReactiveTransactionContext context;

    @BeforeEach
    void beforeEach() {
        transaction = mock(ReactiveTransaction.class);
        context = new DelegatingReactiveTransactionContext(transaction);
    }

    @Test
    void shouldDelegateRunWithValueParams() {
        // GIVEN
        var query = "something";
        var params = mock(Value.class);
        Publisher<ReactiveResult> expected = publisherToFlowPublisher(Mono.empty());
        given(transaction.run(query, params)).willReturn(expected);

        // WHEN
        var actual = context.run(query, params);

        // THEN
        assertEquals(expected, actual);
        then(transaction).should().run(query, params);
    }

    @Test
    void shouldDelegateRunWithMapParams() {
        // GIVEN
        var query = "something";
        Map<String, Object> params = Collections.emptyMap();
        Publisher<ReactiveResult> expected = publisherToFlowPublisher(Mono.empty());
        given(transaction.run(query, params)).willReturn(expected);

        // WHEN
        var actual = context.run(query, params);

        // THEN
        assertEquals(expected, actual);
        then(transaction).should().run(query, params);
    }

    @Test
    void shouldDelegateRunWithRecordParams() {
        // GIVEN
        var query = "something";
        var params = mock(Record.class);
        Publisher<ReactiveResult> expected = publisherToFlowPublisher(Mono.empty());
        given(transaction.run(query, params)).willReturn(expected);

        // WHEN
        var actual = context.run(query, params);

        // THEN
        assertEquals(expected, actual);
        then(transaction).should().run(query, params);
    }

    @Test
    void shouldDelegateRun() {
        // GIVEN
        var query = "something";
        Publisher<ReactiveResult> expected = publisherToFlowPublisher(Mono.empty());
        given(transaction.run(query)).willReturn(expected);

        // WHEN
        var actual = context.run(query);

        // THEN
        assertEquals(expected, actual);
        then(transaction).should().run(query);
    }

    @Test
    void shouldDelegateRunWithQueryType() {
        // GIVEN
        var query = mock(Query.class);
        Publisher<ReactiveResult> expected = publisherToFlowPublisher(Mono.empty());
        given(transaction.run(query)).willReturn(expected);

        // WHEN
        var actual = context.run(query);

        // THEN
        assertEquals(expected, actual);
        then(transaction).should().run(query);
    }
}
