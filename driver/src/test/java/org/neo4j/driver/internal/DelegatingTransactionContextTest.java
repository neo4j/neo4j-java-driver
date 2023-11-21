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
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Value;

public class DelegatingTransactionContextTest {
    Transaction transaction;
    DelegatingTransactionContext context;

    @BeforeEach
    void beforeEach() {
        transaction = mock(Transaction.class);
        context = new DelegatingTransactionContext(transaction);
    }

    @Test
    @SuppressWarnings("resource")
    void shouldDelegateRunWithValueParams() {
        // GIVEN
        var query = "something";
        var params = mock(Value.class);
        var expected = mock(Result.class);
        given(transaction.run(query, params)).willReturn(expected);

        // WHEN
        var actual = context.run(query, params);

        // THEN
        assertEquals(expected, actual);
        then(transaction).should().run(query, params);
    }

    @Test
    @SuppressWarnings("resource")
    void shouldDelegateRunWithMapParams() {
        // GIVEN
        var query = "something";
        Map<String, Object> params = Collections.emptyMap();
        var expected = mock(Result.class);
        given(transaction.run(query, params)).willReturn(expected);

        // WHEN
        var actual = context.run(query, params);

        // THEN
        assertEquals(expected, actual);
        then(transaction).should().run(query, params);
    }

    @Test
    @SuppressWarnings("resource")
    void shouldDelegateRunWithRecordParams() {
        // GIVEN
        var query = "something";
        var params = mock(Record.class);
        var expected = mock(Result.class);
        given(transaction.run(query, params)).willReturn(expected);

        // WHEN
        var actual = context.run(query, params);

        // THEN
        assertEquals(expected, actual);
        then(transaction).should().run(query, params);
    }

    @Test
    @SuppressWarnings("resource")
    void shouldDelegateRun() {
        // GIVEN
        var query = "something";
        var expected = mock(Result.class);
        given(transaction.run(query)).willReturn(expected);

        // WHEN
        var actual = context.run(query);

        // THEN
        assertEquals(expected, actual);
        then(transaction).should().run(query);
    }

    @Test
    @SuppressWarnings("resource")
    void shouldDelegateRunWithQueryType() {
        // GIVEN
        var query = mock(Query.class);
        var expected = mock(Result.class);
        given(transaction.run(query)).willReturn(expected);

        // WHEN
        var actual = context.run(query);

        // THEN
        assertEquals(expected, actual);
        then(transaction).should().run(query);
    }
}
