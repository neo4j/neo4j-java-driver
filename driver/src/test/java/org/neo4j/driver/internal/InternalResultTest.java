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

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.Records.column;
import static org.neo4j.driver.Values.ofString;
import static org.neo4j.driver.Values.value;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.bolt.connection.BoltProtocolVersion;
import org.neo4j.bolt.connection.BoltServerAddress;
import org.neo4j.bolt.connection.summary.RunSummary;
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Value;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.exceptions.NoSuchRecordException;
import org.neo4j.driver.exceptions.ResultConsumedException;
import org.neo4j.driver.internal.adaptedbolt.DriverBoltConnection;
import org.neo4j.driver.internal.adaptedbolt.summary.PullSummary;
import org.neo4j.driver.internal.cursor.DisposableResultCursorImpl;
import org.neo4j.driver.internal.cursor.ResultCursorImpl;
import org.neo4j.driver.internal.value.NullValue;
import org.neo4j.driver.util.Pair;

class InternalResultTest {
    @Test
    void iterationShouldWorksAsExpected() {
        // GIVEN
        var result = createResult(3);

        // WHEN
        assertTrue(result.hasNext());
        assertThat(values(result.next()), equalTo(asList(value("v1-1"), value("v2-1"))));

        assertTrue(result.hasNext());
        assertThat(values(result.next()), equalTo(asList(value("v1-2"), value("v2-2"))));

        assertTrue(result.hasNext()); // 1 -> 2

        // THEN
        assertThat(values(result.next()), equalTo(asList(value("v1-3"), value("v2-3"))));
        assertFalse(result.hasNext());

        assertThrows(NoSuchRecordException.class, result::next);
    }

    @Test
    void firstOfFieldNameShouldWorkAsExpected() {
        // GIVEN
        var result = createResult(3);

        // THEN
        assertThat(result.next().get("k1"), equalTo(value("v1-1")));
        assertTrue(result.hasNext());
    }

    @Test
    void firstOfFieldIndexShouldWorkAsExpected() {
        // GIVEN
        var result = createResult(3);

        // THEN
        assertThat(result.next().get(0), equalTo(value("v1-1")));
        assertTrue(result.hasNext());
    }

    @Test
    void singlePastFirstShouldFail() {
        // GIVEN
        var result = createResult(2);
        result.next();
        result.next();

        // THEN
        assertThrows(NoSuchRecordException.class, result::single);
    }

    @Test
    void singleNoneShouldFail() {
        // GIVEN
        var result = createResult(0);

        // THEN
        assertThrows(NoSuchRecordException.class, result::single);
    }

    @Test
    void singleWhenMoreThanOneShouldFail() {
        // GIVEN
        var result = createResult(2);

        // THEN
        assertThrows(NoSuchRecordException.class, result::single);
    }

    @Test
    void singleOfFieldNameShouldWorkAsExpected() {
        // GIVEN
        var result = createResult(1);

        // THEN
        assertThat(result.single().get("k1"), equalTo(value("v1-1")));
        assertFalse(result.hasNext());
    }

    @Test
    void singleOfFieldIndexShouldWorkAsExpected() {
        // GIVEN
        var result = createResult(1);

        // THEN
        assertThat(result.single().get(0), equalTo(value("v1-1")));
        assertFalse(result.hasNext());
    }

    @Test
    void singleShouldWorkAsExpected() {
        assertNotNull(createResult(1).single());
    }

    @Test
    void singleShouldThrowOnBigResult() {
        assertThrows(NoSuchRecordException.class, () -> createResult(42).single());
    }

    @Test
    void singleShouldThrowOnEmptyResult() {
        assertThrows(NoSuchRecordException.class, () -> createResult(0).single());
    }

    @Test
    void singleShouldThrowOnConsumedResult() {
        assertThrows(ResultConsumedException.class, () -> {
            var result = createResult(2);
            result.consume();
            result.single();
        });
    }

    @Test
    void shouldConsumeTwice() {
        // GIVEN
        var result = createResult(2);
        result.consume();

        // WHEN
        result.consume();

        // THEN
        assertThrows(ResultConsumedException.class, result::hasNext);
    }

    @Test
    void shouldList() {
        // GIVEN
        var result = createResult(2);
        var records = result.list(column("k1", ofString()));

        // THEN
        assertThat(records, equalTo(asList("v1-1", "v1-2")));
    }

    @Test
    void shouldListTwice() {
        // GIVEN
        var result = createResult(2);
        var firstList = result.list();
        assertThat(firstList.size(), equalTo(2));

        // THEN
        var secondList = result.list();
        assertThat(secondList.size(), equalTo(0));
    }

    @Test
    void singleShouldNotThrowOnPartiallyConsumedResult() {
        // Given
        var result = createResult(2);
        result.next();

        // When + Then
        assertNotNull(result.single());
    }

    @Test
    void singleShouldConsumeIfFailing() {
        var result = createResult(2);

        assertThrows(NoSuchRecordException.class, result::single);
        assertFalse(result.hasNext());
    }

    @Test
    void retainShouldWorkAsExpected() {
        // GIVEN
        var result = createResult(3);

        // WHEN
        var records = result.list();

        // THEN
        assertFalse(result.hasNext());
        assertThat(records, hasSize(3));
    }

    @Test
    void retainAndMapByKeyShouldWorkAsExpected() {
        // GIVEN
        var result = createResult(3);

        // WHEN
        var records = result.list(column("k1"));

        // THEN
        assertFalse(result.hasNext());
        assertThat(records, hasSize(3));
    }

    @Test
    void retainAndMapByIndexShouldWorkAsExpected() {
        // GIVEN
        var result = createResult(3);

        // WHEN
        var records = result.list(column(0));

        // THEN
        assertFalse(result.hasNext());
        assertThat(records, hasSize(3));
    }

    @Test
    void accessingOutOfBoundsShouldBeNull() {
        // GIVEN
        var result = createResult(1);

        // WHEN
        var record = result.single();

        // THEN
        assertThat(record.get(0), equalTo(value("v1-1")));
        assertThat(record.get(1), equalTo(value("v2-1")));
        assertThat(record.get(2), equalTo(NullValue.NULL));
        assertThat(record.get(-37), equalTo(NullValue.NULL));
    }

    @Test
    void accessingKeysWithoutCallingNextShouldNotFail() {
        // GIVEN
        var result = createResult(11);

        // WHEN
        // not calling next or single

        // THEN
        assertThat(result.keys(), equalTo(asList("k1", "k2")));
    }

    @Test
    void shouldPeekIntoTheFuture() {
        // WHEN
        var result = createResult(2);

        // THEN
        assertThat(result.peek().get("k1"), equalTo(value("v1-1")));

        // WHEN
        result.next();

        // THEN
        assertThat(result.peek().get("k1"), equalTo(value("v1-2")));

        // WHEN
        result.next();

        // THEN
        assertThrows(NoSuchRecordException.class, result::peek);
    }

    @Test
    void shouldNotPeekIntoTheFutureWhenResultIsEmpty() {
        // GIVEN
        var result = createResult(0);

        // THEN
        assertThrows(NoSuchRecordException.class, result::peek);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldDelegateIsOpen(boolean expectedState) {
        // GIVEN
        var cursor = mock(ResultCursor.class);
        given(cursor.isOpenAsync()).willReturn(CompletableFuture.completedFuture(expectedState));
        Result result = new InternalResult(null, cursor);

        // WHEN
        var actualState = result.isOpen();

        // THEN
        assertEquals(expectedState, actualState);
        then(cursor).should().isOpenAsync();
    }

    private Result createResult(int numberOfRecords) {
        var query = new Query("<unknown>");
        var connection = mock(DriverBoltConnection.class);
        when(connection.serverAddress()).thenReturn(BoltServerAddress.LOCAL_DEFAULT);
        when(connection.protocolVersion()).thenReturn(new BoltProtocolVersion(4, 3));
        when(connection.serverAgent()).thenReturn("Neo4j/4.2.5");

        var resultCursor = new ResultCursorImpl(connection, query, -1, ignored -> {}, false, null, ignored -> {}, null);
        var runSummary = mock(RunSummary.class);
        given(runSummary.keys()).willReturn(asList("k1", "k2"));
        resultCursor.onRunSummary(runSummary);
        for (var i = 1; i <= numberOfRecords; i++) {
            resultCursor.onRecord(new Value[] {value("v1-" + i), value("v2-" + i)});
        }
        resultCursor.onPullSummary(new PullSummary() {
            @Override
            public boolean hasMore() {
                return false;
            }

            @Override
            public Map<String, Value> metadata() {
                return Map.of();
            }
        });
        return new InternalResult(connection, new DisposableResultCursorImpl(resultCursor));
    }

    private List<Value> values(Record record) {
        return record.fields().stream()
                .map(Pair::value)
                .collect(Collectors.toCollection(
                        () -> new ArrayList<>(record.keys().size())));
    }
}
