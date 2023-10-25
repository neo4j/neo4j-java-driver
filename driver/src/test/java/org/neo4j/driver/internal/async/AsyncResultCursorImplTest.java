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

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.Values.values;
import static org.neo4j.driver.internal.summary.InternalDatabaseInfo.DEFAULT_DATABASE_INFO;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.internal.util.Futures.failedFuture;
import static org.neo4j.driver.testutil.TestUtil.await;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.exceptions.NoSuchRecordException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.cursor.AsyncResultCursorImpl;
import org.neo4j.driver.internal.handlers.PullAllResponseHandler;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.messaging.v3.BoltProtocolV3;
import org.neo4j.driver.internal.messaging.v43.BoltProtocolV43;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.summary.InternalResultSummary;
import org.neo4j.driver.internal.summary.InternalServerInfo;
import org.neo4j.driver.internal.summary.InternalSummaryCounters;
import org.neo4j.driver.summary.QueryType;
import org.neo4j.driver.summary.ResultSummary;

class AsyncResultCursorImplTest {
    @Test
    void shouldReturnQueryKeys() {
        var runHandler = newRunResponseHandler();
        var pullAllHandler = mock(PullAllResponseHandler.class);

        var keys = asList("key1", "key2", "key3");
        runHandler.onSuccess(singletonMap("fields", value(keys)));

        var cursor = newCursor(runHandler, pullAllHandler);

        assertEquals(keys, cursor.keys());
    }

    @Test
    void shouldReturnSummary() {
        var pullAllHandler = mock(PullAllResponseHandler.class);

        ResultSummary summary = new InternalResultSummary(
                new Query("RETURN 42"),
                new InternalServerInfo("Neo4j/4.2.5", BoltServerAddress.LOCAL_DEFAULT, BoltProtocolV43.VERSION),
                DEFAULT_DATABASE_INFO,
                QueryType.SCHEMA_WRITE,
                new InternalSummaryCounters(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 0),
                null,
                null,
                emptyList(),
                42,
                42);
        when(pullAllHandler.consumeAsync()).thenReturn(completedFuture(summary));

        var cursor = newCursor(pullAllHandler);

        assertEquals(summary, await(cursor.consumeAsync()));
    }

    @Test
    void shouldReturnNextExistingRecord() {
        var pullAllHandler = mock(PullAllResponseHandler.class);

        Record record = new InternalRecord(asList("key1", "key2"), values(1, 2));
        when(pullAllHandler.nextAsync()).thenReturn(completedFuture(record));

        var cursor = newCursor(pullAllHandler);

        assertEquals(record, await(cursor.nextAsync()));
    }

    @Test
    void shouldReturnNextNonExistingRecord() {
        var pullAllHandler = mock(PullAllResponseHandler.class);
        when(pullAllHandler.nextAsync()).thenReturn(completedWithNull());

        var cursor = newCursor(pullAllHandler);

        assertNull(await(cursor.nextAsync()));
    }

    @Test
    void shouldPeekExistingRecord() {
        var pullAllHandler = mock(PullAllResponseHandler.class);

        Record record = new InternalRecord(asList("key1", "key2", "key3"), values(3, 2, 1));
        when(pullAllHandler.peekAsync()).thenReturn(completedFuture(record));

        var cursor = newCursor(pullAllHandler);

        assertEquals(record, await(cursor.peekAsync()));
    }

    @Test
    void shouldPeekNonExistingRecord() {
        var pullAllHandler = mock(PullAllResponseHandler.class);
        when(pullAllHandler.peekAsync()).thenReturn(completedWithNull());

        var cursor = newCursor(pullAllHandler);

        assertNull(await(cursor.peekAsync()));
    }

    @Test
    void shouldReturnSingleRecord() {
        var pullAllHandler = mock(PullAllResponseHandler.class);

        Record record = new InternalRecord(asList("key1", "key2"), values(42, 42));
        when(pullAllHandler.nextAsync()).thenReturn(completedFuture(record)).thenReturn(completedWithNull());

        var cursor = newCursor(pullAllHandler);

        assertEquals(record, await(cursor.singleAsync()));
    }

    @Test
    void shouldFailWhenAskedForSingleRecordButResultIsEmpty() {
        var pullAllHandler = mock(PullAllResponseHandler.class);
        when(pullAllHandler.nextAsync()).thenReturn(completedWithNull());

        var cursor = newCursor(pullAllHandler);

        var e = assertThrows(NoSuchRecordException.class, () -> await(cursor.singleAsync()));
        assertThat(e.getMessage(), containsString("result is empty"));
    }

    @Test
    void shouldFailWhenAskedForSingleRecordButResultContainsMore() {
        var pullAllHandler = mock(PullAllResponseHandler.class);

        Record record1 = new InternalRecord(asList("key1", "key2"), values(1, 1));
        Record record2 = new InternalRecord(asList("key1", "key2"), values(2, 2));
        when(pullAllHandler.nextAsync()).thenReturn(completedFuture(record1)).thenReturn(completedFuture(record2));

        var cursor = newCursor(pullAllHandler);

        var e = assertThrows(NoSuchRecordException.class, () -> await(cursor.singleAsync()));
        assertThat(e.getMessage(), containsString("Ensure your query returns only one record"));
    }

    @Test
    void shouldForEachAsyncWhenResultContainsMultipleRecords() {
        var pullAllHandler = mock(PullAllResponseHandler.class);

        Record record1 = new InternalRecord(asList("key1", "key2", "key3"), values(1, 1, 1));
        Record record2 = new InternalRecord(asList("key1", "key2", "key3"), values(2, 2, 2));
        Record record3 = new InternalRecord(asList("key1", "key2", "key3"), values(3, 3, 3));
        when(pullAllHandler.nextAsync())
                .thenReturn(completedFuture(record1))
                .thenReturn(completedFuture(record2))
                .thenReturn(completedFuture(record3))
                .thenReturn(completedWithNull());

        var summary = mock(ResultSummary.class);
        when(pullAllHandler.consumeAsync()).thenReturn(completedFuture(summary));

        var cursor = newCursor(pullAllHandler);

        List<Record> records = new CopyOnWriteArrayList<>();
        var summaryStage = cursor.forEachAsync(records::add);

        assertEquals(summary, await(summaryStage));
        assertEquals(asList(record1, record2, record3), records);
    }

    @Test
    void shouldForEachAsyncWhenResultContainsOneRecords() {
        var pullAllHandler = mock(PullAllResponseHandler.class);

        Record record = new InternalRecord(asList("key1", "key2", "key3"), values(1, 1, 1));
        when(pullAllHandler.nextAsync()).thenReturn(completedFuture(record)).thenReturn(completedWithNull());

        var summary = mock(ResultSummary.class);
        when(pullAllHandler.consumeAsync()).thenReturn(completedFuture(summary));

        var cursor = newCursor(pullAllHandler);

        List<Record> records = new CopyOnWriteArrayList<>();
        var summaryStage = cursor.forEachAsync(records::add);

        assertEquals(summary, await(summaryStage));
        assertEquals(singletonList(record), records);
    }

    @Test
    void shouldForEachAsyncWhenResultContainsNoRecords() {
        var pullAllHandler = mock(PullAllResponseHandler.class);
        when(pullAllHandler.nextAsync()).thenReturn(completedWithNull());

        var summary = mock(ResultSummary.class);
        when(pullAllHandler.consumeAsync()).thenReturn(completedFuture(summary));

        var cursor = newCursor(pullAllHandler);

        List<Record> records = new CopyOnWriteArrayList<>();
        var summaryStage = cursor.forEachAsync(records::add);

        assertEquals(summary, await(summaryStage));
        assertEquals(0, records.size());
    }

    @Test
    void shouldFailForEachWhenGivenActionThrows() {
        var pullAllHandler = mock(PullAllResponseHandler.class);

        Record record1 = new InternalRecord(asList("key1", "key2"), values(1, 1));
        Record record2 = new InternalRecord(asList("key1", "key2"), values(2, 2));
        Record record3 = new InternalRecord(asList("key1", "key2"), values(3, 3));
        when(pullAllHandler.nextAsync())
                .thenReturn(completedFuture(record1))
                .thenReturn(completedFuture(record2))
                .thenReturn(completedFuture(record3))
                .thenReturn(completedWithNull());

        var cursor = newCursor(pullAllHandler);

        var recordsProcessed = new AtomicInteger();
        var error = new RuntimeException("Hello");

        var stage = cursor.forEachAsync(record -> {
            if (record.get("key2").asInt() == 2) {
                throw error;
            } else {
                recordsProcessed.incrementAndGet();
            }
        });

        var e = assertThrows(RuntimeException.class, () -> await(stage));
        assertEquals(error, e);

        assertEquals(1, recordsProcessed.get());
        verify(pullAllHandler, times(2)).nextAsync();
    }

    @Test
    void shouldReturnFailureWhenExists() {
        var pullAllHandler = mock(PullAllResponseHandler.class);

        var error = new ServiceUnavailableException("Hi");
        when(pullAllHandler.pullAllFailureAsync()).thenReturn(completedFuture(error));

        var cursor = newCursor(pullAllHandler);

        assertEquals(error, await(cursor.pullAllFailureAsync()));
    }

    @Test
    void shouldReturnNullFailureWhenDoesNotExist() {
        var pullAllHandler = mock(PullAllResponseHandler.class);
        when(pullAllHandler.pullAllFailureAsync()).thenReturn(completedWithNull());

        var cursor = newCursor(pullAllHandler);

        assertNull(await(cursor.pullAllFailureAsync()));
    }

    @Test
    void shouldListAsyncWithoutMapFunction() {
        var pullAllHandler = mock(PullAllResponseHandler.class);

        Record record1 = new InternalRecord(asList("key1", "key2"), values(1, 1));
        Record record2 = new InternalRecord(asList("key1", "key2"), values(2, 2));
        var records = asList(record1, record2);

        when(pullAllHandler.listAsync(Function.identity())).thenReturn(completedFuture(records));

        var cursor = newCursor(pullAllHandler);

        assertEquals(records, await(cursor.listAsync()));
        verify(pullAllHandler).listAsync(Function.identity());
    }

    @Test
    void shouldListAsyncWithMapFunction() {
        Function<Record, String> mapFunction = record -> record.get(0).asString();
        var pullAllHandler = mock(PullAllResponseHandler.class);

        var values = asList("a", "b", "c", "d", "e");
        when(pullAllHandler.listAsync(mapFunction)).thenReturn(completedFuture(values));

        var cursor = newCursor(pullAllHandler);

        assertEquals(values, await(cursor.listAsync(mapFunction)));
        verify(pullAllHandler).listAsync(mapFunction);
    }

    @Test
    void shouldPropagateFailureFromListAsyncWithoutMapFunction() {
        var pullAllHandler = mock(PullAllResponseHandler.class);
        var error = new RuntimeException("Hi");
        when(pullAllHandler.listAsync(Function.identity())).thenReturn(failedFuture(error));

        var cursor = newCursor(pullAllHandler);

        var e = assertThrows(RuntimeException.class, () -> await(cursor.listAsync()));
        assertEquals(error, e);
        verify(pullAllHandler).listAsync(Function.identity());
    }

    @Test
    void shouldPropagateFailureFromListAsyncWithMapFunction() {
        Function<Record, String> mapFunction = record -> record.get(0).asString();
        var pullAllHandler = mock(PullAllResponseHandler.class);
        var error = new RuntimeException("Hi");
        when(pullAllHandler.listAsync(mapFunction)).thenReturn(failedFuture(error));

        var cursor = newCursor(pullAllHandler);

        var e = assertThrows(RuntimeException.class, () -> await(cursor.listAsync(mapFunction)));
        assertEquals(error, e);

        verify(pullAllHandler).listAsync(mapFunction);
    }

    @Test
    void shouldConsumeAsync() {
        var pullAllHandler = mock(PullAllResponseHandler.class);
        var summary = mock(ResultSummary.class);
        when(pullAllHandler.consumeAsync()).thenReturn(completedFuture(summary));

        var cursor = newCursor(pullAllHandler);

        assertEquals(summary, await(cursor.consumeAsync()));
    }

    @Test
    void shouldPropagateFailureInConsumeAsync() {
        var pullAllHandler = mock(PullAllResponseHandler.class);
        var error = new RuntimeException("Hi");
        when(pullAllHandler.consumeAsync()).thenReturn(failedFuture(error));

        var cursor = newCursor(pullAllHandler);

        var e = assertThrows(RuntimeException.class, () -> await(cursor.consumeAsync()));
        assertEquals(error, e);
    }

    @Test
    void shouldThrowOnIsOpenAsync() {
        // GIVEN
        var cursor = new AsyncResultCursorImpl(null, null, null);

        // WHEN & THEN
        assertThrows(UnsupportedOperationException.class, cursor::isOpenAsync);
    }

    private static AsyncResultCursorImpl newCursor(PullAllResponseHandler pullAllHandler) {
        return new AsyncResultCursorImpl(null, newRunResponseHandler(), pullAllHandler);
    }

    private static AsyncResultCursorImpl newCursor(
            RunResponseHandler runHandler, PullAllResponseHandler pullAllHandler) {
        return new AsyncResultCursorImpl(null, runHandler, pullAllHandler);
    }

    private static RunResponseHandler newRunResponseHandler() {
        return new RunResponseHandler(
                new CompletableFuture<>(), BoltProtocolV3.METADATA_EXTRACTOR, mock(Connection.class), null);
    }
}
