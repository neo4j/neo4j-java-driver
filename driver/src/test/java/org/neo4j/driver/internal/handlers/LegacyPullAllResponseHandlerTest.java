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
package org.neo4j.driver.internal.handlers;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.Values.values;
import static org.neo4j.driver.testutil.TestUtil.await;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Query;
import org.neo4j.driver.internal.messaging.v3.BoltProtocolV3;
import org.neo4j.driver.internal.spi.Connection;

class LegacyPullAllResponseHandlerTest extends PullAllResponseHandlerTestBase<LegacyPullAllResponseHandler> {
    @Test
    void shouldDisableAutoReadWhenTooManyRecordsArrive() {
        var connection = connectionMock();
        var handler = newHandler(asList("key1", "key2"), connection);

        for (var i = 0; i < LegacyPullAllResponseHandler.RECORD_BUFFER_HIGH_WATERMARK + 1; i++) {
            handler.onRecord(values(100, 200));
        }

        verify(connection).disableAutoRead();
    }

    @Test
    void shouldEnableAutoReadWhenRecordsRetrievedFromBuffer() {
        var connection = connectionMock();
        var keys = asList("key1", "key2");
        var handler = newHandler(keys, connection);

        int i;
        for (i = 0; i < LegacyPullAllResponseHandler.RECORD_BUFFER_HIGH_WATERMARK + 1; i++) {
            handler.onRecord(values(100, 200));
        }

        verify(connection, never()).enableAutoRead();
        verify(connection).disableAutoRead();

        while (i-- > LegacyPullAllResponseHandler.RECORD_BUFFER_LOW_WATERMARK - 1) {
            var record = await(handler.nextAsync());
            assertNotNull(record);
            assertEquals(keys, record.keys());
            assertEquals(100, record.get("key1").asInt());
            assertEquals(200, record.get("key2").asInt());
        }
        verify(connection).enableAutoRead();
    }

    @Test
    void shouldNotDisableAutoReadWhenSummaryRequested() {
        var connection = connectionMock();
        var keys = asList("key1", "key2");
        var handler = newHandler(keys, connection);

        var summaryFuture = handler.consumeAsync().toCompletableFuture();
        assertFalse(summaryFuture.isDone());

        var recordCount = LegacyPullAllResponseHandler.RECORD_BUFFER_HIGH_WATERMARK + 10;
        for (var i = 0; i < recordCount; i++) {
            handler.onRecord(values("a", "b"));
        }

        verify(connection, never()).disableAutoRead();

        handler.onSuccess(emptyMap());
        assertTrue(summaryFuture.isDone());

        var summary = await(summaryFuture);
        assertNotNull(summary);
        assertNull(await(handler.nextAsync()));
    }

    @Test
    void shouldNotDisableAutoReadWhenFailureRequested() {
        var connection = connectionMock();
        var keys = asList("key1", "key2");
        var handler = newHandler(keys, connection);

        var failureFuture = handler.pullAllFailureAsync().toCompletableFuture();
        assertFalse(failureFuture.isDone());

        var recordCount = LegacyPullAllResponseHandler.RECORD_BUFFER_HIGH_WATERMARK + 5;
        for (var i = 0; i < recordCount; i++) {
            handler.onRecord(values(123, 456));
        }

        verify(connection, never()).disableAutoRead();

        var error = new IllegalStateException("Wrong config");
        handler.onFailure(error);

        assertTrue(failureFuture.isDone());
        assertEquals(error, await(failureFuture));

        for (var i = 0; i < recordCount; i++) {
            var record = await(handler.nextAsync());
            assertNotNull(record);
            assertEquals(keys, record.keys());
            assertEquals(123, record.get("key1").asInt());
            assertEquals(456, record.get("key2").asInt());
        }

        assertNull(await(handler.nextAsync()));
    }

    @Test
    void shouldEnableAutoReadOnConnectionWhenFailureRequestedButNotAvailable() throws Exception {
        var connection = connectionMock();
        var handler = newHandler(asList("key1", "key2"), connection);

        handler.onRecord(values(1, 2));
        handler.onRecord(values(3, 4));

        verify(connection, never()).enableAutoRead();
        verify(connection, never()).disableAutoRead();

        var failureFuture = handler.pullAllFailureAsync().toCompletableFuture();
        assertFalse(failureFuture.isDone());

        verify(connection).enableAutoRead();
        verify(connection, never()).disableAutoRead();

        assertNotNull(await(handler.nextAsync()));
        assertNotNull(await(handler.nextAsync()));

        var error = new RuntimeException("Oh my!");
        handler.onFailure(error);

        assertTrue(failureFuture.isDone());
        assertEquals(error, failureFuture.get());
    }

    @Test
    void shouldNotDisableAutoReadWhenAutoReadManagementDisabled() {
        var connection = connectionMock();
        var handler = newHandler(asList("key1", "key2"), connection);
        handler.disableAutoReadManagement();

        for (var i = 0; i < LegacyPullAllResponseHandler.RECORD_BUFFER_HIGH_WATERMARK + 1; i++) {
            handler.onRecord(values(100, 200));
        }

        verify(connection, never()).disableAutoRead();
    }

    @Test
    void shouldReturnEmptyListInListAsyncAfterFailure() {
        var handler = newHandler();

        var error = new RuntimeException("Hi");
        handler.onFailure(error);

        // consume the error
        assertEquals(error, await(handler.pullAllFailureAsync()));
        assertEquals(emptyList(), await(handler.listAsync(Function.identity())));
    }

    @Test
    void shouldEnableAutoReadOnConnectionWhenSummaryRequestedButNotAvailable() throws Exception // TODO for auto run
            {
        var connection = connectionMock();
        PullAllResponseHandler handler = newHandler(asList("key1", "key2", "key3"), connection);

        handler.onRecord(values(1, 2, 3));
        handler.onRecord(values(4, 5, 6));

        verify(connection, never()).enableAutoRead();
        verify(connection, never()).disableAutoRead();

        var summaryFuture = handler.consumeAsync().toCompletableFuture();
        assertFalse(summaryFuture.isDone());

        verify(connection).enableAutoRead();
        verify(connection, never()).disableAutoRead();

        assertNull(await(handler.nextAsync()));

        handler.onSuccess(emptyMap());

        assertTrue(summaryFuture.isDone());
        assertNotNull(summaryFuture.get());
    }

    protected LegacyPullAllResponseHandler newHandler(Query query, List<String> queryKeys, Connection connection) {
        var runResponseHandler = new RunResponseHandler(
                new CompletableFuture<>(), BoltProtocolV3.METADATA_EXTRACTOR, mock(Connection.class), null);
        runResponseHandler.onSuccess(singletonMap("fields", value(queryKeys)));
        return new LegacyPullAllResponseHandler(
                query,
                runResponseHandler,
                connection,
                BoltProtocolV3.METADATA_EXTRACTOR,
                mock(PullResponseCompletionListener.class));
    }
}
