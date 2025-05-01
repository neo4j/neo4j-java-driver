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
package org.neo4j.driver.internal.cursor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.MockitoAnnotations.openMocks;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.stubbing.Answer;
import org.neo4j.bolt.connection.BoltProtocolVersion;
import org.neo4j.bolt.connection.BoltServerAddress;
import org.neo4j.bolt.connection.message.Message;
import org.neo4j.bolt.connection.message.Messages;
import org.neo4j.bolt.connection.summary.RunSummary;
import org.neo4j.driver.Query;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.NoSuchRecordException;
import org.neo4j.driver.internal.DatabaseBookmark;
import org.neo4j.driver.internal.adaptedbolt.DriverBoltConnection;
import org.neo4j.driver.internal.adaptedbolt.DriverResponseHandler;
import org.neo4j.driver.internal.adaptedbolt.summary.PullSummary;

class ResultCursorImplTest {
    ResultCursorImpl cursor;

    @Mock
    DriverBoltConnection connection;

    @Mock
    Consumer<DatabaseBookmark> bookmarkConsumer;

    @Mock
    RunSummary runSummary;

    final Query query = new Query("query");
    final long fetchSize = 1000;
    boolean closeOnSummary;

    @BeforeEach
    @SuppressWarnings("resource")
    void beforeEach() {
        openMocks(this);
        given(connection.protocolVersion()).willReturn(new BoltProtocolVersion(5, 5));
        cursor = new ResultCursorImpl(
                connection, query, fetchSize, bookmarkConsumer, closeOnSummary, null, ignored -> {}, null);
        cursor.onRunSummary(runSummary);
    }

    @Test
    void shouldNextAsync() {
        cursor.onPullSummary(new PullSummaryImpl(true, Collections.emptyMap()));
        given(connection.writeAndFlush(any(), any(Message.class)))
                .willAnswer((Answer<CompletionStage<Void>>) invocation -> {
                    var handler = (DriverResponseHandler) invocation.getArgument(0);
                    handler.onRecord(new Value[0]);
                    return CompletableFuture.completedStage(null);
                });

        var record = cursor.nextAsync().toCompletableFuture().join();

        assertNotNull(record);
    }

    @Test
    void shouldFailNextAsyncOnError() {
        var error = new Neo4jException("code", "message");
        cursor.onError(error);
        cursor.onComplete();

        var future = cursor.nextAsync().toCompletableFuture();

        var exception = assertThrows(CompletionException.class, future::join);
        assertEquals(error, exception.getCause());
    }

    @Test
    void shouldFailNextAsyncOnFlushError() {
        cursor.onPullSummary(new PullSummaryImpl(true, Collections.emptyMap()));
        var error = new RuntimeException("message");
        given(connection.writeAndFlush(any(), any(Message.class)))
                .willAnswer((Answer<CompletionStage<Void>>) invocation -> CompletableFuture.failedStage(error));

        var future = cursor.nextAsync().toCompletableFuture();

        var exception = assertThrows(CompletionException.class, future::join);
        assertEquals(error, exception.getCause());
    }

    @Test
    void shouldSingleAsync() {
        given(connection.serverAddress()).willReturn(BoltServerAddress.LOCAL_DEFAULT);
        cursor.onRecord(new Value[0]);
        cursor.onPullSummary(mock(PullSummary.class));

        var record = cursor.singleAsync().toCompletableFuture().join();

        assertNotNull(record);
    }

    @Test
    void shouldFailSingleAsync() {
        cursor.onPullSummary(new PullSummaryImpl(true, Collections.emptyMap()));
        given(connection.serverAddress()).willReturn(BoltServerAddress.LOCAL_DEFAULT);
        given(connection.writeAndFlush(any(), any(Message.class)))
                .willAnswer((Answer<CompletionStage<Void>>) invocation -> {
                    var handler = (DriverResponseHandler) invocation.getArgument(0);
                    handler.onRecord(new Value[0]);
                    var pullSummary = mock(PullSummary.class);
                    given(pullSummary.hasMore()).willReturn(true);
                    handler.onPullSummary(pullSummary);
                    return CompletableFuture.completedStage(null);
                });

        var future = cursor.singleAsync().toCompletableFuture();

        var exception = assertThrows(CompletionException.class, future::join);
        assertInstanceOf(NoSuchRecordException.class, exception.getCause());
    }

    @Test
    void shouldFailSingleAsyncOnError() {
        cursor.onPullSummary(new PullSummaryImpl(true, Collections.emptyMap()));
        given(connection.serverAddress()).willReturn(BoltServerAddress.LOCAL_DEFAULT);
        var error = new Neo4jException("code", "message");
        given(connection.writeAndFlush(any(), any(Message.class)))
                .willAnswer((Answer<CompletionStage<Void>>) invocation -> {
                    var handler = (DriverResponseHandler) invocation.getArgument(0);
                    handler.onError(error);
                    handler.onComplete();
                    return CompletableFuture.completedStage(null);
                });

        var future = cursor.singleAsync().toCompletableFuture();

        var exception = assertThrows(CompletionException.class, future::join);
        assertEquals(error, exception.getCause());
    }

    @Test
    void shouldFailSingleAsyncOnFlushError() {
        cursor.onPullSummary(new PullSummaryImpl(true, Collections.emptyMap()));
        given(connection.serverAddress()).willReturn(BoltServerAddress.LOCAL_DEFAULT);
        var error = new RuntimeException("message");
        given(connection.writeAndFlush(any(), any(Message.class)))
                .willAnswer((Answer<CompletionStage<Void>>) invocation -> CompletableFuture.failedStage(error));

        var future = cursor.singleAsync().toCompletableFuture();

        var exception = assertThrows(CompletionException.class, future::join);
        assertEquals(error, exception.getCause());
    }

    @Test
    void shouldFetchMore() {
        cursor.onPullSummary(new PullSummaryImpl(true, Collections.emptyMap()));
        given(connection.serverAddress()).willReturn(BoltServerAddress.LOCAL_DEFAULT);
        given(connection.writeAndFlush(any(), any(Message.class)))
                .willAnswer((Answer<CompletionStage<Void>>) invocation -> {
                    var handler = (DriverResponseHandler) invocation.getArgument(0);
                    for (var i = 0; i < fetchSize; i++) {
                        handler.onRecord(new Value[0]);
                    }
                    var pullSummary = mock(PullSummary.class);
                    given(pullSummary.hasMore()).willReturn(true);
                    handler.onPullSummary(pullSummary);
                    return CompletableFuture.completedStage(null);
                });
        for (var i = 0; i < fetchSize; i++) {
            cursor.nextAsync().toCompletableFuture().join();
        }

        assertNotNull(cursor.nextAsync().toCompletableFuture().join());

        then(connection).should(times(2)).writeAndFlush(any(), eq(Messages.pull(0, fetchSize)));
    }

    @Test
    void shouldListAsync() {
        cursor.onPullSummary(new PullSummaryImpl(true, Collections.emptyMap()));
        given(connection.serverAddress()).willReturn(BoltServerAddress.LOCAL_DEFAULT);
        given(connection.writeAndFlush(any(), any(Message.class)))
                .willAnswer((Answer<CompletionStage<Void>>) invocation -> {
                    var handler = (DriverResponseHandler) invocation.getArgument(0);
                    handler.onRecord(new Value[0]);
                    var pullSummary = mock(PullSummary.class);
                    handler.onPullSummary(pullSummary);
                    return CompletableFuture.completedStage(null);
                });

        assertEquals(1, cursor.listAsync().toCompletableFuture().join().size());
        then(connection).should().writeAndFlush(any(), eq(Messages.pull(0, -1)));
    }

    @Test
    void shouldFailListAsyncOnError() {
        cursor.onPullSummary(new PullSummaryImpl(true, Collections.emptyMap()));
        given(connection.serverAddress()).willReturn(BoltServerAddress.LOCAL_DEFAULT);
        var error = new Neo4jException("code", "message");
        given(connection.writeAndFlush(any(), any(Message.class)))
                .willAnswer((Answer<CompletionStage<Void>>) invocation -> {
                    var handler = (DriverResponseHandler) invocation.getArgument(0);
                    handler.onError(error);
                    handler.onComplete();
                    return CompletableFuture.completedStage(null);
                });

        var future = cursor.listAsync().toCompletableFuture();

        var exception = assertThrows(CompletionException.class, future::join);
        assertEquals(error, exception.getCause());
    }

    @Test
    void shouldFailListAsyncOnFlushError() {
        cursor.onPullSummary(new PullSummaryImpl(true, Collections.emptyMap()));
        given(connection.serverAddress()).willReturn(BoltServerAddress.LOCAL_DEFAULT);
        var error = new RuntimeException("message");
        given(connection.writeAndFlush(any(), any(Message.class)))
                .willAnswer((Answer<CompletionStage<Void>>) invocation -> CompletableFuture.failedStage(error));

        var future = cursor.listAsync().toCompletableFuture();

        var exception = assertThrows(CompletionException.class, future::join);
        assertEquals(error, exception.getCause());
    }

    @Test
    void shouldFailPeekAsyncOnError() {
        cursor.onPullSummary(new PullSummaryImpl(true, Collections.emptyMap()));
        given(connection.serverAddress()).willReturn(BoltServerAddress.LOCAL_DEFAULT);
        var error = new Neo4jException("code", "message");
        given(connection.writeAndFlush(any(), any(Message.class)))
                .willAnswer((Answer<CompletionStage<Void>>) invocation -> {
                    var handler = (DriverResponseHandler) invocation.getArgument(0);
                    handler.onError(error);
                    handler.onComplete();
                    return CompletableFuture.completedStage(null);
                });

        var future = cursor.peekAsync().toCompletableFuture();

        var exception = assertThrows(CompletionException.class, future::join);
        assertEquals(error, exception.getCause());
    }

    @Test
    void shouldFailListPeekOnFlushError() {
        cursor.onPullSummary(new PullSummaryImpl(true, Collections.emptyMap()));
        given(connection.serverAddress()).willReturn(BoltServerAddress.LOCAL_DEFAULT);
        var error = new RuntimeException("message");
        given(connection.writeAndFlush(any(), any(Message.class)))
                .willAnswer((Answer<CompletionStage<Void>>) invocation -> CompletableFuture.failedStage(error));

        var future = cursor.peekAsync().toCompletableFuture();

        var exception = assertThrows(CompletionException.class, future::join);
        assertEquals(error, exception.getCause());
    }

    @Test
    void shouldFailConsumeAsyncOnError() {
        cursor.onPullSummary(new PullSummaryImpl(true, Collections.emptyMap()));
        given(connection.serverAddress()).willReturn(BoltServerAddress.LOCAL_DEFAULT);
        var error = new Neo4jException("code", "message");
        given(connection.writeAndFlush(any(), any(Message.class)))
                .willAnswer((Answer<CompletionStage<Void>>) invocation -> {
                    var handler = (DriverResponseHandler) invocation.getArgument(0);
                    handler.onError(error);
                    handler.onComplete();
                    return CompletableFuture.completedStage(null);
                });

        var future = cursor.consumeAsync().toCompletableFuture();

        var exception = assertThrows(CompletionException.class, future::join);
        assertEquals(error, exception.getCause());
    }

    @Test
    void shouldFailConsumeAsyncOnFlushError() {
        cursor.onPullSummary(new PullSummaryImpl(true, Collections.emptyMap()));
        given(connection.serverAddress()).willReturn(BoltServerAddress.LOCAL_DEFAULT);
        var error = new RuntimeException("message");
        given(connection.writeAndFlush(any(), any(Message.class)))
                .willAnswer((Answer<CompletionStage<Void>>) invocation -> CompletableFuture.failedStage(error));

        var future = cursor.consumeAsync().toCompletableFuture();

        var exception = assertThrows(CompletionException.class, future::join);
        assertEquals(error, exception.getCause());
    }

    public record PullSummaryImpl(boolean hasMore, Map<String, Value> metadata) implements PullSummary {}
}
