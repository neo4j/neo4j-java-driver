/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
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
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.Values.values;
import static org.neo4j.driver.testutil.TestUtil.await;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.exceptions.AuthorizationExpiredException;
import org.neo4j.driver.exceptions.ConnectionReadTimeoutException;
import org.neo4j.driver.internal.async.UnmanagedTransaction;
import org.neo4j.driver.internal.messaging.v3.BoltProtocolV3;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.MetadataExtractor;

class RunResponseHandlerTest {
    @Test
    void shouldNotifyRunFutureOnSuccess() throws Exception {
        CompletableFuture<Void> runFuture = new CompletableFuture<>();
        RunResponseHandler handler = newHandler(runFuture);

        assertFalse(runFuture.isDone());
        handler.onSuccess(emptyMap());

        assertTrue(runFuture.isDone());
        assertNull(runFuture.get());
    }

    @Test
    void shouldNotifyRunFutureOnFailure() {
        CompletableFuture<Void> runFuture = new CompletableFuture<>();
        RunResponseHandler handler = newHandler(runFuture);

        assertFalse(runFuture.isDone());
        RuntimeException exception = new RuntimeException();
        handler.onFailure(exception);

        assertTrue(runFuture.isCompletedExceptionally());
        ExecutionException executionException = assertThrows(ExecutionException.class, runFuture::get);
        assertThat(executionException.getCause(), equalTo(exception));
    }

    @Test
    void shouldThrowOnRecord() {
        RunResponseHandler handler = newHandler();

        assertThrows(UnsupportedOperationException.class, () -> handler.onRecord(values("a", "b", "c")));
    }

    @Test
    void shouldReturnNoKeysWhenFailed() {
        RunResponseHandler handler = newHandler();

        handler.onFailure(new RuntimeException());

        assertEquals(emptyList(), handler.queryKeys().keys());
        assertEquals(emptyMap(), handler.queryKeys().keyIndex());
    }

    @Test
    void shouldReturnDefaultResultAvailableAfterWhenFailed() {
        RunResponseHandler handler = newHandler();

        handler.onFailure(new RuntimeException());

        assertEquals(-1, handler.resultAvailableAfter());
    }

    @Test
    void shouldReturnKeysWhenSucceeded() {
        RunResponseHandler handler = newHandler();

        List<String> keys = asList("key1", "key2", "key3");
        Map<String, Integer> keyIndex = new HashMap<>();
        keyIndex.put("key1", 0);
        keyIndex.put("key2", 1);
        keyIndex.put("key3", 2);
        handler.onSuccess(singletonMap("fields", value(keys)));

        assertEquals(keys, handler.queryKeys().keys());
        assertEquals(keyIndex, handler.queryKeys().keyIndex());
    }

    @Test
    void shouldReturnResultAvailableAfterWhenSucceededV3() {
        testResultAvailableAfterOnSuccess("t_first", BoltProtocolV3.METADATA_EXTRACTOR);
    }

    @Test
    void shouldMarkTxAndKeepConnectionAndFailOnFailure() {
        CompletableFuture<Void> runFuture = new CompletableFuture<>();
        Connection connection = mock(Connection.class);
        UnmanagedTransaction tx = mock(UnmanagedTransaction.class);
        RunResponseHandler handler =
                new RunResponseHandler(runFuture, BoltProtocolV3.METADATA_EXTRACTOR, connection, tx);
        Throwable throwable = new RuntimeException();

        assertFalse(runFuture.isDone());
        handler.onFailure(throwable);

        assertTrue(runFuture.isCompletedExceptionally());
        Throwable actualException = assertThrows(Throwable.class, () -> await(runFuture));
        assertSame(throwable, actualException);
        verify(tx).markTerminated(throwable);
        verify(connection, never()).release();
        verify(connection, never()).terminateAndRelease(any(String.class));
    }

    @Test
    void shouldNotReleaseConnectionAndFailOnFailure() {
        CompletableFuture<Void> runFuture = new CompletableFuture<>();
        Connection connection = mock(Connection.class);
        RunResponseHandler handler =
                new RunResponseHandler(runFuture, BoltProtocolV3.METADATA_EXTRACTOR, connection, null);
        Throwable throwable = new RuntimeException();

        assertFalse(runFuture.isDone());
        handler.onFailure(throwable);

        assertTrue(runFuture.isCompletedExceptionally());
        Throwable actualException = assertThrows(Throwable.class, () -> await(runFuture));
        assertSame(throwable, actualException);
        verify(connection, never()).release();
        verify(connection, never()).terminateAndRelease(any(String.class));
    }

    @Test
    void shouldReleaseConnectionImmediatelyAndFailOnAuthorizationExpiredExceptionFailure() {
        CompletableFuture<Void> runFuture = new CompletableFuture<>();
        Connection connection = mock(Connection.class);
        RunResponseHandler handler =
                new RunResponseHandler(runFuture, BoltProtocolV3.METADATA_EXTRACTOR, connection, null);
        AuthorizationExpiredException authorizationExpiredException =
                new AuthorizationExpiredException("code", "message");

        assertFalse(runFuture.isDone());
        handler.onFailure(authorizationExpiredException);

        assertTrue(runFuture.isCompletedExceptionally());
        AuthorizationExpiredException actualException =
                assertThrows(AuthorizationExpiredException.class, () -> await(runFuture));
        assertSame(authorizationExpiredException, actualException);
        verify(connection).terminateAndRelease(AuthorizationExpiredException.DESCRIPTION);
        verify(connection, never()).release();
    }

    @Test
    void shouldReleaseConnectionImmediatelyAndFailOnConnectionReadTimeoutExceptionFailure() {
        CompletableFuture<Void> runFuture = new CompletableFuture<>();
        Connection connection = mock(Connection.class);
        RunResponseHandler handler =
                new RunResponseHandler(runFuture, BoltProtocolV3.METADATA_EXTRACTOR, connection, null);

        assertFalse(runFuture.isDone());
        handler.onFailure(ConnectionReadTimeoutException.INSTANCE);

        assertTrue(runFuture.isCompletedExceptionally());
        ConnectionReadTimeoutException actualException =
                assertThrows(ConnectionReadTimeoutException.class, () -> await(runFuture));
        assertSame(ConnectionReadTimeoutException.INSTANCE, actualException);
        verify(connection).terminateAndRelease(ConnectionReadTimeoutException.INSTANCE.getMessage());
        verify(connection, never()).release();
    }

    private static void testResultAvailableAfterOnSuccess(String key, MetadataExtractor metadataExtractor) {
        RunResponseHandler handler = newHandler(metadataExtractor);

        handler.onSuccess(singletonMap(key, value(42)));

        assertEquals(42L, handler.resultAvailableAfter());
    }

    private static RunResponseHandler newHandler() {
        return newHandler(BoltProtocolV3.METADATA_EXTRACTOR);
    }

    private static RunResponseHandler newHandler(CompletableFuture<Void> runFuture) {
        return newHandler(runFuture, BoltProtocolV3.METADATA_EXTRACTOR);
    }

    private static RunResponseHandler newHandler(MetadataExtractor metadataExtractor) {
        return newHandler(new CompletableFuture<>(), metadataExtractor);
    }

    private static RunResponseHandler newHandler(
            CompletableFuture<Void> runFuture, MetadataExtractor metadataExtractor) {
        return new RunResponseHandler(runFuture, metadataExtractor, mock(Connection.class), null);
    }
}
