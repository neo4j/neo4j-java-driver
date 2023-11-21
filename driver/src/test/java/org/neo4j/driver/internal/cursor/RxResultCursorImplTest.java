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

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.internal.cursor.RxResultCursorImpl.DISCARD_RECORD_CONSUMER;
import static org.neo4j.driver.internal.messaging.v3.BoltProtocolV3.METADATA_EXTRACTOR;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.internal.util.Futures.failedFuture;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.neo4j.driver.Record;
import org.neo4j.driver.exceptions.ResultConsumedException;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.handlers.pulln.PullResponseHandler;
import org.neo4j.driver.internal.reactive.util.ListBasedPullHandler;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.summary.ResultSummary;

class RxResultCursorImplTest {
    @Test
    @SuppressWarnings("unchecked")
    void shouldInstallSummaryConsumerWithoutReportingError() {
        // Given
        var error = new RuntimeException("Hi");
        var runHandler = newRunResponseHandler(error);
        var pullHandler = mock(PullResponseHandler.class);

        // When
        new RxResultCursorImpl(error, runHandler, pullHandler, () -> CompletableFuture.completedStage(null));

        // Then
        verify(pullHandler).installSummaryConsumer(any(BiConsumer.class));
        verifyNoMoreInteractions(pullHandler);
    }

    @Test
    void shouldReturnQueryKeys() {
        // Given
        var runHandler = newRunResponseHandler();
        var expected = asList("key1", "key2", "key3");
        runHandler.onSuccess(Collections.singletonMap("fields", value(expected)));

        var pullHandler = mock(PullResponseHandler.class);

        // When
        RxResultCursor cursor = new RxResultCursorImpl(runHandler, pullHandler);
        var actual = cursor.keys();

        // Then
        assertEquals(expected, actual);
    }

    @Test
    void shouldSupportReturnQueryKeysMultipleTimes() {
        // Given
        var runHandler = newRunResponseHandler();
        var expected = asList("key1", "key2", "key3");
        runHandler.onSuccess(Collections.singletonMap("fields", value(expected)));

        var pullHandler = mock(PullResponseHandler.class);

        // When
        RxResultCursor cursor = new RxResultCursorImpl(runHandler, pullHandler);

        // Then
        var actual = cursor.keys();
        assertEquals(expected, actual);

        // Many times
        actual = cursor.keys();
        assertEquals(expected, actual);

        actual = cursor.keys();
        assertEquals(expected, actual);
    }

    @Test
    void shouldPull() {
        // Given
        var runHandler = newRunResponseHandler();
        var pullHandler = mock(PullResponseHandler.class);
        RxResultCursor cursor = new RxResultCursorImpl(runHandler, pullHandler);

        // When
        cursor.request(100);

        // Then
        verify(pullHandler).request(100);
    }

    @Test
    void shouldPullUnboundedOnLongMax() {
        // Given
        var runHandler = newRunResponseHandler();
        var pullHandler = mock(PullResponseHandler.class);
        RxResultCursor cursor = new RxResultCursorImpl(runHandler, pullHandler);

        // When
        cursor.request(Long.MAX_VALUE);

        // Then
        verify(pullHandler).request(-1);
    }

    @Test
    void shouldCancel() {
        // Given
        var runHandler = newRunResponseHandler();
        var pullHandler = mock(PullResponseHandler.class);
        RxResultCursor cursor = new RxResultCursorImpl(runHandler, pullHandler);

        // When
        cursor.cancel();

        // Then
        verify(pullHandler).cancel();
    }

    @Test
    void shouldInstallRecordConsumerAndReportError() {
        // Given
        var error = new RuntimeException("Hi");
        @SuppressWarnings("unchecked")
        BiConsumer<Record, Throwable> recordConsumer = mock(BiConsumer.class);

        // When
        var runHandler = newRunResponseHandler(error);
        PullResponseHandler pullHandler = new ListBasedPullHandler();
        RxResultCursor cursor =
                new RxResultCursorImpl(error, runHandler, pullHandler, () -> CompletableFuture.completedStage(null));
        cursor.installRecordConsumer(recordConsumer);

        // Then
        verify(recordConsumer).accept(null, error);
        verifyNoMoreInteractions(recordConsumer);
    }

    @Test
    void shouldReturnSummaryFuture() {
        // Given
        var runHandler = newRunResponseHandler();
        PullResponseHandler pullHandler = new ListBasedPullHandler();
        RxResultCursor cursor = new RxResultCursorImpl(runHandler, pullHandler);

        // When
        cursor.installRecordConsumer(DISCARD_RECORD_CONSUMER);
        cursor.request(10);
        cursor.summaryAsync();

        // Then
        assertTrue(cursor.isDone());
    }

    @Test
    void shouldNotAllowToInstallRecordConsumerAfterSummary() {
        // Given
        var runHandler = newRunResponseHandler();
        PullResponseHandler pullHandler = new ListBasedPullHandler();
        RxResultCursor cursor = new RxResultCursorImpl(runHandler, pullHandler);

        // When
        cursor.summaryAsync();

        // Then
        assertThrows(ResultConsumedException.class, () -> cursor.installRecordConsumer(null));
    }

    @Test
    void shouldAllowToCallSummaryMultipleTimes() {
        // Given
        var runHandler = newRunResponseHandler();
        PullResponseHandler pullHandler = new ListBasedPullHandler();
        RxResultCursor cursor = new RxResultCursorImpl(runHandler, pullHandler);

        // When
        cursor.summaryAsync();

        // Then
        cursor.summaryAsync();
        cursor.summaryAsync();
    }

    @Test
    void shouldOnlyInstallRecordConsumerOnce() {
        // Given
        var runHandler = newRunResponseHandler();
        var pullHandler = mock(PullResponseHandler.class);
        RxResultCursor cursor = new RxResultCursorImpl(runHandler, pullHandler);

        // When
        cursor.installRecordConsumer(DISCARD_RECORD_CONSUMER); // any consumer
        cursor.installRecordConsumer(DISCARD_RECORD_CONSUMER); // any consumer

        // Then
        verify(pullHandler).installRecordConsumer(any());
    }

    @Test
    void shouldCancelIfNotPulled() {
        // Given
        var runHandler = newRunResponseHandler();
        var pullHandler = mock(PullResponseHandler.class);
        RxResultCursor cursor = new RxResultCursorImpl(runHandler, pullHandler);

        // When
        cursor.summaryAsync();

        // Then
        verify(pullHandler).installRecordConsumer(DISCARD_RECORD_CONSUMER);
        verify(pullHandler).cancel();
        assertFalse(cursor.isDone());
    }

    @Test
    void shouldPropagateSummaryErrorViaSummaryStageWhenItIsRetrievedExternally()
            throws ExecutionException, InterruptedException {
        // Given
        var runHandler = mock(RunResponseHandler.class);
        var pullHandler = mock(PullResponseHandler.class);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<BiConsumer<ResultSummary, Throwable>> summaryConsumerCaptor =
                ArgumentCaptor.forClass(BiConsumer.class);
        RxResultCursor cursor = new RxResultCursorImpl(runHandler, pullHandler);
        verify(pullHandler, times(1)).installSummaryConsumer(summaryConsumerCaptor.capture());
        var summaryConsumer = summaryConsumerCaptor.getValue();
        var exception = mock(RuntimeException.class);

        // When
        var summaryStage = cursor.summaryAsync();
        var discardStage = cursor.discardAllFailureAsync();
        summaryConsumer.accept(null, exception);

        // Then
        verify(pullHandler).installRecordConsumer(DISCARD_RECORD_CONSUMER);
        verify(pullHandler).cancel();
        var actualException = assertThrows(
                ExecutionException.class,
                () -> summaryStage.toCompletableFuture().get());
        assertSame(exception, actualException.getCause());
        assertNull(discardStage.toCompletableFuture().get());
    }

    @Test
    void shouldPropagateSummaryErrorViaDiscardStageWhenSummaryStageIsNotRetrievedExternally()
            throws ExecutionException, InterruptedException {
        // Given
        var runHandler = mock(RunResponseHandler.class);
        var pullHandler = mock(PullResponseHandler.class);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<BiConsumer<ResultSummary, Throwable>> summaryConsumerCaptor =
                ArgumentCaptor.forClass(BiConsumer.class);
        RxResultCursor cursor = new RxResultCursorImpl(runHandler, pullHandler);
        verify(pullHandler, times(1)).installSummaryConsumer(summaryConsumerCaptor.capture());
        var summaryConsumer = summaryConsumerCaptor.getValue();
        var exception = mock(RuntimeException.class);

        // When
        var discardStage = cursor.discardAllFailureAsync();
        summaryConsumer.accept(null, exception);

        // Then
        verify(pullHandler).installRecordConsumer(DISCARD_RECORD_CONSUMER);
        verify(pullHandler).cancel();
        assertSame(exception, discardStage.toCompletableFuture().get().getCause());
    }

    private static RunResponseHandler newRunResponseHandler(CompletableFuture<Void> runFuture) {
        return new RunResponseHandler(runFuture, METADATA_EXTRACTOR, mock(Connection.class), null);
    }

    private static RunResponseHandler newRunResponseHandler(Throwable error) {
        return newRunResponseHandler(failedFuture(error));
    }

    private static RunResponseHandler newRunResponseHandler() {
        return newRunResponseHandler(completedWithNull());
    }
}
