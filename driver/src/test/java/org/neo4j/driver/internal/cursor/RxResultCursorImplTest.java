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
package org.neo4j.driver.internal.cursor;

import static java.util.Arrays.asList;
import static junit.framework.TestCase.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.neo4j.driver.exceptions.ResultConsumedException;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.handlers.pulln.PullResponseHandler;
import org.neo4j.driver.internal.reactive.util.ListBasedPullHandler;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.summary.ResultSummary;

class RxResultCursorImplTest {
    @Test
    void shouldInstallSummaryConsumerWithoutReportingError() {
        // Given
        RuntimeException error = new RuntimeException("Hi");
        RunResponseHandler runHandler = newRunResponseHandler(error);
        PullResponseHandler pullHandler = mock(PullResponseHandler.class);

        // When
        new RxResultCursorImpl(error, runHandler, pullHandler);

        // Then
        verify(pullHandler).installSummaryConsumer(any(BiConsumer.class));
        verifyNoMoreInteractions(pullHandler);
    }

    @Test
    void shouldReturnQueryKeys() {
        // Given
        RunResponseHandler runHandler = newRunResponseHandler();
        List<String> expected = asList("key1", "key2", "key3");
        runHandler.onSuccess(Collections.singletonMap("fields", value(expected)));

        PullResponseHandler pullHandler = mock(PullResponseHandler.class);

        // When
        RxResultCursor cursor = new RxResultCursorImpl(runHandler, pullHandler);
        List<String> actual = cursor.keys();

        // Then
        assertEquals(expected, actual);
    }

    @Test
    void shouldSupportReturnQueryKeysMultipleTimes() {
        // Given
        RunResponseHandler runHandler = newRunResponseHandler();
        List<String> expected = asList("key1", "key2", "key3");
        runHandler.onSuccess(Collections.singletonMap("fields", value(expected)));

        PullResponseHandler pullHandler = mock(PullResponseHandler.class);

        // When
        RxResultCursor cursor = new RxResultCursorImpl(runHandler, pullHandler);

        // Then
        List<String> actual = cursor.keys();
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
        RunResponseHandler runHandler = newRunResponseHandler();
        PullResponseHandler pullHandler = mock(PullResponseHandler.class);
        RxResultCursor cursor = new RxResultCursorImpl(runHandler, pullHandler);

        // When
        cursor.request(100);

        // Then
        verify(pullHandler).request(100);
    }

    @Test
    void shouldPullUnboundedOnLongMax() {
        // Given
        RunResponseHandler runHandler = newRunResponseHandler();
        PullResponseHandler pullHandler = mock(PullResponseHandler.class);
        RxResultCursor cursor = new RxResultCursorImpl(runHandler, pullHandler);

        // When
        cursor.request(Long.MAX_VALUE);

        // Then
        verify(pullHandler).request(-1);
    }

    @Test
    void shouldCancel() {
        // Given
        RunResponseHandler runHandler = newRunResponseHandler();
        PullResponseHandler pullHandler = mock(PullResponseHandler.class);
        RxResultCursor cursor = new RxResultCursorImpl(runHandler, pullHandler);

        // When
        cursor.cancel();

        // Then
        verify(pullHandler).cancel();
    }

    @Test
    void shouldInstallRecordConsumerAndReportError() {
        // Given
        RuntimeException error = new RuntimeException("Hi");
        BiConsumer recordConsumer = mock(BiConsumer.class);

        // When
        RunResponseHandler runHandler = newRunResponseHandler(error);
        PullResponseHandler pullHandler = new ListBasedPullHandler();
        RxResultCursor cursor = new RxResultCursorImpl(error, runHandler, pullHandler);
        cursor.installRecordConsumer(recordConsumer);

        // Then
        verify(recordConsumer).accept(null, error);
        verifyNoMoreInteractions(recordConsumer);
    }

    @Test
    void shouldReturnSummaryFuture() {
        // Given
        RunResponseHandler runHandler = newRunResponseHandler();
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
        RunResponseHandler runHandler = newRunResponseHandler();
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
        RunResponseHandler runHandler = newRunResponseHandler();
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
        RunResponseHandler runHandler = newRunResponseHandler();
        PullResponseHandler pullHandler = mock(PullResponseHandler.class);
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
        RunResponseHandler runHandler = newRunResponseHandler();
        PullResponseHandler pullHandler = mock(PullResponseHandler.class);
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
        RunResponseHandler runHandler = mock(RunResponseHandler.class);
        PullResponseHandler pullHandler = mock(PullResponseHandler.class);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<BiConsumer<ResultSummary, Throwable>> summaryConsumerCaptor =
                ArgumentCaptor.forClass(BiConsumer.class);
        RxResultCursor cursor = new RxResultCursorImpl(runHandler, pullHandler);
        verify(pullHandler, times(1)).installSummaryConsumer(summaryConsumerCaptor.capture());
        BiConsumer<ResultSummary, Throwable> summaryConsumer = summaryConsumerCaptor.getValue();
        RuntimeException exception = mock(RuntimeException.class);

        // When
        CompletionStage<ResultSummary> summaryStage = cursor.summaryAsync();
        CompletionStage<Throwable> discardStage = cursor.discardAllFailureAsync();
        summaryConsumer.accept(null, exception);

        // Then
        verify(pullHandler).installRecordConsumer(DISCARD_RECORD_CONSUMER);
        verify(pullHandler).cancel();
        ExecutionException actualException = assertThrows(
                ExecutionException.class,
                () -> summaryStage.toCompletableFuture().get());
        assertSame(exception, actualException.getCause());
        assertNull(discardStage.toCompletableFuture().get());
    }

    @Test
    void shouldPropagateSummaryErrorViaDiscardStageWhenSummaryStageIsNotRetrievedExternally()
            throws ExecutionException, InterruptedException {
        // Given
        RunResponseHandler runHandler = mock(RunResponseHandler.class);
        PullResponseHandler pullHandler = mock(PullResponseHandler.class);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<BiConsumer<ResultSummary, Throwable>> summaryConsumerCaptor =
                ArgumentCaptor.forClass(BiConsumer.class);
        RxResultCursor cursor = new RxResultCursorImpl(runHandler, pullHandler);
        verify(pullHandler, times(1)).installSummaryConsumer(summaryConsumerCaptor.capture());
        BiConsumer<ResultSummary, Throwable> summaryConsumer = summaryConsumerCaptor.getValue();
        RuntimeException exception = mock(RuntimeException.class);

        // When
        CompletionStage<Throwable> discardStage = cursor.discardAllFailureAsync();
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
