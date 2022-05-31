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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.driver.internal.util.Futures.getNow;
import static org.neo4j.driver.util.TestUtil.await;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.internal.handlers.PullAllResponseHandler;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.handlers.pulln.PullResponseHandler;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.spi.Connection;

class ResultCursorFactoryImplTest {
    // asyncResult
    @Test
    void shouldReturnAsyncResultWhenRunSucceeded() {
        // Given
        Connection connection = mock(Connection.class);
        ResultCursorFactory cursorFactory = newResultCursorFactory(connection, null);

        // When
        CompletionStage<AsyncResultCursor> cursorFuture = cursorFactory.asyncResult();

        // Then
        verifyRunCompleted(connection, cursorFuture);
    }

    @Test
    void shouldReturnAsyncResultWithRunErrorWhenRunFailed() {
        // Given
        Throwable error = new RuntimeException("Hi there");
        ResultCursorFactory cursorFactory = newResultCursorFactory(error);

        // When
        CompletionStage<AsyncResultCursor> cursorFuture = cursorFactory.asyncResult();

        // Then
        AsyncResultCursor cursor = getNow(cursorFuture);
        Throwable actual = assertThrows(error.getClass(), () -> await(cursor.mapSuccessfulRunCompletionAsync()));
        assertSame(error, actual);
    }

    @Test
    void shouldPrePopulateRecords() {
        // Given
        Connection connection = mock(Connection.class);
        Message runMessage = mock(Message.class);

        RunResponseHandler runHandler = mock(RunResponseHandler.class);
        CompletableFuture<Void> runFuture = new CompletableFuture<>();

        PullResponseHandler pullHandler = mock(PullResponseHandler.class);
        PullAllResponseHandler pullAllHandler = mock(PullAllResponseHandler.class);

        ResultCursorFactory cursorFactory =
                new ResultCursorFactoryImpl(connection, runMessage, runHandler, runFuture, pullHandler, pullAllHandler);

        // When
        cursorFactory.asyncResult();

        // Then
        verify(pullAllHandler).prePopulateRecords();
        verifyNoMoreInteractions(pullHandler);
    }

    // rxResult
    @Test
    void shouldReturnRxResultWhenRunSucceeded() {
        // Given
        Connection connection = mock(Connection.class);
        ResultCursorFactory cursorFactory = newResultCursorFactory(connection, null);

        // When
        CompletionStage<RxResultCursor> cursorFuture = cursorFactory.rxResult();

        // Then
        verifyRxRunCompleted(connection, cursorFuture);
    }

    @Test
    void shouldReturnRxResultWhenRunFailed() {
        // Given
        Connection connection = mock(Connection.class);
        Throwable error = new RuntimeException("Hi there");
        ResultCursorFactory cursorFactory = newResultCursorFactory(connection, error);

        // When
        CompletionStage<RxResultCursor> cursorFuture = cursorFactory.rxResult();

        // Then
        verifyRxRunCompleted(connection, cursorFuture);
    }

    private ResultCursorFactoryImpl newResultCursorFactory(Connection connection, Throwable runError) {
        Message runMessage = mock(Message.class);

        RunResponseHandler runHandler = mock(RunResponseHandler.class);
        CompletableFuture<Void> runFuture = new CompletableFuture<>();
        if (runError != null) {
            runFuture.completeExceptionally(runError);
        } else {
            runFuture.complete(null);
        }

        PullResponseHandler pullHandler = mock(PullResponseHandler.class);
        PullAllResponseHandler pullAllHandler = mock(PullAllResponseHandler.class);

        return new ResultCursorFactoryImpl(connection, runMessage, runHandler, runFuture, pullHandler, pullAllHandler);
    }

    private ResultCursorFactoryImpl newResultCursorFactory(Throwable runError) {
        Connection connection = mock(Connection.class);
        return newResultCursorFactory(connection, runError);
    }

    private void verifyRunCompleted(Connection connection, CompletionStage<AsyncResultCursor> cursorFuture) {
        verify(connection).write(any(Message.class), any(RunResponseHandler.class));
        assertThat(getNow(cursorFuture), instanceOf(AsyncResultCursor.class));
    }

    private void verifyRxRunCompleted(Connection connection, CompletionStage<RxResultCursor> cursorFuture) {
        verify(connection).writeAndFlush(any(Message.class), any(RunResponseHandler.class));
        assertThat(getNow(cursorFuture), instanceOf(RxResultCursorImpl.class));
    }
}
