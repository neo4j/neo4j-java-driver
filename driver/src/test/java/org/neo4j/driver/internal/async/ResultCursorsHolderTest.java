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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.testutil.TestUtil.await;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.internal.FailableCursor;

class ResultCursorsHolderTest {
    @Test
    void shouldReturnNoErrorWhenNoCursorStages() {
        var holder = new ResultCursorsHolder();

        var error = await(holder.retrieveNotConsumedError());
        assertNull(error);
    }

    @Test
    void shouldFailToAddNullCursorStage() {
        var holder = new ResultCursorsHolder();

        assertThrows(NullPointerException.class, () -> holder.add(null));
    }

    @Test
    void shouldReturnNoErrorWhenCursorStagesHaveNoErrors() {
        var holder = new ResultCursorsHolder();

        holder.add(cursorWithoutError());
        holder.add(cursorWithoutError());
        holder.add(cursorWithoutError());
        holder.add(cursorWithoutError());

        var error = await(holder.retrieveNotConsumedError());
        assertNull(error);
    }

    @Test
    void shouldNotReturnStageErrors() {
        var holder = new ResultCursorsHolder();

        holder.add(failedFuture(new RuntimeException("Failed to acquire a connection")));
        holder.add(cursorWithoutError());
        holder.add(cursorWithoutError());
        holder.add(failedFuture(new IOException("Failed to do IO")));

        var error = await(holder.retrieveNotConsumedError());
        assertNull(error);
    }

    @Test
    void shouldReturnErrorWhenOneCursorFailed() {
        var error = new IOException("IO failed");
        var holder = new ResultCursorsHolder();

        holder.add(cursorWithoutError());
        holder.add(cursorWithoutError());
        holder.add(cursorWithError(error));
        holder.add(cursorWithoutError());

        var retrievedError = await(holder.retrieveNotConsumedError());
        assertEquals(error, retrievedError);
    }

    @Test
    void shouldReturnFirstError() {
        var error1 = new RuntimeException("Error 1");
        var error2 = new IOException("Error 2");
        var error3 = new TimeoutException("Error 3");
        var holder = new ResultCursorsHolder();

        holder.add(cursorWithoutError());
        holder.add(cursorWithError(error1));
        holder.add(cursorWithError(error2));
        holder.add(cursorWithError(error3));

        assertEquals(error1, await(holder.retrieveNotConsumedError()));
    }

    @Test
    void shouldWaitForAllFailuresToArrive() {
        var error1 = new RuntimeException("Error 1");
        var error2Future = new CompletableFuture<Throwable>();
        var holder = new ResultCursorsHolder();

        holder.add(cursorWithoutError());
        holder.add(cursorWithError(error1));
        holder.add(cursorWithFailureFuture(error2Future));

        var failureFuture = holder.retrieveNotConsumedError().toCompletableFuture();
        assertFalse(failureFuture.isDone());

        error2Future.complete(null);
        assertTrue(failureFuture.isDone());

        assertEquals(error1, await(failureFuture));
    }

    @Test
    void shouldRemoveConsumedResults() {
        var holder = new ResultCursorsHolder();
        var list = IntStream.range(0, 100)
                .mapToObj(i -> {
                    var cursor = mock(FailableCursor.class);
                    var consume = new CompletableFuture<Void>();
                    given(cursor.consumed()).willReturn(consume);
                    holder.add(CompletableFuture.completedFuture(cursor));
                    if (i % 2 == 0) {
                        consume.complete(null);
                        given(cursor.discardAllFailureAsync())
                                .willReturn(CompletableFuture.failedFuture(new RuntimeException()));
                    } else {
                        given(cursor.discardAllFailureAsync()).willReturn(CompletableFuture.completedStage(null));
                    }
                    return cursor;
                })
                .toList();

        holder.retrieveNotConsumedError().toCompletableFuture().join();

        for (var i = 0; i < list.size(); i++) {
            var cursor = list.get(i);
            then(cursor).should().consumed();
            if (i % 2 == 1) {
                then(cursor).should().discardAllFailureAsync();
            }
            then(cursor).shouldHaveNoMoreInteractions();
        }
    }

    private static CompletionStage<FailableCursor> cursorWithoutError() {
        return cursorWithError(null);
    }

    private static CompletionStage<FailableCursor> cursorWithError(Throwable error) {
        return cursorWithFailureFuture(completedFuture(error));
    }

    private static CompletionStage<FailableCursor> cursorWithFailureFuture(CompletableFuture<Throwable> future) {
        var cursor = mock(FailableCursor.class);
        when(cursor.consumed()).thenReturn(new CompletableFuture<>());
        when(cursor.discardAllFailureAsync()).thenReturn(future);
        return completedFuture(cursor);
    }
}
