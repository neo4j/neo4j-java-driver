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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.util.TestUtil.await;

import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.internal.util.Futures;

class DisposableAsyncResultCursorTest {
    DisposableAsyncResultCursor cursor;

    AsyncResultCursor delegate;

    @BeforeEach
    void beforeEach() {
        delegate = mock(AsyncResultCursor.class);

        when(delegate.consumeAsync()).thenReturn(Futures.completedWithNull());
        when(delegate.discardAllFailureAsync()).thenReturn(Futures.completedWithNull());
        when(delegate.peekAsync()).thenReturn(Futures.completedWithNull());
        when(delegate.nextAsync()).thenReturn(Futures.completedWithNull());
        when(delegate.singleAsync()).thenReturn(Futures.completedWithNull());
        when(delegate.forEachAsync(any())).thenReturn(Futures.completedWithNull());
        when(delegate.listAsync()).thenReturn(Futures.completedWithNull());
        when(delegate.listAsync(any())).thenReturn(Futures.completedWithNull());
        when(delegate.pullAllFailureAsync()).thenReturn(Futures.completedWithNull());
        when(delegate.mapSuccessfulRunCompletionAsync()).thenReturn(CompletableFuture.completedFuture(delegate));

        cursor = new DisposableAsyncResultCursor(delegate);
    }

    @Test
    void summaryShouldDisposeCursor() {
        // When
        await(cursor.consumeAsync());

        // Then
        assertTrue(cursor.isDisposed());
    }

    @Test
    void consumeShouldDisposeCursor() {
        // When
        await(cursor.discardAllFailureAsync());

        // Then
        assertTrue(cursor.isDisposed());
    }

    @Test
    void shouldNotDisposeCursor() {
        // When
        cursor.keys();
        await(cursor.peekAsync());
        await(cursor.nextAsync());
        await(cursor.singleAsync());
        await(cursor.forEachAsync(record -> {}));
        await(cursor.listAsync());
        await(cursor.listAsync(record -> record));
        await(cursor.pullAllFailureAsync());

        // Then
        assertFalse(cursor.isDisposed());
    }

    @Test
    void shouldReturnItselfOnMapSuccessfulRunCompletionAsync() {
        // When
        AsyncResultCursor actual = await(cursor.mapSuccessfulRunCompletionAsync());

        // Then
        then(delegate).should().mapSuccessfulRunCompletionAsync();
        assertSame(cursor, actual);
    }

    @Test
    void shouldFailOnMapSuccessfulRunCompletionAsyncFailure() {
        // Given
        Throwable error = mock(Throwable.class);
        given(delegate.mapSuccessfulRunCompletionAsync()).willReturn(Futures.failedFuture(error));

        // When
        Throwable actual = assertThrows(Throwable.class, () -> await(cursor.mapSuccessfulRunCompletionAsync()));

        // Then
        then(delegate).should().mapSuccessfulRunCompletionAsync();
        assertSame(error, actual);
    }
}
