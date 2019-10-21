/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.junit.jupiter.api.Test;

import org.neo4j.driver.internal.util.Futures;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.util.TestUtil.await;

class DisposableAsyncStatementResultCursorTest
{
    @Test
    void summaryShouldDisposeCursor() throws Throwable
    {
        // Given
        DisposableAsyncStatementResultCursor cursor = newCursor();

        // When
        await( cursor.summaryAsync() );

        // Then
        assertTrue( cursor.isDisposed() );
    }

    @Test
    void consumeShouldDisposeCursor() throws Throwable
    {
        // Given
        DisposableAsyncStatementResultCursor cursor = newCursor();

        // When
        await( cursor.consumeAsync() );

        // Then
        assertTrue( cursor.isDisposed() );
    }

    @Test
    void shouldNotDisposeCursor() throws Throwable
    {
        // Given
        DisposableAsyncStatementResultCursor cursor = newCursor();

        // When
        cursor.keys();
        await( cursor.peekAsync() );
        await( cursor.nextAsync() );
        await( cursor.singleAsync() );
        await( cursor.forEachAsync( record -> {} ) );
        await( cursor.listAsync() );
        await( cursor.listAsync( record -> record ) );
        await( cursor.failureAsync() );

        // Then
        assertFalse( cursor.isDisposed() );
    }

    private static DisposableAsyncStatementResultCursor newCursor()
    {
        AsyncStatementResultCursor delegate = mock( AsyncStatementResultCursor.class );
        when( delegate.summaryAsync() ).thenReturn( Futures.completedWithNull() );
        when( delegate.consumeAsync() ).thenReturn( Futures.completedWithNull() );
        when( delegate.peekAsync() ).thenReturn( Futures.completedWithNull() );
        when( delegate.nextAsync() ).thenReturn( Futures.completedWithNull() );
        when( delegate.singleAsync() ).thenReturn( Futures.completedWithNull() );
        when( delegate.forEachAsync( any() ) ).thenReturn( Futures.completedWithNull() );
        when( delegate.listAsync() ).thenReturn( Futures.completedWithNull() );
        when( delegate.listAsync( any() ) ).thenReturn( Futures.completedWithNull() );
        when( delegate.failureAsync() ).thenReturn( Futures.completedWithNull() );
        return new DisposableAsyncStatementResultCursor( delegate );
    }
}
