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
package org.neo4j.driver.internal.async;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeoutException;

import org.neo4j.driver.internal.cursor.AsyncResultCursorImpl;
import org.neo4j.driver.internal.util.Futures;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.util.TestUtil.await;

class ResultCursorsHolderTest
{
    @Test
    void shouldReturnNoErrorWhenNoCursorStages()
    {
        ResultCursorsHolder holder = new ResultCursorsHolder();

        Throwable error = await( holder.retrieveNotConsumedError() );
        assertNull( error );
    }

    @Test
    void shouldFailToAddNullCursorStage()
    {
        ResultCursorsHolder holder = new ResultCursorsHolder();

        assertThrows( NullPointerException.class, () -> holder.add( null ) );
    }

    @Test
    void shouldReturnNoErrorWhenCursorStagesHaveNoErrors()
    {
        ResultCursorsHolder holder = new ResultCursorsHolder();

        holder.add( cursorWithoutError() );
        holder.add( cursorWithoutError() );
        holder.add( cursorWithoutError() );
        holder.add( cursorWithoutError() );

        Throwable error = await( holder.retrieveNotConsumedError() );
        assertNull( error );
    }

    @Test
    void shouldNotReturnStageErrors()
    {
        ResultCursorsHolder holder = new ResultCursorsHolder();

        holder.add( Futures.failedFuture( new RuntimeException( "Failed to acquire a connection" ) ) );
        holder.add( cursorWithoutError() );
        holder.add( cursorWithoutError() );
        holder.add( Futures.failedFuture( new IOException( "Failed to do IO" ) ) );

        Throwable error = await( holder.retrieveNotConsumedError() );
        assertNull( error );
    }

    @Test
    void shouldReturnErrorWhenOneCursorFailed()
    {
        IOException error = new IOException( "IO failed" );
        ResultCursorsHolder holder = new ResultCursorsHolder();

        holder.add( cursorWithoutError() );
        holder.add( cursorWithoutError() );
        holder.add( cursorWithError( error ) );
        holder.add( cursorWithoutError() );

        Throwable retrievedError = await( holder.retrieveNotConsumedError() );
        assertEquals( error, retrievedError );
    }

    @Test
    void shouldReturnFirstError()
    {
        RuntimeException error1 = new RuntimeException( "Error 1" );
        IOException error2 = new IOException( "Error 2" );
        TimeoutException error3 = new TimeoutException( "Error 3" );
        ResultCursorsHolder holder = new ResultCursorsHolder();

        holder.add( cursorWithoutError() );
        holder.add( cursorWithError( error1 ) );
        holder.add( cursorWithError( error2 ) );
        holder.add( cursorWithError( error3 ) );

        assertEquals( error1, await( holder.retrieveNotConsumedError() ) );
    }

    @Test
    void shouldWaitForAllFailuresToArrive()
    {
        RuntimeException error1 = new RuntimeException( "Error 1" );
        CompletableFuture<Throwable> error2Future = new CompletableFuture<>();
        ResultCursorsHolder holder = new ResultCursorsHolder();

        holder.add( cursorWithoutError() );
        holder.add( cursorWithError( error1 ) );
        holder.add( cursorWithFailureFuture( error2Future ) );

        CompletableFuture<Throwable> failureFuture = holder.retrieveNotConsumedError().toCompletableFuture();
        assertFalse( failureFuture.isDone() );

        error2Future.complete( null );
        assertTrue( failureFuture.isDone() );

        assertEquals( error1, await( failureFuture ) );
    }

    private static CompletionStage<AsyncResultCursorImpl> cursorWithoutError()
    {
        return cursorWithError( null );
    }

    private static CompletionStage<AsyncResultCursorImpl> cursorWithError(Throwable error )
    {
        return cursorWithFailureFuture( completedFuture( error ) );
    }

    private static CompletionStage<AsyncResultCursorImpl> cursorWithFailureFuture(CompletableFuture<Throwable> future )
    {
        AsyncResultCursorImpl cursor = mock( AsyncResultCursorImpl.class );
        when( cursor.discardAllFailureAsync() ).thenReturn( future );
        return completedFuture( cursor );
    }
}
