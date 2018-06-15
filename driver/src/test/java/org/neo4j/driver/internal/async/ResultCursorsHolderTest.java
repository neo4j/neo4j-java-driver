/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeoutException;

import org.neo4j.driver.internal.InternalStatementResultCursor;
import org.neo4j.driver.internal.util.Futures;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.v1.util.TestUtil.await;

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

    private CompletionStage<InternalStatementResultCursor> cursorWithoutError()
    {
        return cursorWithError( null );
    }

    private CompletionStage<InternalStatementResultCursor> cursorWithError( Throwable error )
    {
        InternalStatementResultCursor cursor = mock( InternalStatementResultCursor.class );
        when( cursor.failureAsync() ).thenReturn( completedFuture( error ) );
        return completedFuture( cursor );
    }
}
