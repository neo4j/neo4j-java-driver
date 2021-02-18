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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import org.neo4j.driver.internal.handlers.PullAllResponseHandler;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.handlers.pulln.PullResponseHandler;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.spi.Connection;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.internal.util.Futures.failedFuture;
import static org.neo4j.driver.internal.util.Futures.getNow;

class ResultCursorFactoryImplTest
{
    private static Stream<Boolean> waitForRun()
    {
        return Stream.of( true, false );
    }

    // asyncResult
    @ParameterizedTest
    @MethodSource( "waitForRun" )
    void shouldReturnAsyncResultWhenRunSucceeded( boolean waitForRun ) throws Throwable
    {
        // Given
        Connection connection = mock( Connection.class );
        ResultCursorFactory cursorFactory = newResultCursorFactory( connection, completedWithNull(), waitForRun );

        // When
        CompletionStage<AsyncResultCursor> cursorFuture = cursorFactory.asyncResult();

        // Then
        verifyRunCompleted( connection, cursorFuture );
    }

    @ParameterizedTest
    @MethodSource( "waitForRun" )
    void shouldReturnAsyncResultWhenRunCompletedWithFailure( boolean waitForRun ) throws Throwable
    {
        // Given
        Connection connection = mock( Connection.class );
        Throwable error = new RuntimeException( "Hi there" );
        ResultCursorFactory cursorFactory = newResultCursorFactory( connection, completedFuture( error ), waitForRun );

        // When
        CompletionStage<AsyncResultCursor> cursorFuture = cursorFactory.asyncResult();

        // Then
        verifyRunCompleted( connection, cursorFuture );
    }

    @Test
    void shouldFailAsyncResultWhenRunFailed() throws Throwable
    {
        // Given
        Throwable error = new RuntimeException( "Hi there" );
        ResultCursorFactory cursorFactory = newResultCursorFactory( failedFuture( error ), true );

        // When
        CompletionStage<AsyncResultCursor> cursorFuture = cursorFactory.asyncResult();

        // Then
        CompletionException actual = assertThrows( CompletionException.class, () -> getNow( cursorFuture ) );
        assertThat( actual.getCause(), equalTo( error ) );
    }

    @Test
    void shouldNotFailAsyncResultEvenWhenRunFailed() throws Throwable
    {
        // Given
        Connection connection = mock( Connection.class );
        Throwable error = new RuntimeException( "Hi there" );
        ResultCursorFactory cursorFactory = newResultCursorFactory( connection, failedFuture( error ), false );

        // When
        CompletionStage<AsyncResultCursor> cursorFuture = cursorFactory.asyncResult();

        // Then
        verifyRunCompleted( connection, cursorFuture );
    }

    @ParameterizedTest
    @MethodSource( "waitForRun" )
    void shouldPrePopulateRecords( boolean waitForRun ) throws Throwable
    {
        // Given
        Connection connection = mock( Connection.class );
        Message runMessage = mock( Message.class );

        RunResponseHandler runHandler = mock( RunResponseHandler.class );
        when( runHandler.runFuture() ).thenReturn( completedWithNull() );

        PullResponseHandler pullHandler = mock( PullResponseHandler.class );
        PullAllResponseHandler pullAllHandler = mock( PullAllResponseHandler.class );

        ResultCursorFactory cursorFactory = new ResultCursorFactoryImpl( connection, runMessage, runHandler, pullHandler, pullAllHandler, waitForRun );

        // When
        cursorFactory.asyncResult();

        // Then
        verify( pullAllHandler ).prePopulateRecords();
        verifyNoMoreInteractions( pullHandler );
    }

    // rxResult
    @ParameterizedTest
    @MethodSource( "waitForRun" )
    void shouldReturnRxResultWhenRunSucceeded( boolean waitForRun ) throws Throwable
    {
        // Given
        Connection connection = mock( Connection.class );
        ResultCursorFactory cursorFactory = newResultCursorFactory( connection, completedWithNull(), waitForRun );

        // When
        CompletionStage<RxResultCursor> cursorFuture = cursorFactory.rxResult();

        // Then
        verifyRxRunCompleted( connection, cursorFuture );
    }

    @ParameterizedTest
    @MethodSource( "waitForRun" )
    void shouldReturnRxResultWhenRunCompletedWithFailure( boolean waitForRun ) throws Throwable
    {
        // Given
        Connection connection = mock( Connection.class );
        Throwable error = new RuntimeException( "Hi there" );
        ResultCursorFactory cursorFactory = newResultCursorFactory( connection, completedFuture( error ), waitForRun );

        // When
        CompletionStage<RxResultCursor> cursorFuture = cursorFactory.rxResult();

        // Then
        verifyRxRunCompleted( connection, cursorFuture );
    }

    @Test
    void shouldFailRxResultWhenRunFailed() throws Throwable
    {
        // Given
        Throwable error = new RuntimeException( "Hi there" );
        ResultCursorFactory cursorFactory = newResultCursorFactory( failedFuture( error ), true );

        // When & Then
        CompletionStage<RxResultCursor> rxCursorFuture = cursorFactory.rxResult();
        CompletionException actual = assertThrows( CompletionException.class, () -> getNow( rxCursorFuture ) );
        assertThat( actual.getCause(), equalTo( error ) );
    }

    @Test
    void shouldNotFailRxResultEvenWhenRunFailed() throws Throwable
    {
        // Given
        Connection connection = mock( Connection.class );
        Throwable error = new RuntimeException( "Hi there" );
        ResultCursorFactory cursorFactory = newResultCursorFactory( connection, completedFuture( error ), false );

        // When
        CompletionStage<RxResultCursor> cursorFuture = cursorFactory.rxResult();

        // Then
        verifyRxRunCompleted( connection, cursorFuture );
    }

    private ResultCursorFactoryImpl newResultCursorFactory(Connection connection, CompletableFuture<Throwable> runFuture, boolean waitForRun )
    {
        Message runMessage = mock( Message.class );

        RunResponseHandler runHandler = mock( RunResponseHandler.class );
        when( runHandler.runFuture() ).thenReturn( runFuture );

        PullResponseHandler pullHandler = mock( PullResponseHandler.class );
        PullAllResponseHandler pullAllHandler = mock( PullAllResponseHandler.class );

        return new ResultCursorFactoryImpl( connection, runMessage, runHandler, pullHandler, pullAllHandler, waitForRun );
    }

    private ResultCursorFactoryImpl newResultCursorFactory(CompletableFuture<Throwable> runFuture, boolean waitForRun )
    {
        Connection connection = mock( Connection.class );
        return newResultCursorFactory( connection, runFuture, waitForRun );
    }

    private void verifyRunCompleted( Connection connection, CompletionStage<AsyncResultCursor> cursorFuture )
    {
        verify( connection ).write( any( Message.class ), any( RunResponseHandler.class ) );
        assertThat( getNow( cursorFuture ), instanceOf( AsyncResultCursor.class ) );
    }

    private void verifyRxRunCompleted( Connection connection, CompletionStage<RxResultCursor> cursorFuture )
    {
        verify( connection ).writeAndFlush( any( Message.class ), any( RunResponseHandler.class ) );
        assertThat( getNow( cursorFuture ), instanceOf( RxResultCursorImpl.class ) );
    }
}
