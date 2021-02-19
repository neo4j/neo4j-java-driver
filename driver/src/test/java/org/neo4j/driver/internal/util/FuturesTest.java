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
package org.neo4j.driver.internal.util;

import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.FailedFuture;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.SucceededFuture;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.neo4j.driver.internal.async.connection.EventLoopGroupFactory;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.internal.util.Matchers.blockingOperationInEventLoopError;
import static org.neo4j.driver.util.DaemonThreadFactory.daemon;
import static org.neo4j.driver.util.TestUtil.sleep;

class FuturesTest
{
    @Test
    void shouldConvertCanceledNettyFutureToCompletionStage() throws Exception
    {
        DefaultPromise<String> promise = new DefaultPromise<>( ImmediateEventExecutor.INSTANCE );
        promise.cancel( true );

        CompletableFuture<String> future = Futures.asCompletionStage( promise ).toCompletableFuture();

        assertTrue( future.isCancelled() );
        assertTrue( future.isCompletedExceptionally() );
        assertThrows( CancellationException.class, future::get );
    }

    @Test
    void shouldConvertSucceededNettyFutureToCompletionStage() throws Exception
    {
        SucceededFuture<String> nettyFuture = new SucceededFuture<>( ImmediateEventExecutor.INSTANCE, "Hello" );

        CompletableFuture<String> future = Futures.asCompletionStage( nettyFuture ).toCompletableFuture();

        assertTrue( future.isDone() );
        assertFalse( future.isCompletedExceptionally() );
        assertEquals( "Hello", future.get() );
    }

    @Test
    void shouldConvertFailedNettyFutureToCompletionStage() throws Exception
    {
        RuntimeException error = new RuntimeException( "Hello" );
        FailedFuture<Object> nettyFuture = new FailedFuture<>( ImmediateEventExecutor.INSTANCE, error );

        CompletableFuture<Object> future = Futures.asCompletionStage( nettyFuture ).toCompletableFuture();

        assertTrue( future.isCompletedExceptionally() );
        ExecutionException e = assertThrows( ExecutionException.class, future::get );
        assertEquals( error, e.getCause() );
    }

    @Test
    void shouldConvertRunningNettyFutureToCompletionStageWhenFutureCanceled() throws Exception
    {
        DefaultPromise<String> promise = new DefaultPromise<>( ImmediateEventExecutor.INSTANCE );

        CompletableFuture<String> future = Futures.asCompletionStage( promise ).toCompletableFuture();

        assertFalse( future.isDone() );
        promise.cancel( true );

        assertTrue( future.isCancelled() );
        assertTrue( future.isCompletedExceptionally() );
        assertThrows( CancellationException.class, future::get );
    }

    @Test
    void shouldConvertRunningNettyFutureToCompletionStageWhenFutureSucceeded() throws Exception
    {
        DefaultPromise<String> promise = new DefaultPromise<>( ImmediateEventExecutor.INSTANCE );

        CompletableFuture<String> future = Futures.asCompletionStage( promise ).toCompletableFuture();

        assertFalse( future.isDone() );
        promise.setSuccess( "Hello" );

        assertTrue( future.isDone() );
        assertFalse( future.isCompletedExceptionally() );
        assertEquals( "Hello", future.get() );
    }

    @Test
    void shouldConvertRunningNettyFutureToCompletionStageWhenFutureFailed() throws Exception
    {
        RuntimeException error = new RuntimeException( "Hello" );
        DefaultPromise<String> promise = new DefaultPromise<>( ImmediateEventExecutor.INSTANCE );

        CompletableFuture<String> future = Futures.asCompletionStage( promise ).toCompletableFuture();

        assertFalse( future.isDone() );
        promise.setFailure( error );

        assertTrue( future.isCompletedExceptionally() );
        ExecutionException e = assertThrows( ExecutionException.class, future::get );
        assertEquals( error, e.getCause() );
    }

    @Test
    void shouldCreateFailedFutureWithUncheckedException() throws Exception
    {
        RuntimeException error = new RuntimeException( "Hello" );
        CompletableFuture<Object> future = Futures.failedFuture( error ).toCompletableFuture();
        assertTrue( future.isCompletedExceptionally() );
        ExecutionException e = assertThrows( ExecutionException.class, future::get );
        assertEquals( error, e.getCause() );
    }

    @Test
    void shouldCreateFailedFutureWithCheckedException() throws Exception
    {
        IOException error = new IOException( "Hello" );
        CompletableFuture<Object> future = Futures.failedFuture( error ).toCompletableFuture();
        assertTrue( future.isCompletedExceptionally() );
        ExecutionException e = assertThrows( ExecutionException.class, future::get );
        assertEquals( error, e.getCause() );
    }

    @Test
    void shouldFailBlockingGetInEventLoopThread() throws Exception
    {
        EventLoopGroup eventExecutor = EventLoopGroupFactory.newEventLoopGroup( 1 );
        try
        {
            CompletableFuture<String> future = new CompletableFuture<>();
            Future<String> result = eventExecutor.submit( () -> Futures.blockingGet( future ) );

            ExecutionException e = assertThrows( ExecutionException.class, result::get );
            assertThat( e.getCause(), is( blockingOperationInEventLoopError() ) );
        }
        finally
        {
            eventExecutor.shutdownGracefully();
        }
    }

    @Test
    void shouldThrowInBlockingGetWhenFutureThrowsUncheckedException()
    {
        RuntimeException error = new RuntimeException( "Hello" );

        CompletableFuture<String> future = new CompletableFuture<>();
        future.completeExceptionally( error );

        Exception e = assertThrows( Exception.class, () -> Futures.blockingGet( future ) );
        assertEquals( error, e );
    }

    @Test
    void shouldThrowInBlockingGetWhenFutureThrowsCheckedException()
    {
        IOException error = new IOException( "Hello" );

        CompletableFuture<String> future = new CompletableFuture<>();
        future.completeExceptionally( error );

        Exception e = assertThrows( Exception.class, () -> Futures.blockingGet( future ) );
        assertEquals( error, e );
    }

    @Test
    void shouldReturnFromBlockingGetWhenFutureCompletes()
    {
        CompletableFuture<String> future = new CompletableFuture<>();
        future.complete( "Hello" );

        assertEquals( "Hello", Futures.blockingGet( future ) );
    }

    @Test
    void shouldWaitForFutureInBlockingGetEvenWhenInterrupted()
    {
        ExecutorService executor = Executors.newSingleThreadExecutor( daemon( "InterruptThread" ) );
        try
        {
            CompletableFuture<String> future = new CompletableFuture<>();

            Thread.currentThread().interrupt();
            executor.submit( () ->
            {
                sleep( 1_000 );
                future.complete( "Hello" );
            } );

            assertEquals( "Hello", Futures.blockingGet( future ) );
            assertTrue( Thread.currentThread().isInterrupted() );
        }
        finally
        {
            Thread.interrupted(); // clear interruption status
            executor.shutdown();
        }
    }

    @Test
    void shouldHandleInterruptsInBlockingGet()
    {
        try
        {
            CompletableFuture<String> future = new CompletableFuture<>();
            Thread.currentThread().interrupt();

            Runnable interruptHandler = () -> future.complete( "Hello" );
            assertEquals( "Hello", Futures.blockingGet( future, interruptHandler ) );
            assertTrue( Thread.currentThread().isInterrupted() );
        }
        finally
        {
            Thread.interrupted(); // clear interruption status
        }
    }

    @Test
    void shouldGetNowWhenFutureDone()
    {
        CompletableFuture<String> future = new CompletableFuture<>();
        future.complete( "Hello" );

        assertEquals( "Hello", Futures.getNow( future ) );
    }

    @Test
    void shouldGetNowWhenFutureNotDone()
    {
        CompletableFuture<String> future = new CompletableFuture<>();

        assertNull( Futures.getNow( future ) );
    }

    @Test
    void shouldGetCauseFromCompletionException()
    {
        RuntimeException error = new RuntimeException( "Hello" );
        CompletionException completionException = new CompletionException( error );

        assertEquals( error, Futures.completionExceptionCause( completionException ) );
    }

    @Test
    void shouldReturnSameExceptionWhenItIsNotCompletionException()
    {
        RuntimeException error = new RuntimeException( "Hello" );

        assertEquals( error, Futures.completionExceptionCause( error ) );
    }

    @Test
    void shouldWrapWithCompletionException()
    {
        RuntimeException error = new RuntimeException( "Hello" );
        CompletionException completionException = Futures.asCompletionException( error );
        assertEquals( error, completionException.getCause() );
    }

    @Test
    void shouldKeepCompletionExceptionAsIs()
    {
        CompletionException error = new CompletionException( new RuntimeException( "Hello" ) );
        assertEquals( error, Futures.asCompletionException( error ) );
    }

    @Test
    void shouldCombineTwoErrors()
    {
        RuntimeException error1 = new RuntimeException( "Error1" );
        RuntimeException error2Cause = new RuntimeException( "Error2" );
        CompletionException error2 = new CompletionException( error2Cause );

        CompletionException combined = Futures.combineErrors( error1, error2 );

        assertEquals( error1, combined.getCause() );
        assertArrayEquals( new Throwable[]{error2Cause}, combined.getCause().getSuppressed() );
    }

    @Test
    void shouldCombineErrorAndNull()
    {
        RuntimeException error1 = new RuntimeException( "Error1" );

        CompletionException combined = Futures.combineErrors( error1, null );

        assertEquals( error1, combined.getCause() );
    }

    @Test
    void shouldCombineNullAndError()
    {
        RuntimeException error2 = new RuntimeException( "Error2" );

        CompletionException combined = Futures.combineErrors( null, error2 );

        assertEquals( error2, combined.getCause() );
    }

    @Test
    void shouldCombineNullAndNullErrors()
    {
        assertNull( Futures.combineErrors( null, null ) );
    }
}
