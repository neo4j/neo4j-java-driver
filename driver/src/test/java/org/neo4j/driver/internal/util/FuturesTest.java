/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.neo4j.driver.internal.async.EventLoopGroupFactory;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.driver.internal.util.Matchers.blockingOperationInEventLoopError;
import static org.neo4j.driver.v1.util.DaemonThreadFactory.daemon;
import static org.neo4j.driver.v1.util.TestUtil.sleep;

public class FuturesTest
{
    @Test
    public void shouldConvertCanceledNettyFutureToCompletionStage() throws Exception
    {
        DefaultPromise<String> promise = new DefaultPromise<>( ImmediateEventExecutor.INSTANCE );
        promise.cancel( true );

        CompletableFuture<String> future = Futures.asCompletionStage( promise ).toCompletableFuture();

        assertTrue( future.isCancelled() );
        assertTrue( future.isCompletedExceptionally() );
        try
        {
            future.get();
            fail( "Exception expected" );
        }
        catch ( CancellationException ignore )
        {
            // expected
        }
    }

    @Test
    public void shouldConvertSucceededNettyFutureToCompletionStage() throws Exception
    {
        SucceededFuture<String> nettyFuture = new SucceededFuture<>( ImmediateEventExecutor.INSTANCE, "Hello" );

        CompletableFuture<String> future = Futures.asCompletionStage( nettyFuture ).toCompletableFuture();

        assertTrue( future.isDone() );
        assertFalse( future.isCompletedExceptionally() );
        assertEquals( "Hello", future.get() );
    }

    @Test
    public void shouldConvertFailedNettyFutureToCompletionStage() throws Exception
    {
        RuntimeException error = new RuntimeException( "Hello" );
        FailedFuture<Object> nettyFuture = new FailedFuture<>( ImmediateEventExecutor.INSTANCE, error );

        CompletableFuture<Object> future = Futures.asCompletionStage( nettyFuture ).toCompletableFuture();

        assertTrue( future.isCompletedExceptionally() );
        try
        {
            future.get();
            fail( "Exception expected" );
        }
        catch ( ExecutionException e )
        {
            assertEquals( error, e.getCause() );
        }
    }

    @Test
    public void shouldConvertRunningNettyFutureToCompletionStageWhenFutureCanceled() throws Exception
    {
        DefaultPromise<String> promise = new DefaultPromise<>( ImmediateEventExecutor.INSTANCE );

        CompletableFuture<String> future = Futures.asCompletionStage( promise ).toCompletableFuture();

        assertFalse( future.isDone() );
        promise.cancel( true );

        assertTrue( future.isCancelled() );
        assertTrue( future.isCompletedExceptionally() );
        try
        {
            future.get();
            fail( "Exception expected" );
        }
        catch ( CancellationException ignore )
        {
            // expected
        }
    }

    @Test
    public void shouldConvertRunningNettyFutureToCompletionStageWhenFutureSucceeded() throws Exception
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
    public void shouldConvertRunningNettyFutureToCompletionStageWhenFutureFailed() throws Exception
    {
        RuntimeException error = new RuntimeException( "Hello" );
        DefaultPromise<String> promise = new DefaultPromise<>( ImmediateEventExecutor.INSTANCE );

        CompletableFuture<String> future = Futures.asCompletionStage( promise ).toCompletableFuture();

        assertFalse( future.isDone() );
        promise.setFailure( error );

        assertTrue( future.isCompletedExceptionally() );
        try
        {
            future.get();
            fail( "Exception expected" );
        }
        catch ( ExecutionException e )
        {
            assertEquals( error, e.getCause() );
        }
    }

    @Test
    public void shouldCreateFailedFutureWithUncheckedException() throws Exception
    {
        RuntimeException error = new RuntimeException( "Hello" );
        CompletableFuture<Object> future = Futures.failedFuture( error ).toCompletableFuture();
        assertTrue( future.isCompletedExceptionally() );
        try
        {
            future.get();
            fail( "Exception expected" );
        }
        catch ( ExecutionException e )
        {
            assertEquals( error, e.getCause() );
        }
    }

    @Test
    public void shouldCreateFailedFutureWithCheckedException() throws Exception
    {
        IOException error = new IOException( "Hello" );
        CompletableFuture<Object> future = Futures.failedFuture( error ).toCompletableFuture();
        assertTrue( future.isCompletedExceptionally() );
        try
        {
            future.get();
            fail( "Exception expected" );
        }
        catch ( ExecutionException e )
        {
            assertEquals( error, e.getCause() );
        }
    }

    @Test
    public void shouldFailBlockingGetInEventLoopThread() throws Exception
    {
        EventLoopGroup eventExecutor = EventLoopGroupFactory.newEventLoopGroup( 1 );
        try
        {
            CompletableFuture<String> future = new CompletableFuture<>();
            Future<String> result = eventExecutor.submit( () -> Futures.blockingGet( future ) );

            try
            {
                result.get();
                fail( "Exception expected" );
            }
            catch ( ExecutionException e )
            {
                assertThat( e.getCause(), is( blockingOperationInEventLoopError() ) );
            }
        }
        finally
        {
            eventExecutor.shutdownGracefully();
        }
    }

    @Test
    public void shouldThrowInBlockingGetWhenFutureThrowsUncheckedException()
    {
        RuntimeException error = new RuntimeException( "Hello" );

        CompletableFuture<String> future = new CompletableFuture<>();
        future.completeExceptionally( error );

        try
        {
            Futures.blockingGet( future );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertEquals( error, e );
        }
    }

    @Test
    public void shouldThrowInBlockingGetWhenFutureThrowsCheckedException()
    {
        IOException error = new IOException( "Hello" );

        CompletableFuture<String> future = new CompletableFuture<>();
        future.completeExceptionally( error );

        try
        {
            Futures.blockingGet( future );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertEquals( error, e );
        }
    }

    @Test
    public void shouldReturnFromBlockingGetWhenFutureCompletes()
    {
        CompletableFuture<String> future = new CompletableFuture<>();
        future.complete( "Hello" );

        assertEquals( "Hello", Futures.blockingGet( future ) );
    }

    @Test
    public void shouldWaitForFutureInBlockingGetEvenWhenInterrupted()
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
    public void shouldHandleInterruptsInBlockingGet()
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
    public void shouldGetNowWhenFutureDone()
    {
        CompletableFuture<String> future = new CompletableFuture<>();
        future.complete( "Hello" );

        assertEquals( "Hello", Futures.getNow( future ) );
    }

    @Test
    public void shouldGetNowWhenFutureNotDone()
    {
        CompletableFuture<String> future = new CompletableFuture<>();

        assertNull( Futures.getNow( future ) );
    }

    @Test
    public void shouldGetCauseFromCompletionException()
    {
        RuntimeException error = new RuntimeException( "Hello" );
        CompletionException completionException = new CompletionException( error );

        assertEquals( error, Futures.completionExceptionCause( completionException ) );
    }

    @Test
    public void shouldReturnSameExceptionWhenItIsNotCompletionException()
    {
        RuntimeException error = new RuntimeException( "Hello" );

        assertEquals( error, Futures.completionExceptionCause( error ) );
    }

    @Test
    public void shouldWrapWithCompletionException()
    {
        RuntimeException error = new RuntimeException( "Hello" );
        CompletionException completionException = Futures.asCompletionException( error );
        assertEquals( error, completionException.getCause() );
    }

    @Test
    public void shouldKeepCompletionExceptionAsIs()
    {
        CompletionException error = new CompletionException( new RuntimeException( "Hello" ) );
        assertEquals( error, Futures.asCompletionException( error ) );
    }

    @Test
    public void shouldCombineTwoErrors()
    {
        RuntimeException error1 = new RuntimeException( "Error1" );
        RuntimeException error2Cause = new RuntimeException( "Error2" );
        CompletionException error2 = new CompletionException( error2Cause );

        CompletionException combined = Futures.combineErrors( error1, error2 );

        assertEquals( error1, combined.getCause() );
        assertArrayEquals( new Throwable[]{error2Cause}, combined.getCause().getSuppressed() );
    }

    @Test
    public void shouldCombineErrorAndNull()
    {
        RuntimeException error1 = new RuntimeException( "Error1" );

        CompletionException combined = Futures.combineErrors( error1, null );

        assertEquals( error1, combined.getCause() );
    }

    @Test
    public void shouldCombineNullAndError()
    {
        RuntimeException error2 = new RuntimeException( "Error2" );

        CompletionException combined = Futures.combineErrors( null, error2 );

        assertEquals( error2, combined.getCause() );
    }

    @Test
    public void shouldCombineNullAndNullErrors()
    {
        assertNull( Futures.combineErrors( null, null ) );
    }
}
