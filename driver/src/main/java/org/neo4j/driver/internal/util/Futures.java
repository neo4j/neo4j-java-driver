/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import io.netty.util.internal.PlatformDependent;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import org.neo4j.driver.internal.async.EventLoopGroupFactory;

import static java.util.concurrent.CompletableFuture.completedFuture;

public final class Futures
{
    private static final CompletableFuture<?> COMPLETED_WITH_NULL = completedFuture( null );
    private static final CompletableFuture<Boolean> COMPLETED_WITH_FALSE = completedFuture( false );

    private Futures()
    {
    }

    @SuppressWarnings( "unchecked" )
    public static <T> CompletableFuture<T> completedWithNull()
    {
        return (CompletableFuture) COMPLETED_WITH_NULL;
    }

    public static CompletableFuture<Boolean> completedWithFalse()
    {
        return COMPLETED_WITH_FALSE;
    }

    public static <T> CompletionStage<T> asCompletionStage( io.netty.util.concurrent.Future<T> future )
    {
        CompletableFuture<T> result = new CompletableFuture<>();
        if ( future.isCancelled() )
        {
            result.cancel( true );
        }
        else if ( future.isSuccess() )
        {
            result.complete( future.getNow() );
        }
        else if ( future.cause() != null )
        {
            result.completeExceptionally( future.cause() );
        }
        else
        {
            future.addListener( ignore ->
            {
                if ( future.isCancelled() )
                {
                    result.cancel( true );
                }
                else if ( future.isSuccess() )
                {
                    result.complete( future.getNow() );
                }
                else
                {
                    result.completeExceptionally( future.cause() );
                }
            } );
        }
        return result;
    }

    public static <T> CompletableFuture<T> failedFuture( Throwable error )
    {
        CompletableFuture<T> result = new CompletableFuture<>();
        result.completeExceptionally( error );
        return result;
    }

    public static <V> V blockingGet( CompletionStage<V> stage )
    {
        return blockingGet( stage, Futures::noOpInterruptHandler );
    }

    public static <V> V blockingGet( CompletionStage<V> stage, Runnable interruptHandler )
    {
        EventLoopGroupFactory.assertNotInEventLoopThread();

        Future<V> future = stage.toCompletableFuture();
        boolean interrupted = false;
        try
        {
            while ( true )
            {
                try
                {
                    return future.get();
                }
                catch ( InterruptedException e )
                {
                    // this thread was interrupted while waiting
                    // computation denoted by the future might still be running

                    interrupted = true;

                    // run the interrupt handler and ignore if it throws
                    // need to wait for IO thread to actually finish, can't simply re-rethrow
                    safeRun( interruptHandler );
                }
                catch ( ExecutionException e )
                {
                    PlatformDependent.throwException( e.getCause() );
                }
            }
        }
        finally
        {
            if ( interrupted )
            {
                Thread.currentThread().interrupt();
            }
        }
    }

    public static <T> T getNow( CompletionStage<T> stage )
    {
        return stage.toCompletableFuture().getNow( null );
    }

    /**
     * Helper method to extract cause of a {@link CompletionException}.
     * <p>
     * When using {@link CompletionStage#whenComplete(BiConsumer)} and {@link CompletionStage#handle(BiFunction)}
     * propagated exceptions might get wrapped in a {@link CompletionException}.
     *
     * @param error the exception to get cause for.
     * @return cause of the given exception if it is a {@link CompletionException}, given exception otherwise.
     */
    public static Throwable completionExceptionCause( Throwable error )
    {
        if ( error instanceof CompletionException )
        {
            return error.getCause();
        }
        return error;
    }

    /**
     * Helped method to turn given exception into a {@link CompletionException}.
     *
     * @param error the exception to convert.
     * @return given exception wrapped with {@link CompletionException} if it's not one already.
     */
    public static CompletionException asCompletionException( Throwable error )
    {
        if ( error instanceof CompletionException )
        {
            return ((CompletionException) error);
        }
        return new CompletionException( error );
    }

    private static void safeRun( Runnable runnable )
    {
        try
        {
            runnable.run();
        }
        catch ( Throwable ignore )
        {
        }
    }

    private static void noOpInterruptHandler()
    {
    }
}
