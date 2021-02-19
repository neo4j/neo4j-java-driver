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

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.neo4j.driver.internal.async.connection.EventLoopGroupFactory;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.neo4j.driver.internal.util.ErrorUtil.addSuppressed;

public final class Futures
{
    private static final CompletableFuture<?> COMPLETED_WITH_NULL = completedFuture( null );

    private Futures()
    {
    }

    @SuppressWarnings( "unchecked" )
    public static <T> CompletableFuture<T> completedWithNull()
    {
        return (CompletableFuture) COMPLETED_WITH_NULL;
    }

    public static <T> CompletableFuture<T> completeWithNullIfNoError( CompletableFuture<T> future, Throwable error )
    {
        if ( error != null )
        {
            future.completeExceptionally( error );
        }
        else
        {
            future.complete( null );
        }
        return future;
    }

    public static <T> CompletionStage<T> asCompletionStage( io.netty.util.concurrent.Future<T> future )
    {
        CompletableFuture<T> result = new CompletableFuture<>();
        return asCompletionStage( future, result );
    }

    public static <T> CompletionStage<T> asCompletionStage( io.netty.util.concurrent.Future<T> future, CompletableFuture<T> result )
    {
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
                    ErrorUtil.rethrowAsyncException( e );
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

    /**
     * Combine given errors into a single {@link CompletionException} to be rethrown from inside a
     * {@link CompletionStage} chain.
     *
     * @param error1 the first error or {@code null}.
     * @param error2 the second error or {@code null}.
     * @return {@code null} if both errors are null, {@link CompletionException} otherwise.
     */
    public static CompletionException combineErrors( Throwable error1, Throwable error2 )
    {
        if ( error1 != null && error2 != null )
        {
            Throwable cause1 = completionExceptionCause( error1 );
            Throwable cause2 = completionExceptionCause( error2 );
            addSuppressed( cause1, cause2 );
            return asCompletionException( cause1 );
        }
        else if ( error1 != null )
        {
            return asCompletionException( error1 );
        }
        else if ( error2 != null )
        {
            return asCompletionException( error2 );
        }
        else
        {
            return null;
        }
    }

    /**
     * Given a future, if the future completes successfully then return a new completed future with the completed value.
     * Otherwise if the future completes with an error, then this method first saves the error in the error recorder, and then continues with the onErrorAction.
     * @param future the future.
     * @param errorRecorder saves error if the given future completes with an error.
     * @param onErrorAction continues the future with this action if the future completes with an error.
     * @param <T> type
     * @return a new completed future with the same completed value if the given future completes successfully, otherwise continues with the onErrorAction.
     */
    @SuppressWarnings( "ThrowableNotThrown" )
    public static <T> CompletableFuture<T> onErrorContinue( CompletableFuture<T> future, Throwable errorRecorder,
            Function<Throwable,? extends CompletionStage<T>> onErrorAction )
    {
        Objects.requireNonNull( future );
        return future.handle( ( value, error ) -> {
            if ( error != null )
            {
                // record error
                Futures.combineErrors( errorRecorder, error );
                return new CompletionResult<T>( null, error );
            }
            return new CompletionResult<>( value, null );
        } ).thenCompose( result -> {
            if ( result.value != null )
            {
                return completedFuture( result.value );
            }
            else
            {
                return onErrorAction.apply( result.error );
            }
        } );
    }

    private static class CompletionResult<T>
    {
        T value;
        Throwable error;

        CompletionResult( T value, Throwable error )
        {
            this.value = value;
            this.error = error;
        }
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
