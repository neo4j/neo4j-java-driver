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

import static java.util.concurrent.CompletableFuture.completedFuture;

public final class Futures
{
    private Futures()
    {
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

    public static <V> V getBlocking( CompletionStage<V> stage )
    {
        Future<V> future = stage.toCompletableFuture();
        return getBlocking( future );
    }

    public static <V> V getBlocking( Future<V> future )
    {
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
                    interrupted = true;
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

    // todo: test all call sites
    public static Throwable completionErrorCause( Throwable error )
    {
        if ( error instanceof CompletionException )
        {
            return error.getCause();
        }
        return error;
    }

    public static <T> CompletionStage<T> firstNotNull( CompletionStage<T> stage1, CompletionStage<T> stage2 )
    {
        return stage1.thenCompose( value ->
        {
            if ( value != null )
            {
                return completedFuture( value );
            }
            return stage2;
        } );
    }
}
