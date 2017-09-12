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
package org.neo4j.driver.internal.async;

import io.netty.bootstrap.Bootstrap;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;

import org.neo4j.driver.v1.util.Function;

final class Futures
{
    private Futures()
    {
    }

    static <T, U> InternalFuture<U> thenApply( InternalFuture<T> future, Function<T,U> fn )
    {
        InternalPromise<U> result = new InternalPromise<>( future.eventLoop() );
        future.addListener( new ThenApplyListener<>( result, fn ) );
        return result;
    }

    static <T, U> InternalFuture<U> thenApply( Future<T> future, Bootstrap bootstrap, Function<T,U> fn )
    {
        InternalPromise<U> result = new InternalPromise<>( bootstrap );
        future.addListener( new ThenApplyListener<>( result, fn ) );
        return result;
    }

    static <T, U> InternalFuture<U> thenCombine( InternalFuture<T> future, Function<T,InternalFuture<U>> fn )
    {
        InternalPromise<U> result = new InternalPromise<>( future.eventLoop() );
        future.addListener( new ThenCombineListener<>( result, fn ) );
        return result;
    }

    static <T> InternalFuture<T> whenComplete( InternalFuture<T> future, Runnable action )
    {
        InternalPromise<T> result = new InternalPromise<>( future.eventLoop() );
        future.addListener( new CompletionListener<>( result, action ) );
        return result;
    }

    private static class ThenApplyListener<T, U> implements GenericFutureListener<Future<T>>
    {
        final Promise<U> result;
        final Function<T,U> fn;

        ThenApplyListener( Promise<U> result, Function<T,U> fn )
        {
            this.result = result;
            this.fn = fn;
        }

        @Override
        public void operationComplete( Future<T> future ) throws Exception
        {
            if ( future.isCancelled() )
            {
                result.cancel( true );
            }
            else if ( future.isSuccess() )
            {
                try
                {
                    T originalValue = future.getNow();
                    U newValue = fn.apply( originalValue );
                    result.setSuccess( newValue );
                }
                catch ( Throwable t )
                {
                    result.setFailure( t );
                }
            }
            else
            {
                result.setFailure( future.cause() );
            }
        }
    }

    private static class ThenCombineListener<T, U> implements GenericFutureListener<Future<T>>
    {
        final Promise<U> result;
        final Function<T,InternalFuture<U>> fn;

        ThenCombineListener( Promise<U> result, Function<T,InternalFuture<U>> fn )
        {
            this.result = result;
            this.fn = fn;
        }

        @Override
        public void operationComplete( Future<T> future ) throws Exception
        {
            if ( future.isCancelled() )
            {
                result.cancel( true );
            }
            else if ( future.isSuccess() )
            {
                try
                {
                    T value = future.getNow();
                    InternalFuture<U> newFuture = fn.apply( value );
                    newFuture.addListener( new NestedThenCombineListener<>( result, newFuture ) );
                }
                catch ( Throwable t )
                {
                    result.setFailure( t );
                }
            }
            else
            {
                result.setFailure( future.cause() );
            }
        }
    }

    private static class NestedThenCombineListener<T> implements GenericFutureListener<Future<T>>
    {

        final Promise<T> result;
        final Future<T> future;

        NestedThenCombineListener( Promise<T> result, Future<T> future )
        {
            this.result = result;
            this.future = future;
        }

        @Override
        public void operationComplete( Future<T> future ) throws Exception
        {
            if ( future.isCancelled() )
            {
                result.cancel( true );
            }
            else if ( future.isSuccess() )
            {
                result.setSuccess( future.getNow() );
            }
            else
            {
                result.setFailure( future.cause() );
            }
        }
    }

    private static class CompletionListener<T> implements GenericFutureListener<Future<T>>
    {
        final Promise<T> result;
        final Runnable action;

        CompletionListener( Promise<T> result, Runnable action )
        {
            this.result = result;
            this.action = action;
        }

        @Override
        public void operationComplete( Future<T> future ) throws Exception
        {
            if ( future.isCancelled() )
            {
                result.cancel( true );
            }
            else if ( future.isSuccess() )
            {
                try
                {
                    action.run();
                    result.setSuccess( future.getNow() );
                }
                catch ( Throwable t )
                {
                    result.setFailure( t );
                }
            }
            else
            {
                Throwable error = future.cause();
                try
                {
                    action.run();
                }
                catch ( Throwable t )
                {
                    error.addSuppressed( t );
                }
                result.setFailure( error );
            }
        }
    }
}
