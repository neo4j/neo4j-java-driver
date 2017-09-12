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

import org.neo4j.driver.internal.util.Consumer;
import org.neo4j.driver.v1.util.Function;

public final class Futures
{
    private Futures()
    {
    }

    public static <T> InternalPromise<T> completed( InternalPromise<T> promise, T value )
    {
        promise.setSuccess( value );
        return promise;
    }

    public static <T, U> InternalFuture<U> transform( InternalFuture<T> future, Function<T,U> transformer )
    {
        InternalPromise<U> result = new InternalPromise<>( future.eventLoop() );
        future.addListener( new TransformListener<>( result, transformer ) );
        return result;
    }

    public static <T, U> InternalFuture<U> transform( Future<T> future, Bootstrap bootstrap,
            Function<T,U> transformer )
    {
        InternalPromise<U> result = new InternalPromise<>( bootstrap );
        future.addListener( new TransformListener<>( result, transformer ) );
        return result;
    }

    public static <T> InternalFuture<T> unwrap( InternalFuture<InternalFuture<T>> future )
    {
        final InternalPromise<T> result = new InternalPromise<>( future.eventLoop() );
        future.addListener( new GenericFutureListener<Future<Future<T>>>()
        {
            @Override
            public void operationComplete( Future<Future<T>> future ) throws Exception
            {
                if ( future.isCancelled() )
                {
                    result.cancel( true );
                }
                else if ( future.isSuccess() )
                {
                    Future<T> nested = future.getNow();
                    nested.addListener( new GenericFutureListener<Future<T>>()
                    {
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
                    } );
                }
                else
                {
                    result.setFailure( future.cause() );
                }
            }
        } );
        return result;
    }

    public static <T> InternalFuture<T> fallback( InternalFuture<T> from, InternalFuture<T> to )
    {
        InternalPromise<T> result = new InternalPromise<>( from.eventLoop() );
        from.addListener( new FallbackListener<>( result, to ) );
        return result;
    }

    public static <T> InternalFuture<T> onSuccess( InternalFuture<T> future, Consumer<T> action )
    {
        InternalPromise<T> result = new InternalPromise<>( future.eventLoop() );
        future.addListener( new SuccessListener<>( result, action ) );
        return result;
    }

    public static <T> InternalFuture<T> onCompletion( InternalFuture<T> future, Consumer<Void> action )
    {
        InternalPromise<T> result = new InternalPromise<>( future.eventLoop() );
        future.addListener( new CompletionListener<>( result, action ) );
        return result;
    }

    private static class TransformListener<T, U> implements GenericFutureListener<Future<T>>
    {
        final Promise<U> result;
        final Function<T,U> transformer;

        TransformListener( Promise<U> result, Function<T,U> transformer )
        {
            this.result = result;
            this.transformer = transformer;
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
                    U transformedValue = transformer.apply( originalValue );
                    result.setSuccess( transformedValue );
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

    private static class FallbackListener<T> implements GenericFutureListener<Future<T>>
    {
        final Promise<T> result;
        final Future<T> fallback;

        FallbackListener( Promise<T> result, Future<T> fallback )
        {
            this.result = result;
            this.fallback = fallback;
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
                fallback.addListener( new NestedFallbackListener<>( result, future.cause() ) );
            }
        }
    }

    private static class NestedFallbackListener<T> implements GenericFutureListener<Future<T>>
    {
        final Promise<T> result;
        final Throwable originalError;

        NestedFallbackListener( Promise<T> result, Throwable originalError )
        {
            this.result = result;
            this.originalError = originalError;
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
                result.setFailure( originalError );
            }
        }
    }

    private static class SuccessListener<T> implements GenericFutureListener<Future<T>>
    {
        final Promise<T> result;
        final Consumer<T> action;

        SuccessListener( Promise<T> result, Consumer<T> action )
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
                    T value = future.getNow();
                    action.accept( value );
                    result.setSuccess( value );
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

    private static class CompletionListener<T> implements GenericFutureListener<Future<T>>
    {
        final Promise<T> result;
        final Consumer<Void> action;

        CompletionListener( Promise<T> result, Consumer<Void> action )
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
                    action.accept( null );
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
                    action.accept( null );
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
