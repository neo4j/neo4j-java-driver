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
package org.neo4j.driver.internal.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;

import org.neo4j.driver.v1.util.Function;

public final class Futures
{
    private Futures()
    {
    }

    public static <T, U> EventLoopAwareFuture<U> transform( EventLoopAwareFuture<T> future, Function<T,U> transformer )
    {
        EventLoopAwarePromise<U> result = new EventLoopAwarePromise<>( future.eventLoop() );
        future.addListener( new TransformListener<>( result, transformer ) );
        return result;
    }

    public static <T, U> EventLoopAwareFuture<U> transform( Future<T> future, Bootstrap bootstrap,
            Function<T,U> transformer )
    {
        EventLoopAwarePromise<U> result = new EventLoopAwarePromise<>( bootstrap );
        future.addListener( new TransformListener<>( result, transformer ) );
        return result;
    }

    public static <T> EventLoopAwareFuture<T> unwrap( EventLoopAwareFuture<EventLoopAwareFuture<T>> future )
    {
        final EventLoopAwarePromise<T> result = new EventLoopAwarePromise<>( future.eventLoop() );
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
}
