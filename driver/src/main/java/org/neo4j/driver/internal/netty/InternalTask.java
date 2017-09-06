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

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class InternalTask<T> implements Task<T>
{
    private final Future<T> delegate;

    public InternalTask( Future<T> delegate )
    {
        this.delegate = Objects.requireNonNull( delegate );
    }

    @Override
    public void addListener( final TaskListener<T> listener )
    {
        delegate.addListener( new FutureListener<T>()
        {
            @Override
            public void operationComplete( Future<T> future )
            {
                if ( future.isSuccess() )
                {
                    listener.taskCompleted( future.getNow(), null );
                }
                else
                {
                    listener.taskCompleted( null, future.cause() );
                }
            }
        } );
    }

    @Override
    public boolean cancel( boolean mayInterruptIfRunning )
    {
        return delegate.cancel( mayInterruptIfRunning );
    }

    @Override
    public boolean isCancelled()
    {
        return delegate.isCancelled();
    }

    @Override
    public boolean isDone()
    {
        return delegate.isDone();
    }

    @Override
    public T get() throws InterruptedException, ExecutionException
    {
        return delegate.get();
    }

    @Override
    public T get( long timeout, TimeUnit unit ) throws InterruptedException, ExecutionException, TimeoutException
    {
        return delegate.get( timeout, unit );
    }
}
