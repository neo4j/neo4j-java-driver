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
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.driver.v1.Response;
import org.neo4j.driver.v1.util.Function;

public class InternalPromise<T> implements InternalFuture<T>, Promise<T>
{
    private final EventLoop eventLoop;
    private final Promise<T> delegate;

    public InternalPromise( Bootstrap bootstrap )
    {
        this( bootstrap.config().group().next() );
    }

    public InternalPromise( EventLoop eventLoop )
    {
        this.eventLoop = eventLoop;
        this.delegate = eventLoop.newPromise();
    }

    @Override
    public EventLoop eventLoop()
    {
        return eventLoop;
    }

    @Override
    public Response<T> asTask()
    {
        return new InternalResponse<>( this );
    }

    @Override
    public <U> InternalFuture<U> thenApply( Function<T,U> fn )
    {
        return Futures.thenApply( this, fn );
    }

    @Override
    public <U> InternalFuture<U> thenCombine( Function<T,InternalFuture<U>> fn )
    {
        return Futures.thenCombine( this, fn );
    }

    @Override
    public InternalFuture<T> whenComplete( Runnable action )
    {
        return Futures.whenComplete( this, action );
    }

    @Override
    public InternalPromise<T> setSuccess( T result )
    {
        delegate.setSuccess( result );
        return this;
    }

    @Override
    public boolean trySuccess( T result )
    {
        return delegate.trySuccess( result );
    }

    @Override
    public InternalPromise<T> setFailure( Throwable cause )
    {
        delegate.setFailure( cause );
        return this;
    }

    @Override
    public boolean tryFailure( Throwable cause )
    {
        return delegate.tryFailure( cause );
    }

    @Override
    public boolean setUncancellable()
    {
        return delegate.setUncancellable();
    }

    @Override
    public InternalPromise<T> addListener( GenericFutureListener<? extends Future<? super T>> listener )
    {
        delegate.addListener( listener );
        return this;
    }

    @Override
    public InternalPromise<T> addListeners( GenericFutureListener<? extends Future<? super T>>... listeners )
    {
        delegate.addListeners( listeners );
        return this;
    }

    @Override
    public InternalPromise<T> removeListener( GenericFutureListener<? extends Future<? super T>> listener )
    {
        delegate.removeListener( listener );
        return this;
    }

    @Override
    public InternalPromise<T> removeListeners( GenericFutureListener<? extends Future<? super T>>... listeners )
    {
        delegate.removeListeners( listeners );
        return this;
    }

    @Override
    public InternalPromise<T> await() throws InterruptedException
    {
        delegate.await();
        return this;
    }

    @Override
    public InternalPromise<T> awaitUninterruptibly()
    {
        delegate.awaitUninterruptibly();
        return this;
    }

    @Override
    public InternalPromise<T> sync() throws InterruptedException
    {
        delegate.sync();
        return this;
    }

    @Override
    public InternalPromise<T> syncUninterruptibly()
    {
        delegate.syncUninterruptibly();
        return this;
    }

    @Override
    public boolean isSuccess()
    {
        return delegate.isSuccess();
    }

    @Override
    public boolean isCancellable()
    {
        return delegate.isCancellable();
    }

    @Override
    public Throwable cause()
    {
        return delegate.cause();
    }

    @Override
    public boolean await( long timeout, TimeUnit unit ) throws InterruptedException
    {
        return delegate.await( timeout, unit );
    }

    @Override
    public boolean await( long timeoutMillis ) throws InterruptedException
    {
        return delegate.await( timeoutMillis );
    }

    @Override
    public boolean awaitUninterruptibly( long timeout, TimeUnit unit )
    {
        return delegate.awaitUninterruptibly( timeout, unit );
    }

    @Override
    public boolean awaitUninterruptibly( long timeoutMillis )
    {
        return delegate.awaitUninterruptibly( timeoutMillis );
    }

    @Override
    public T getNow()
    {
        return delegate.getNow();
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
