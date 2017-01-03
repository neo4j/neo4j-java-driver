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
package org.neo4j.driver.internal.net.pooling;

import java.util.Map;

import org.neo4j.driver.internal.exceptions.InternalException;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.spi.Collector;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.PooledConnection;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.Consumer;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.summary.ServerInfo;

/**
 * The state of a pooledConnection from a pool point of view could be one of the following:
 * Created,
 * Available,
 * Claimed,
 * Closed,
 * Disposed.
 *
 * The state machine looks like:
 *
 *                      session.finalize
 *                       session.close     failed return to pool
 * Created -------> Claimed  ----------> Closed ---------> Disposed
 *                    ^                    |                    ^
 *      pool.acquire  |                    |returned to pool    |
 *                    |                    |                    |
 *                    ---- Available <-----                     |
 *                              |           pool.close          |
 *                              ---------------------------------
 */
public class PooledSocketConnection implements PooledConnection
{
    /** The real connection who will do all the real jobs */
    private final Connection delegate;
    private final Consumer<PooledConnection> release;

    private boolean unrecoverableErrorsOccurred = false;
    private Runnable onError = null;
    private final Clock clock;
    private long lastUsedTimestamp;

    public PooledSocketConnection( Connection delegate, Consumer<PooledConnection> release, Clock clock )
    {
        this.delegate = delegate;
        this.release = release;
        this.clock = clock;
        updateLastUsedTimestamp();
    }

    @Override
    public void init( String clientName, Map<String,Value> authToken )
    {
        try
        {
            delegate.init( clientName, authToken );
        }
        catch ( InternalException e )
        {
            onDelegateException( e );
        }
        catch( RuntimeException e )
        {
            onDelegateException( e );
        }
    }

    @Override
    public void run( String statement, Map<String,Value> parameters,
            Collector collector )
    {
        try
        {
            delegate.run( statement, parameters, collector );
        }
        catch ( InternalException e )
        {
            onDelegateException( e );
        }
        catch( RuntimeException e )
        {
            onDelegateException( e );
        }
    }

    @Override
    public void discardAll( Collector collector )
    {
        try
        {
            delegate.discardAll( collector );
        }
        catch ( InternalException e )
        {
            onDelegateException( e );
        }
        catch( RuntimeException e )
        {
            onDelegateException( e );
        }
    }

    @Override
    public void pullAll( Collector collector )
    {
        try
        {
            delegate.pullAll( collector );
        }
        catch ( InternalException e )
        {
            onDelegateException( e );
        }
        catch ( RuntimeException e )
        {
            onDelegateException( e );
        }
    }

    @Override
    public void reset()
    {
        try
        {
            delegate.reset();
        }
        catch ( InternalException e )
        {
            onDelegateException( e );
        }
        catch ( RuntimeException e )
        {
            onDelegateException( e );
        }
    }

    @Override
    public void ackFailure()
    {
        try
        {
            delegate.ackFailure();
        }
        catch ( InternalException e )
        {
            onDelegateException( e );
        }
        catch ( RuntimeException e )
        {
            onDelegateException( e );
        }
    }

    @Override
    public void sync()
    {
        try
        {
            delegate.sync();
        }
        catch ( InternalException e )
        {
            onDelegateException( e );
        }
        catch ( RuntimeException e )
        {
            onDelegateException( e );
        }
    }

    @Override
    public void flush()
    {
        try
        {
            delegate.flush();
        }
        catch ( InternalException e )
        {
            onDelegateException( e );
        }
        catch ( RuntimeException e )
        {
            onDelegateException( e );
        }
    }

    @Override
    public void receiveOne()
    {
        try
        {
            delegate.receiveOne();
        }
        catch ( InternalException e )
        {
            onDelegateException( e );
        }
        catch ( RuntimeException e )
        {
            onDelegateException( e );
        }
    }

    /**
     * Make sure only close the connection once on each session to avoid releasing the connection twice, a.k.a.
     * adding back the connection twice into the pool.
     */
    @Override
    public void close()
    {
        updateLastUsedTimestamp();
        release.accept( this );
        // put the full logic of deciding whether to dispose the connection or to put it back to
        // the pool into the release object
    }

    @Override
    public boolean isOpen()
    {
        return delegate.isOpen();
    }

    public boolean hasUnrecoverableErrors()
    {
        return unrecoverableErrorsOccurred;
    }

    @Override
    public void resetAsync()
    {
        try
        {
            delegate.resetAsync();
        }
        catch ( InternalException e )
        {
            onDelegateException( e );
        }
        catch( RuntimeException e )
        {
            onDelegateException( e );
        }
    }

    @Override
    public ServerInfo server()
    {
        return delegate.server();
    }

    @Override
    public BoltServerAddress boltServerAddress()
    {
        return delegate.boltServerAddress();
    }

    @Override
    public Logger logger()
    {
        return delegate.logger();
    }

    @Override
    public void dispose()
    {
        delegate.close();
    }

    /**
     * All runtime errors are unrecoverable errors.
     * TODO this method will eventually go away after all internal runtime errors are converted into checked errors
     * @param e
     */
    private void onDelegateException( RuntimeException e )
    {
        unrecoverableErrorsOccurred = true;
        onError( e );
    }
    /**
     * If something goes wrong with the delegate, we want to figure out if this "wrong" is something that means
     * the connection cannot be reused (and thus should be evicted from the pool), or if it's something that we can
     * safely recover from.
     * @param e the exception the delegate threw
     */
    private void onDelegateException( InternalException e )
    {
        if ( e.isUnrecoverableError() )
        {
            unrecoverableErrorsOccurred = true;
        }
        else
        {
            ackFailure();
        }
        // cast all checked error back to runtime errors
        onError( e.publicException() );
    }

    private void onError( RuntimeException e )
    {
        if( onError != null )
        {
            onError.run();
        }
        throw e;
    }

    @Override
    public void onError( Runnable runnable )
    {
        this.onError = runnable;
    }

    public long lastUsedTimestamp()
    {
        return lastUsedTimestamp;
    }

    private void updateLastUsedTimestamp()
    {
        this.lastUsedTimestamp = clock.millis();
    }
}
