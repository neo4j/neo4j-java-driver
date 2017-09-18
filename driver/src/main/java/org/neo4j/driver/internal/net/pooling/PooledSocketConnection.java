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

import org.neo4j.driver.internal.SessionResourcesHandler;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.PooledConnection;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.Consumer;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.summary.ServerInfo;

import static org.neo4j.driver.internal.util.ErrorUtil.isRecoverable;

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
    private SessionResourcesHandler resourcesHandler;
    private final Clock clock;

    private final long creationTimestamp;
    private long lastUsedTimestamp;

    public PooledSocketConnection( Connection delegate, Consumer<PooledConnection> release, Clock clock )
    {
        this.delegate = delegate;
        this.release = release;
        this.clock = clock;
        this.creationTimestamp = clock.millis();
        updateLastUsedTimestamp();
    }

    @Override
    public void init( String clientName, Map<String,Value> authToken )
    {
        try
        {
            delegate.init( clientName, authToken );
        }
        catch( RuntimeException e )
        {
            onDelegateException( e );
        }
    }

    @Override
    public void run( String statement, Map<String,Value> parameters, ResponseHandler handler )
    {
        try
        {
            delegate.run( statement, parameters, handler );
        }
        catch(RuntimeException e)
        {
            onDelegateException( e );
        }
    }

    @Override
    public void discardAll( ResponseHandler handler )
    {
        try
        {
            delegate.discardAll( handler );
        }
        catch ( RuntimeException e )
        {
            onDelegateException( e );
        }
    }

    @Override
    public void pullAll( ResponseHandler handler )
    {
        try
        {
            delegate.pullAll( handler );
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
        resourcesHandler = null;
        release.accept( this );
        // put the full logic of deciding whether to dispose the connection or to put it back to
        // the pool into the release object
    }

    @Override
    public boolean isOpen()
    {
        return delegate.isOpen();
    }

    @Override
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
        catch( RuntimeException e )
        {
            onDelegateException( e );
        }
    }

    @Override
    public boolean isAckFailureMuted()
    {
        return delegate.isAckFailureMuted();
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
    public void dispose()
    {
        delegate.close();
    }

    /**
     * If something goes wrong with the delegate, we want to figure out if this "wrong" is something that means
     * the connection cannot be reused (and thus should be evicted from the pool), or if it's something that we can
     * safely recover from.
     * @param e the exception the delegate threw
     */
    private void onDelegateException( RuntimeException e )
    {
        if ( !isRecoverable( e ) )
        {
            unrecoverableErrorsOccurred = true;
        }
        else if ( !isAckFailureMuted() )
        {
            ackFailure();
        }
        if ( resourcesHandler != null )
        {
            resourcesHandler.onConnectionError( !unrecoverableErrorsOccurred );
        }
        throw e;
    }

    @Override
    public void setResourcesHandler( SessionResourcesHandler resourcesHandler )
    {
        this.resourcesHandler = resourcesHandler;
    }

    @Override
    public long creationTimestamp()
    {
        return creationTimestamp;
    }

    @Override
    public long lastUsedTimestamp()
    {
        return lastUsedTimestamp;
    }

    private void updateLastUsedTimestamp()
    {
        lastUsedTimestamp = clock.millis();
    }
}
