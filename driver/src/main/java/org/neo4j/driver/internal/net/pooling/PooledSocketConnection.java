/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.driver.internal.exceptions.BoltProtocolException;
import org.neo4j.driver.internal.exceptions.ConnectionException;
import org.neo4j.driver.internal.exceptions.InternalException;
import org.neo4j.driver.internal.exceptions.InvalidOperationException;
import org.neo4j.driver.internal.exceptions.ServerNeo4jException;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.spi.Collector;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.PooledConnection;
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
    private final PooledConnectionReleaser release;

    private boolean unrecoverableErrorsOccurred = false;
    private Runnable onError = null;

    public PooledSocketConnection( Connection delegate, PooledConnectionReleaser release )
    {
        this.delegate = delegate;
        this.release = release;
    }

    @Override
    public void init( String clientName, Map<String,Value> authToken )
            throws ConnectionException, ServerNeo4jException, InvalidOperationException, BoltProtocolException
    {
        try
        {
            delegate.init( clientName, authToken );
        }
        catch( ConnectionException e )
        {
            onDelegateException( e );
        }
        catch( ServerNeo4jException e )
        {
            onDelegateException( e );
        }
        catch( InvalidOperationException e )
        {
            onDelegateException( e );
        }
        catch( BoltProtocolException e )
        {
            onDelegateException( e );
        }
    }

    @Override
    public void run( String statement, Map<String,Value> parameters, Collector collector )
            throws InvalidOperationException, BoltProtocolException
    {
        try
        {
            delegate.run( statement, parameters, collector );
        }
        catch( InvalidOperationException e )
        {
            onDelegateException( e );
        }
        catch( BoltProtocolException e )
        {
            onDelegateException( e );
        }
    }

    @Override
    public void discardAll( Collector collector ) throws InvalidOperationException, BoltProtocolException
    {
        try
        {
            delegate.discardAll( collector );
        }
        catch ( InvalidOperationException e )
        {
            onDelegateException( e );
        }
        catch ( BoltProtocolException e )
        {
            onDelegateException( e );
        }
    }

    @Override
    public void pullAll( Collector collector ) throws InvalidOperationException, BoltProtocolException
    {
        try
        {
            delegate.pullAll( collector );
        }
        catch ( InvalidOperationException e )
        {
            onDelegateException( e );
        }
        catch ( BoltProtocolException e )
        {
            onDelegateException( e );
        }
    }

    @Override
    public void reset() throws InvalidOperationException, BoltProtocolException
    {
        try
        {
            delegate.reset();
        }
        catch ( InvalidOperationException e )
        {
            onDelegateException( e );
        }
        catch ( BoltProtocolException e )
        {
            onDelegateException( e );
        }
    }

    @Override
    public void ackFailure()
    {
        delegate.ackFailure();
    }

    @Override
    public void sync()
            throws ConnectionException, ServerNeo4jException, InvalidOperationException, BoltProtocolException
    {
        try
        {
            delegate.sync();
        }
        catch ( ConnectionException e )
        {
            onDelegateException( e );
        }
        catch ( ServerNeo4jException e )
        {
            onDelegateException( e );
        }
        catch ( InvalidOperationException e )
        {
            onDelegateException( e );
        }
        catch ( BoltProtocolException e )
        {
            onDelegateException( e );
        }
    }

    @Override
    public void flush() throws ConnectionException, InvalidOperationException, BoltProtocolException
    {
        try
        {
            delegate.flush();
        }
        catch ( ConnectionException e )
        {
            onDelegateException( e );
        }
        catch ( InvalidOperationException e )
        {
            onDelegateException( e );
        }
        catch ( BoltProtocolException e )
        {
            onDelegateException( e );
        }
    }

    @Override
    public void receiveOne() throws ConnectionException, ServerNeo4jException, BoltProtocolException
    {
        try
        {
            delegate.receiveOne();
        }
        catch ( ConnectionException e )
        {
            onDelegateException( e );
        }
        catch ( ServerNeo4jException e )
        {
            onDelegateException( e );
        }
        catch ( BoltProtocolException e )
        {
            onDelegateException( e );
        }
    }

    @Override
    /**
     * Make sure only close the connection once on each session to avoid releasing the connection twice, a.k.a.
     * adding back the connection twice into the pool.
     */
    public void close() throws InvalidOperationException
    {
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
    public void resetAsync() throws ConnectionException, InvalidOperationException, BoltProtocolException
    {
        try
        {
            delegate.resetAsync();
        }
        catch ( ConnectionException e )
        {
            onDelegateException( e );
        }
        catch ( InvalidOperationException e )
        {
            onDelegateException( e );
        }
        catch ( BoltProtocolException e )
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
    public void dispose() throws InvalidOperationException
    {
        delegate.close();
    }

    /**
     * * If something goes wrong with the delegate, we want to figure out if this "wrong" is something that means
     * the connection cannot be reused (and thus should be evicted from the pool), or if it's something that we can
     * safely recover from.
     * @param e the exception the delegate threw
     * @param <T> the type of the exception that is going to rethrow
     * @throws T the type of the exception that is going to rethrow
     */
    private <T extends InternalException> void onDelegateException( T e ) throws T
    {
        if( e.isUnrecoverableError() )
        {
            unrecoverableErrorsOccurred = true;
        }
        else
        {
            ackFailure();
        }
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
}
