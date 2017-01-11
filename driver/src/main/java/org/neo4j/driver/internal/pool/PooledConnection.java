/**
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
package org.neo4j.driver.internal.pool;

import java.util.Map;

import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.StreamCollector;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.Consumer;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.Neo4jException;

import static java.lang.String.format;

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
public class PooledConnection implements Connection
{
    /** The real connection who will do all the real jobs */
    private final Connection delegate;
    private final Consumer<PooledConnection> release;

    private boolean unrecoverableErrorsOccurred = false;
    private Runnable onError = null;
    private final Clock clock;
    private long lastUsed;

    public PooledConnection( Connection delegate, Consumer<PooledConnection> release, Clock clock )
    {
        this.delegate = delegate;
        this.release = release;
        this.clock = clock;
        this.lastUsed = clock.millis();
    }

    public void updateUsageTimestamp()
    {
        lastUsed = clock.millis();
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
    public void run( String statement, Map<String,Value> parameters,
            StreamCollector collector )
    {
        try
        {
            delegate.run( statement, parameters, collector );
        }
        catch( RuntimeException e )
        {
            onDelegateException( e );
        }
    }

    @Override
    public void discardAll()
    {
        try
        {
            delegate.discardAll();
        }
        catch( RuntimeException e )
        {
            onDelegateException( e );
        }
    }

    @Override
    public void pullAll( StreamCollector collector )
    {
        try
        {
            delegate.pullAll( collector );
        }
        catch( RuntimeException e )
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

    @Override
    /**
     * Make sure only close the connection once on each session to avoid releasing the connection twice, a.k.a.
     * adding back the connection twice into the pool.
     */
    public void close()
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

    public void dispose()
    {
        delegate.close();
    }

    /**
     * If something goes wrong with the delegate, we want to figure out if this "wrong" is something that means
     * the connection is screwed (and thus should be evicted from the pool), or if it's something that we can
     * safely recover from.
     * @param e the exception the delegate threw
     */
    private void onDelegateException( RuntimeException e )
    {
        if ( isUnrecoverableErrorsOccurred( e ) )
        {
            unrecoverableErrorsOccurred = true;
            if( isClusterError( e ) )
            {
                e = new ClientException( format(
                        "Failed to run a statement on a 3.1+ Neo4j core-edge cluster server. %n" +
                        "Please retry the statement if you have defined your own routing strategy to route driver messages to the cluster. " +
                        "Note: 1.0 java driver does not support routing statements to a 3.1+ Neo4j core edge cluster. " +
                        "Please upgrade to 1.1 driver and use `bolt+routing` scheme to route and run statements " +
                        "directly to a 3.1+ core edge cluster." ), e );
            }
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

    private boolean isUnrecoverableErrorsOccurred( RuntimeException e )
    {
        return !isClientOrTransientError( e ) || isProtocolViolationError( e ) || isClusterError( e );
    }

    private boolean isProtocolViolationError( RuntimeException e )
    {
        return ( e instanceof Neo4jException ) &&
                ( ( Neo4jException ) e ).neo4jErrorCode().startsWith( "Neo.ClientError.Request" );
    }

    private boolean isClientOrTransientError( RuntimeException e )
    {
        // Eg: DatabaseErrors and unknown (no status code or not neo4j exception) cause session to be discarded
        return ( e instanceof Neo4jException ) && ( ( Neo4jException ) e ).neo4jErrorCode().contains( "ClientError" )
                || ( e instanceof Neo4jException ) && ( ( Neo4jException ) e ).neo4jErrorCode().contains( "TransientError" );
    }

    private boolean isClusterError( RuntimeException e )
    {
        return ( e instanceof Neo4jException ) && ( ( Neo4jException ) e ).neo4jErrorCode().contains( "Cluster" );
    }


    @Override
    public String server()
    {
        return delegate.server();
    }

    public long idleTime()
    {
        long idleTime = clock.millis() - lastUsed;
        return idleTime;
    }
}
