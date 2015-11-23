/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.driver.v1.internal.pool;

import java.util.Map;

import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.Neo4jException;
import org.neo4j.driver.v1.internal.spi.Connection;
import org.neo4j.driver.v1.internal.spi.StreamCollector;
import org.neo4j.driver.v1.internal.util.Consumer;

public class PooledConnection implements Connection
{
    /** The real connection who will do all the real jobs */
    private Connection delegate;
    /** A reference to the {@link ThreadCachingPool pool} so that we could return this resource back */
    private Consumer<PooledConnection> release;
    private boolean unrecoverableErrorsOccurred = false;

    public PooledConnection( Connection delegate, Consumer<PooledConnection> release )
    {
        this.delegate = delegate;
        this.release = release;
    }

    @Override
    public void init( String clientName )
    {
        try
        {
            delegate.init( clientName );
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
        catch(RuntimeException e)
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
        catch(RuntimeException e)
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
        catch(RuntimeException e)
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
        catch(RuntimeException e)
        {
            onDelegateException( e );
        }
    }

    @Override
    public void close()
    {
        release.accept( this );
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
        if ( !isClientOrTransientError( e ) )
        {
            unrecoverableErrorsOccurred = true;
        }
        throw e;
    }

    private boolean isClientOrTransientError( RuntimeException e )
    {
        // Eg: DatabaseErrors and unknown (no status code or not neo4j exception) cause session to be discarded
        return e instanceof Neo4jException
               && (((Neo4jException) e).neo4jErrorCode().contains( "ClientError" )
                   || ((Neo4jException) e).neo4jErrorCode().contains( "TransientError" ));
    }
}
