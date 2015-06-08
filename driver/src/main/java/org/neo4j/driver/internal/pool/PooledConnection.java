/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.driver.internal.pool;

import java.util.Map;

import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.StreamCollector;
import org.neo4j.driver.internal.util.Consumer;

public class PooledConnection implements Connection
{
    private Connection delegate;
    private Consumer<PooledConnection> release;
    private boolean unrecoverableErrorsOccurred = false;

    public PooledConnection( Connection delegate, Consumer<PooledConnection> release )
    {
        this.delegate = delegate;
        this.release = release;
    }

    @Override
    public void initialize( String clientName )
    {
        try
        {
            delegate.initialize( clientName );
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
        if(!isClientOrTransientError( e ) )
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
