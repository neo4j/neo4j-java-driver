package org.neo4j.driver.internal.pool;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.driver.Transaction;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.StreamCollector;
import org.neo4j.driver.internal.util.Consumer;

import static java.util.Arrays.asList;

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
