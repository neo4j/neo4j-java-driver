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
package org.neo4j.driver.internal;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.driver.internal.logging.DevNullLogger;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.types.TypeSystem;

import static org.neo4j.driver.v1.Values.value;

public class NetworkSession implements Session
{
    protected Connection connection;
    private final String sessionId;
    private final Logger logger;

    private String lastBookmark = null;

    // Called when a transaction object is closed
    private final Runnable txCleanup = new Runnable()
    {
        @Override
        public void run()
        {
            synchronized ( NetworkSession.this )
            {
                if ( currentTransaction != null )
                {
                    lastBookmark = currentTransaction.bookmark();
                    currentTransaction = null;
                }
            }
        }
    };

    private ExplicitTransaction currentTransaction;
    private AtomicBoolean isOpen = new AtomicBoolean( true );

    public NetworkSession( Connection connection )
    {
        this.connection = connection;

        if( connection != null && connection.logger() != null )
        {
            this.logger = connection.logger();
        }
        else
        {
            this.logger = DevNullLogger.DEV_NULL_LOGGER;
        }
        sessionId = UUID.randomUUID().toString();
        this.logger.debug( "~~ connection claimed by [session-%s]", sessionId );
    }

    @Override
    public StatementResult run( String statementText )
    {
        return run( statementText, Values.EmptyMap );
    }

    @Override
    public StatementResult run( String statementText, Map<String,Object> statementParameters )
    {
        Value params = statementParameters == null ? Values.EmptyMap : value( statementParameters );
        return run( statementText, params );
    }

    @Override
    public StatementResult run( String statementTemplate, Record statementParameters )
    {
        Value params = statementParameters == null ? Values.EmptyMap : value( statementParameters.asMap() );
        return run( statementTemplate, params );
    }

    @Override
    public StatementResult run( String statementText, Value statementParameters )
    {
        return run( new Statement( statementText, statementParameters ) );
    }

    @Override
    public StatementResult run( Statement statement )
    {
        ensureConnectionIsValidBeforeRunningSession();
        return run( connection, statement );
    }

    public static StatementResult run( Connection connection, Statement statement )
    {
        InternalStatementResult cursor = new InternalStatementResult( connection, null, statement );
        connection.run( statement.text(), statement.parameters().asMap( Values.ofValue() ),
                cursor.runResponseCollector() );
        connection.pullAll( cursor.pullAllResponseCollector() );
        connection.flush();
        return cursor;
    }

    public synchronized void reset()
    {
        ensureSessionIsOpen();
        ensureNoUnrecoverableError();
        ensureConnectionIsOpen();

        if ( currentTransaction != null )
        {
            currentTransaction.markToClose();
            lastBookmark = currentTransaction.bookmark();
            currentTransaction = null;
        }
        connection.resetAsync();
    }

    @Override
    public boolean isOpen()
    {
        return isOpen.get() && connection.isOpen();
    }

    @Override
    public void close()
    {
        // Use atomic operation to protect from closing the connection twice (putting back to the pool twice).
        if ( !isOpen.compareAndSet( true, false ) )
        {
            throw new ClientException( "This session has already been closed." );
        }

        if ( !connection.isOpen() )
        {
            // the socket connection is already closed due to some error, cannot send more data
            closeConnection();
            return;
        }

        synchronized ( this )
        {
            if ( currentTransaction != null )
            {
                try
                {
                    currentTransaction.close();
                }
                catch ( Throwable e )
                {
                    // Best-effort
                    logger.warn( "WARNING: Failed to close tx due to error: " + e.toString() );
                }
            }
        }
        try
        {
            connection.sync();
        }
        finally
        {
            closeConnection();
        }
    }

    private void closeConnection()
    {
        logger.debug( "~~ connection released by [session-%s]", sessionId );
        connection.close();
    }

    @Override
    public Transaction beginTransaction()
    {
        return beginTransaction( null );
    }

    @Override
    public synchronized Transaction beginTransaction( String bookmark )
    {
        ensureConnectionIsValidBeforeOpeningTransaction();
        currentTransaction = new ExplicitTransaction( connection, txCleanup, bookmark );
        connection.onError( new Runnable()
        {
            @Override
            public void run()
            {
                // must check if transaction has been closed
                if ( currentTransaction != null )
                {
                    if ( connection.hasUnrecoverableErrors() )
                    {
                        currentTransaction.markToClose();
                    }
                    else
                    {
                        currentTransaction.failure();
                    }
                }
            }
        } );
        return currentTransaction;
    }

    @Override
    public String lastBookmark()
    {
        return lastBookmark;
    }

    @Override
    public TypeSystem typeSystem()
    {
        return InternalTypeSystem.TYPE_SYSTEM;
    }

    private void ensureConnectionIsValidBeforeRunningSession()
    {
        ensureSessionIsOpen();
        ensureNoUnrecoverableError();
        ensureNoOpenTransactionBeforeRunningSession();
        ensureConnectionIsOpen();
    }

    private void ensureConnectionIsValidBeforeOpeningTransaction()
    {
        ensureSessionIsOpen();
        ensureNoUnrecoverableError();
        ensureNoOpenTransactionBeforeOpeningTransaction();
        ensureConnectionIsOpen();
    }

    @Override
    protected void finalize() throws Throwable
    {
        if ( isOpen.compareAndSet( true, false ) )
        {
            logger.error( "Neo4j Session object leaked, please ensure that your application calls the `close` " +
                          "method on Sessions before disposing of the objects.", null );
            connection.close();
        }
        super.finalize();
    }

    private void ensureNoUnrecoverableError()
    {
        if ( connection.hasUnrecoverableErrors() )
        {
            throw new ClientException( "Cannot run more statements in the current session as an unrecoverable error " +
                                       "has happened. Please close the current session and re-run your statement in a" +
                                       " new session." );
        }
    }

    //should be called from a synchronized block
    private void ensureNoOpenTransactionBeforeRunningSession()
    {
        if ( currentTransaction != null )
        {
            throw new ClientException( "Statements cannot be run directly on a session with an open transaction;" +
                                       " either run from within the transaction or use a different session." );
        }
    }

    //should be called from a synchronized block
    private void ensureNoOpenTransactionBeforeOpeningTransaction()
    {
        if ( currentTransaction != null )
        {
            throw new ClientException( "You cannot begin a transaction on a session with an open transaction;" +
                                       " either run from within the transaction or use a different session." );
        }
    }

    private void ensureConnectionIsOpen()
    {
        if ( !connection.isOpen() )
        {
            throw new ServiceUnavailableException( "The current session cannot be reused as the underlying connection with the " +
                                                   "server has been closed due to unrecoverable errors. " +
                                                   "Please close this session and retry your statement in another new session." );
        }
    }

    private void ensureSessionIsOpen()
    {
        if ( !isOpen.get() )
        {
            throw new ClientException(
                    "No more interaction with this session is allowed " +
                    "as the current session is already closed or marked as closed. " +
                    "You get this error either because you have a bad reference to a session that has already be " +
                    "closed " +
                    "or you are trying to reuse a session that you have called `reset` on it." );
        }
    }
}
