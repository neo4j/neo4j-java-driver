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
package org.neo4j.driver.internal;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionProvider;
import org.neo4j.driver.internal.spi.PooledConnection;
import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.types.TypeSystem;

import static java.lang.String.format;
import static org.neo4j.driver.v1.Values.value;

public class NetworkSession implements Session, ConnectionHandler
{
    private final ConnectionProvider connectionProvider;
    private final String sessionId;
    private final AccessMode mode;
    protected final Logger logger;

    private String lastBookmark;
    private PooledConnection currentConnection;
    private ExplicitTransaction currentTransaction;

    private final AtomicBoolean isOpen = new AtomicBoolean( true );

    // todo: make sure logging is correct
    public NetworkSession( ConnectionProvider connectionProvider, AccessMode mode, Logging logging )
    {
        this.connectionProvider = connectionProvider;
        this.sessionId = UUID.randomUUID().toString();
        this.mode = mode;
        this.logger = logging.getLog( format( "Session-%s", sessionId ) );
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
        ensureSessionIsOpen();
        ensureNoOpenTransactionBeforeRunningSession();

        closeCurrentConnection( true );
        currentConnection = acquireConnection();

        return run( currentConnection, statement, this );
    }

    public static StatementResult run( Connection connection, Statement statement, ConnectionHandler connectionHandler )
    {
        InternalStatementResult cursor = new InternalStatementResult( connection, connectionHandler, null, statement );
        connection.run( statement.text(), statement.parameters().asMap( Values.ofValue() ),
                cursor.runResponseCollector() );
        connection.pullAll( cursor.pullAllResponseCollector() );
        connection.flush();
        return cursor;
    }

    @Override
    public synchronized void reset()
    {
        ensureSessionIsOpen();
        ensureNoUnrecoverableError();

        if ( currentTransaction != null )
        {
            currentTransaction.markToClose();
            lastBookmark = currentTransaction.bookmark();
            currentTransaction = null;
        }
        if ( currentConnection != null )
        {
            currentConnection.resetAsync();
        }
    }

    @Override
    public boolean isOpen()
    {
        return isOpen.get();
    }

    @Override
    public void close()
    {
        // Use atomic operation to protect from closing the connection twice (putting back to the pool twice).
        if ( !isOpen.compareAndSet( true, false ) )
        {
            throw new ClientException( "This session has already been closed." );
        }

        if ( currentConnection != null && !currentConnection.isOpen() )
        {
            // the socket connection is already closed due to some error, cannot send more data
            closeCurrentConnection( false );
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
        closeCurrentConnection( true );
    }

    @Override
    public Transaction beginTransaction()
    {
        return beginTransaction( null );
    }

    @Override
    public synchronized Transaction beginTransaction( String bookmark )
    {
        ensureSessionIsOpen();
        ensureNoOpenTransactionBeforeOpeningTransaction();

        closeCurrentConnection( true );
        currentConnection = acquireConnection();

        currentTransaction = new ExplicitTransaction( currentConnection, this, bookmark );
        currentConnection.setConnectionHandler( this );
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

    @Override
    public synchronized void resultBuffered()
    {
        closeCurrentConnection( true );
    }

    @Override
    public synchronized void transactionClosed( ExplicitTransaction tx )
    {
        if ( currentTransaction != null && currentTransaction == tx )
        {
            lastBookmark = currentTransaction.bookmark();
            currentTransaction = null;
        }
    }

    @Override
    public synchronized void connectionErrorOccurred( boolean recoverable )
    {
        // must check if transaction has been closed
        if ( currentTransaction != null )
        {
            if ( recoverable )
            {
                currentTransaction.failure();
            }
            else
            {
                currentTransaction.markToClose();
            }
        }
    }

    private void ensureNoUnrecoverableError()
    {
        if ( currentConnection != null && currentConnection.hasUnrecoverableErrors() )
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

    private PooledConnection acquireConnection()
    {
        return connectionProvider.acquireConnection( mode );
    }

    boolean currentConnectionIsOpen()
    {
        return currentConnection != null && currentConnection.isOpen();
    }

    private void closeCurrentConnection( boolean sync )
    {
        if ( currentConnection == null )
        {
            return;
        }

        try ( PooledConnection connection = currentConnection )
        {
            currentConnection = null;
            if ( sync )
            {
                connection.sync();
            }
        }
        finally
        {
            logger.debug( "~~ connection released by [session-%s]", sessionId );
        }
    }
}
