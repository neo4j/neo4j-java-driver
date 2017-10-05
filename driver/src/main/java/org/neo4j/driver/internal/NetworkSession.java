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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.driver.internal.async.AsyncConnection;
import org.neo4j.driver.internal.async.QueryRunner;
import org.neo4j.driver.internal.logging.DelegatingLogger;
import org.neo4j.driver.internal.retry.RetryLogic;
import org.neo4j.driver.internal.spi.ConnectionProvider;
import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.StatementResultCursor;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionWork;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.types.TypeSystem;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.neo4j.driver.internal.async.Futures.failedFuture;
import static org.neo4j.driver.internal.async.Futures.getBlocking;
import static org.neo4j.driver.v1.Values.value;

public class NetworkSession implements Session
{
    private static final String LOG_NAME = "Session";

    private final ConnectionProvider connectionProvider;
    private final AccessMode mode;
    private final RetryLogic retryLogic;
    protected final Logger logger;

    private volatile Bookmark bookmark = Bookmark.empty();
    private volatile CompletionStage<ExplicitTransaction> transactionStage;
    private volatile CompletionStage<AsyncConnection> connectionStage;

    private final AtomicBoolean open = new AtomicBoolean( true );

    public NetworkSession( ConnectionProvider connectionProvider, AccessMode mode, RetryLogic retryLogic,
            Logging logging )
    {
        this.connectionProvider = connectionProvider;
        this.mode = mode;
        this.retryLogic = retryLogic;
        this.logger = new DelegatingLogger( logging.getLog( LOG_NAME ), String.valueOf( hashCode() ) );
    }

    @Override
    public StatementResult run( String statementText )
    {
        return run( statementText, Values.EmptyMap );
    }

    @Override
    public CompletionStage<StatementResultCursor> runAsync( String statementText )
    {
        return runAsync( statementText, Values.EmptyMap );
    }

    @Override
    public StatementResult run( String statementText, Map<String,Object> statementParameters )
    {
        Value params = statementParameters == null ? Values.EmptyMap : value( statementParameters );
        return run( statementText, params );
    }

    @Override
    public CompletionStage<StatementResultCursor> runAsync( String statementText,
            Map<String,Object> statementParameters )
    {
        Value params = statementParameters == null ? Values.EmptyMap : value( statementParameters );
        return runAsync( statementText, params );
    }

    @Override
    public StatementResult run( String statementTemplate, Record statementParameters )
    {
        Value params = statementParameters == null ? Values.EmptyMap : value( statementParameters.asMap() );
        return run( statementTemplate, params );
    }

    @Override
    public CompletionStage<StatementResultCursor> runAsync( String statementTemplate, Record statementParameters )
    {
        Value params = statementParameters == null ? Values.EmptyMap : value( statementParameters.asMap() );
        return runAsync( statementTemplate, params );
    }

    @Override
    public StatementResult run( String statementText, Value statementParameters )
    {
        return run( new Statement( statementText, statementParameters ) );
    }

    @Override
    public CompletionStage<StatementResultCursor> runAsync( String statementText, Value parameters )
    {
        return runAsync( new Statement( statementText, parameters ) );
    }

    @Override
    public StatementResult run( Statement statement )
    {
        StatementResultCursor cursor = getBlocking( run( statement, false ) );
        return new InternalStatementResult( cursor );
    }

    @Override
    public CompletionStage<StatementResultCursor> runAsync( Statement statement )
    {
        return run( statement, true );
    }

    @Override
    public boolean isOpen()
    {
        return open.get();
    }

    @Override
    public void close()
    {
        getBlocking( closeAsync() );
    }

    @Override
    public CompletionStage<Void> closeAsync()
    {
        if ( open.compareAndSet( true, false ) )
        {
            if ( transactionStage != null )
            {
                return transactionStage.thenCompose( ExplicitTransaction::rollbackAsync );
            }

            if ( connectionStage != null )
            {
                return connectionStage.thenCompose( AsyncConnection::forceRelease );
            }
        }
        return completedFuture( null );
    }

    @Override
    public Transaction beginTransaction()
    {
        return getBlocking( beginTransactionAsync( mode ) );
    }

    @Deprecated
    @Override
    public Transaction beginTransaction( String bookmark )
    {
        setBookmark( Bookmark.from( bookmark ) );
        return beginTransaction();
    }

    @Override
    public CompletionStage<Transaction> beginTransactionAsync()
    {
        //noinspection unchecked
        return (CompletionStage) beginTransactionAsync( mode );
    }

    @Override
    public <T> T readTransaction( TransactionWork<T> work )
    {
        return transaction( AccessMode.READ, work );
    }

    @Override
    public <T> CompletionStage<T> readTransactionAsync( TransactionWork<CompletionStage<T>> work )
    {
        return transactionAsync( AccessMode.READ, work );
    }

    @Override
    public <T> T writeTransaction( TransactionWork<T> work )
    {
        return transaction( AccessMode.WRITE, work );
    }

    @Override
    public <T> CompletionStage<T> writeTransactionAsync( TransactionWork<CompletionStage<T>> work )
    {
        return transactionAsync( AccessMode.WRITE, work );
    }

    void setBookmark( Bookmark bookmark )
    {
        if ( bookmark != null && !bookmark.isEmpty() )
        {
            this.bookmark = bookmark;
        }
    }

    @Override
    public String lastBookmark()
    {
        return bookmark == null ? null : bookmark.maxBookmarkAsString();
    }

    @Override
    public void reset()
    {
        // todo: implement this by simply sending a RESET message
        throw new UnsupportedOperationException();
    }

    @Override
    public TypeSystem typeSystem()
    {
        return InternalTypeSystem.TYPE_SYSTEM;
    }


    public void asyncTransactionClosed( ExplicitTransaction tx )
    {
        setBookmark( tx.bookmark() );
        transactionStage = null;
    }

    private <T> T transaction( AccessMode mode, TransactionWork<T> work )
    {
        return getBlocking( transactionAsync( mode, tx ->
        {
            try
            {
                T result = work.execute( tx );
                return completedFuture( result );
            }
            catch ( Throwable error )
            {
                return failedFuture( error );
            }
        } ) );
    }

    private <T> CompletionStage<T> transactionAsync( AccessMode mode, TransactionWork<CompletionStage<T>> work )
    {
        return retryLogic.retry( () ->
        {
            CompletableFuture<T> resultFuture = new CompletableFuture<>();
            CompletionStage<ExplicitTransaction> txFuture = beginTransactionAsync( mode );

            txFuture.whenComplete( ( tx, error ) ->
            {
                if ( error != null )
                {
                    resultFuture.completeExceptionally( error );
                }
                else
                {
                    executeWork( resultFuture, tx, work );
                }
            } );

            return resultFuture;
        } );
    }

    private <T> void executeWork( CompletableFuture<T> resultFuture, ExplicitTransaction tx,
            TransactionWork<CompletionStage<T>> work )
    {
        CompletionStage<T> workFuture = safeExecuteWork( tx, work );
        workFuture.whenComplete( ( result, error ) ->
        {
            if ( error != null )
            {
                rollbackTxAfterFailedTransactionWork( tx, resultFuture, error );
            }
            else
            {
                commitTxAfterSucceededTransactionWork( tx, resultFuture, result );
            }
        } );
    }

    private <T> CompletionStage<T> safeExecuteWork( ExplicitTransaction tx, TransactionWork<CompletionStage<T>> work )
    {
        // given work might fail in both async and sync way
        // async failure will result in a failed future being returned
        // sync failure will result in an exception being thrown
        try
        {
            return work.execute( tx );
        }
        catch ( Throwable workError )
        {
            // work threw an exception, wrap it in a future and proceed
            return failedFuture( workError );
        }
    }

    private <T> void rollbackTxAfterFailedTransactionWork( ExplicitTransaction tx, CompletableFuture<T> resultFuture,
            Throwable error )
    {
        if ( tx.isOpen() )
        {
            tx.rollbackAsync().whenComplete( ( ignore, rollbackError ) ->
            {
                if ( rollbackError != null )
                {
                    error.addSuppressed( rollbackError );
                }
                resultFuture.completeExceptionally( error );
            } );
        }
        else
        {
            resultFuture.completeExceptionally( error );
        }
    }

    private <T> void commitTxAfterSucceededTransactionWork( ExplicitTransaction tx, CompletableFuture<T> resultFuture,
            T result )
    {
        if ( tx.isOpen() )
        {
            tx.commitAsync().whenComplete( ( ignore, commitError ) ->
            {
                if ( commitError != null )
                {
                    resultFuture.completeExceptionally( commitError );
                }
                else
                {
                    resultFuture.complete( result );
                }
            } );
        }
        else
        {
            resultFuture.complete( result );
        }
    }

    private CompletionStage<StatementResultCursor> run( Statement statement, boolean async )
    {
        ensureSessionIsOpen();
        ensureNoOpenTransactionBeforeRunningSession();

        return acquireConnection( mode ).thenCompose( connection ->
                {
                    if ( async )
                    {
                        return QueryRunner.runAsync( connection, statement );
                    }
                    return QueryRunner.runSync( connection, statement );
                }
        );
    }

    private CompletionStage<ExplicitTransaction> beginTransactionAsync( AccessMode mode )
    {
        ensureSessionIsOpen();
        ensureNoOpenTransactionBeforeOpeningTransaction();

        transactionStage = acquireConnection( mode ).thenCompose( connection ->
        {
            ExplicitTransaction tx = new ExplicitTransaction( connection, NetworkSession.this );
            return tx.beginAsync( bookmark );
        } );

        return transactionStage;
    }

    private CompletionStage<AsyncConnection> acquireConnection( final AccessMode mode )
    {
        if ( connectionStage == null )
        {
            connectionStage = connectionProvider.acquireConnection( mode );
        }
        else
        {
            // memorize in local so same instance is transformed and used in callbacks
            CompletionStage<AsyncConnection> currentAsyncConnectionStage = connectionStage;

            connectionStage = currentAsyncConnectionStage.thenCompose( connection ->
            {
                if ( connection.tryMarkInUse() )
                {
                    return currentAsyncConnectionStage;
                }
                else
                {
                    return connectionProvider.acquireConnection( mode );
                }
            } );
        }

        return connectionStage;
    }

    private void ensureNoOpenTransactionBeforeRunningSession()
    {
        if ( transactionStage != null )
        {
            throw new ClientException( "Statements cannot be run directly on a session with an open transaction; " +
                                       "either run from within the transaction or use a different session." );
        }
    }

    private void ensureNoOpenTransactionBeforeOpeningTransaction()
    {
        if ( transactionStage != null )
        {
            throw new ClientException( "You cannot begin a transaction on a session with an open transaction; " +
                                       "either run from within the transaction or use a different session." );
        }
    }

    private void ensureSessionIsOpen()
    {
        if ( !open.get() )
        {
            throw new ClientException(
                    "No more interaction with this session are allowed as the current session is already closed. " );
        }
    }
}
