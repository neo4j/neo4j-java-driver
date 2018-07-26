/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.driver.internal.logging.PrefixedLogger;
import org.neo4j.driver.internal.retry.RetryLogic;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionProvider;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.StatementResultCursor;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionConfig;
import org.neo4j.driver.v1.TransactionWork;
import org.neo4j.driver.v1.exceptions.ClientException;

import static java.util.Collections.emptyMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.internal.util.Futures.failedFuture;

public class NetworkSession extends AbstractStatementRunner implements Session
{
    private static final String LOG_NAME = "Session";

    private final ConnectionProvider connectionProvider;
    private final AccessMode mode;
    private final RetryLogic retryLogic;
    private final TransactionConfig defaultTransactionConfig;
    protected final Logger logger;

    private volatile Bookmarks bookmarks = Bookmarks.empty();
    private volatile CompletionStage<ExplicitTransaction> transactionStage = completedWithNull();
    private volatile CompletionStage<Connection> connectionStage = completedWithNull();
    private volatile CompletionStage<InternalStatementResultCursor> resultCursorStage = completedWithNull();

    private final AtomicBoolean open = new AtomicBoolean( true );

    public NetworkSession( ConnectionProvider connectionProvider, AccessMode mode, RetryLogic retryLogic,
            TransactionConfig defaultTransactionConfig, Logging logging )
    {
        this.connectionProvider = connectionProvider;
        this.mode = mode;
        this.retryLogic = retryLogic;
        this.defaultTransactionConfig = defaultTransactionConfig;
        this.logger = new PrefixedLogger( "[" + hashCode() + "]", logging.getLog( LOG_NAME ) );
    }

    @Override
    public StatementResult run( Statement statement )
    {
        return run( statement, TransactionConfig.empty() );
    }

    @Override
    public StatementResult run( String statement, TransactionConfig config )
    {
        return run( statement, emptyMap(), config );
    }

    @Override
    public StatementResult run( String statement, Map<String,Object> parameters, TransactionConfig config )
    {
        return run( new Statement( statement, parameters ), config );
    }

    @Override
    public StatementResult run( Statement statement, TransactionConfig config )
    {
        StatementResultCursor cursor = Futures.blockingGet( run( statement, config, false ),
                () -> terminateConnectionOnThreadInterrupt( "Thread interrupted while running query in session" ) );

        // query executed, it is safe to obtain a connection in a blocking way
        Connection connection = Futures.getNow( connectionStage );
        return new InternalStatementResult( connection, cursor );
    }

    @Override
    public CompletionStage<StatementResultCursor> runAsync( Statement statement )
    {
        return runAsync( statement, TransactionConfig.empty() );
    }

    @Override
    public CompletionStage<StatementResultCursor> runAsync( String statement, TransactionConfig config )
    {
        return runAsync( statement, emptyMap(), config );
    }

    @Override
    public CompletionStage<StatementResultCursor> runAsync( String statement, Map<String,Object> parameters, TransactionConfig config )
    {
        return runAsync( new Statement( statement, parameters ), config );
    }

    @Override
    public CompletionStage<StatementResultCursor> runAsync( Statement statement, TransactionConfig config )
    {
        //noinspection unchecked
        return (CompletionStage) run( statement, config, true );
    }

    @Override
    public boolean isOpen()
    {
        return open.get();
    }

    @Override
    public void close()
    {
        Futures.blockingGet( closeAsync(),
                () -> terminateConnectionOnThreadInterrupt( "Thread interrupted while closing the session" ) );
    }

    @Override
    public CompletionStage<Void> closeAsync()
    {
        if ( open.compareAndSet( true, false ) )
        {
            return resultCursorStage.thenCompose( cursor ->
            {
                if ( cursor != null )
                {
                    // there exists a cursor with potentially unconsumed error, try to extract and propagate it
                    return cursor.failureAsync();
                }
                // no result cursor exists so no error exists
                return completedWithNull();
            } ).thenCompose( cursorError -> closeTransactionAndReleaseConnection().thenApply( txCloseError ->
            {
                // now we have cursor error, active transaction has been closed and connection has been released
                // back to the pool; try to propagate cursor and transaction close errors, if any
                CompletionException combinedError = Futures.combineErrors( cursorError, txCloseError );
                if ( combinedError != null )
                {
                    throw combinedError;
                }
                return null;
            } ) );
        }
        return completedWithNull();
    }

    @Override
    public Transaction beginTransaction()
    {
        return beginTransaction( defaultTransactionConfig );
    }

    @Override
    public Transaction beginTransaction( TransactionConfig config )
    {
        return beginTransaction( mode, config );
    }

    @Deprecated
    @Override
    public Transaction beginTransaction( String bookmark )
    {
        setBookmarks( Bookmarks.from( bookmark ) );
        return beginTransaction();
    }

    @Override
    public CompletionStage<Transaction> beginTransactionAsync()
    {
        return beginTransactionAsync( defaultTransactionConfig );
    }

    @Override
    public CompletionStage<Transaction> beginTransactionAsync( TransactionConfig config )
    {
        //noinspection unchecked
        return (CompletionStage) beginTransactionAsync( mode, config );
    }

    @Override
    public <T> T readTransaction( TransactionWork<T> work )
    {
        return readTransaction( work, defaultTransactionConfig );
    }

    @Override
    public <T> T readTransaction( TransactionWork<T> work, TransactionConfig config )
    {
        return transaction( AccessMode.READ, work, config );
    }

    @Override
    public <T> CompletionStage<T> readTransactionAsync( TransactionWork<CompletionStage<T>> work )
    {
        return readTransactionAsync( work, defaultTransactionConfig );
    }

    @Override
    public <T> CompletionStage<T> readTransactionAsync( TransactionWork<CompletionStage<T>> work, TransactionConfig config )
    {
        return transactionAsync( AccessMode.READ, work, config );
    }

    @Override
    public <T> T writeTransaction( TransactionWork<T> work )
    {
        return writeTransaction( work, defaultTransactionConfig );
    }

    @Override
    public <T> T writeTransaction( TransactionWork<T> work, TransactionConfig config )
    {
        return transaction( AccessMode.WRITE, work, config );
    }

    @Override
    public <T> CompletionStage<T> writeTransactionAsync( TransactionWork<CompletionStage<T>> work )
    {
        return writeTransactionAsync( work, defaultTransactionConfig );
    }

    @Override
    public <T> CompletionStage<T> writeTransactionAsync( TransactionWork<CompletionStage<T>> work, TransactionConfig config )
    {
        return transactionAsync( AccessMode.WRITE, work, config );
    }

    void setBookmarks( Bookmarks bookmarks )
    {
        if ( bookmarks != null && !bookmarks.isEmpty() )
        {
            this.bookmarks = bookmarks;
        }
    }

    @Override
    public String lastBookmark()
    {
        return bookmarks == null ? null : bookmarks.maxBookmarkAsString();
    }

    @Override
    @SuppressWarnings( "deprecation" )
    public void reset()
    {
        Futures.blockingGet( resetAsync(),
                () -> terminateConnectionOnThreadInterrupt( "Thread interrupted while resetting the session" ) );
    }

    private CompletionStage<Void> resetAsync()
    {
        return existingTransactionOrNull()
                .thenAccept( tx ->
                {
                    if ( tx != null )
                    {
                        tx.markTerminated();
                    }
                } )
                .thenCompose( ignore -> connectionStage )
                .thenCompose( connection ->
                {
                    if ( connection != null )
                    {
                        // there exists an active connection, send a RESET message over it
                        return connection.reset();
                    }
                    return completedWithNull();
                } );
    }

    CompletionStage<Boolean> currentConnectionIsOpen()
    {
        return connectionStage.handle( ( connection, error ) ->
                error == null && // no acquisition error
                connection != null && // some connection has actually been acquired
                connection.isOpen() ); // and it's still open
    }

    private <T> T transaction( AccessMode mode, TransactionWork<T> work, TransactionConfig config )
    {
        // use different code path compared to async so that work is executed in the caller thread
        // caller thread will also be the one who sleeps between retries;
        // it is unsafe to execute retries in the event loop threads because this can cause a deadlock
        // event loop thread will bock and wait for itself to read some data
        return retryLogic.retry( () ->
        {
            try ( Transaction tx = beginTransaction( mode, config ) )
            {
                try
                {
                    T result = work.execute( tx );
                    tx.success();
                    return result;
                }
                catch ( Throwable t )
                {
                    // mark transaction for failure if the given unit of work threw exception
                    // this will override any success marks that were made by the unit of work
                    tx.failure();
                    throw t;
                }
            }
        } );
    }

    private <T> CompletionStage<T> transactionAsync( AccessMode mode, TransactionWork<CompletionStage<T>> work, TransactionConfig config )
    {
        return retryLogic.retryAsync( () ->
        {
            CompletableFuture<T> resultFuture = new CompletableFuture<>();
            CompletionStage<ExplicitTransaction> txFuture = beginTransactionAsync( mode, config );

            txFuture.whenComplete( ( tx, completionError ) ->
            {
                Throwable error = Futures.completionExceptionCause( completionError );
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
        workFuture.whenComplete( ( result, completionError ) ->
        {
            Throwable error = Futures.completionExceptionCause( completionError );
            if ( error != null )
            {
                rollbackTxAfterFailedTransactionWork( tx, resultFuture, error );
            }
            else
            {
                closeTxAfterSucceededTransactionWork( tx, resultFuture, result );
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
            CompletionStage<T> result = work.execute( tx );

            // protect from given transaction function returning null
            return result == null ? completedWithNull() : result;
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

    private <T> void closeTxAfterSucceededTransactionWork( ExplicitTransaction tx, CompletableFuture<T> resultFuture,
            T result )
    {
        if ( tx.isOpen() )
        {
            tx.success();
            tx.closeAsync().whenComplete( ( ignore, completionError ) ->
            {
                Throwable commitError = Futures.completionExceptionCause( completionError );
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

    private CompletionStage<InternalStatementResultCursor> run( Statement statement, TransactionConfig config, boolean waitForRunResponse )
    {
        ensureSessionIsOpen();

        CompletionStage<InternalStatementResultCursor> newResultCursorStage = ensureNoOpenTxBeforeRunningQuery()
                .thenCompose( ignore -> acquireConnection( mode ) )
                .thenCompose( connection ->
                        connection.protocol().runInAutoCommitTransaction( connection, statement, bookmarks, config, waitForRunResponse ) );

        resultCursorStage = newResultCursorStage.exceptionally( error -> null );

        return newResultCursorStage;
    }

    private Transaction beginTransaction( AccessMode mode, TransactionConfig config )
    {
        return Futures.blockingGet( beginTransactionAsync( mode, config ),
                () -> terminateConnectionOnThreadInterrupt( "Thread interrupted while starting a transaction" ) );
    }

    private CompletionStage<ExplicitTransaction> beginTransactionAsync( AccessMode mode, TransactionConfig config )
    {
        ensureSessionIsOpen();

        // create a chain that acquires connection and starts a transaction
        CompletionStage<ExplicitTransaction> newTransactionStage = ensureNoOpenTxBeforeStartingTx()
                .thenCompose( ignore -> acquireConnection( mode ) )
                .thenCompose( connection ->
                {
                    ExplicitTransaction tx = new ExplicitTransaction( connection, NetworkSession.this );
                    return tx.beginAsync( bookmarks, config );
                } );

        // update the reference to the only known transaction
        CompletionStage<ExplicitTransaction> currentTransactionStage = transactionStage;

        transactionStage = newTransactionStage
                .exceptionally( error -> null ) // ignore errors from starting new transaction
                .thenCompose( tx ->
                {
                    if ( tx == null )
                    {
                        // failed to begin new transaction, keep reference to the existing one
                        return currentTransactionStage;
                    }
                    // new transaction started, keep reference to it
                    return completedFuture( tx );
                } );

        return newTransactionStage;
    }

    private CompletionStage<Connection> acquireConnection( AccessMode mode )
    {
        CompletionStage<Connection> currentConnectionStage = connectionStage;

        CompletionStage<Connection> newConnectionStage = resultCursorStage.thenCompose( cursor ->
        {
            if ( cursor == null )
            {
                return completedWithNull();
            }
            // make sure previous result is fully consumed and connection is released back to the pool
            return cursor.failureAsync();
        } ).thenCompose( error ->
        {
            if ( error == null )
            {
                // there is no unconsumed error, so one of the following is true:
                //   1) this is first time connection is acquired in this session
                //   2) previous result has been successful and is fully consumed
                //   3) previous result failed and error has been consumed

                // return existing connection, which should've been released back to the pool by now
                return currentConnectionStage.exceptionally( ignore -> null );
            }
            else
            {
                // there exists unconsumed error, re-throw it
                throw new CompletionException( error );
            }
        } ).thenCompose( existingConnection ->
        {
            if ( existingConnection != null && existingConnection.isOpen() )
            {
                // there somehow is an existing open connection, this should not happen, just a precondition
                throw new IllegalStateException( "Existing open connection detected" );
            }
            return connectionProvider.acquireConnection( mode );
        } );

        connectionStage = newConnectionStage.exceptionally( error -> null );

        return newConnectionStage;
    }

    private CompletionStage<Throwable> closeTransactionAndReleaseConnection()
    {
        return existingTransactionOrNull().thenCompose( tx ->
        {
            if ( tx != null )
            {
                // there exists an open transaction, let's close it and propagate the error, if any
                return tx.closeAsync()
                        .thenApply( ignore -> (Throwable) null )
                        .exceptionally( error -> error );
            }
            // no open transaction so nothing to close
            return completedWithNull();
        } ).thenCompose( txCloseError ->
                // then release the connection and propagate transaction close error, if any
                releaseConnection().thenApply( ignore -> txCloseError ) );
    }

    private CompletionStage<Void> releaseConnection()
    {
        return connectionStage.thenCompose( connection ->
        {
            if ( connection != null )
            {
                // there exists connection, try to release it back to the pool
                return connection.release();
            }
            // no connection so return null
            return completedWithNull();
        } );
    }

    private void terminateConnectionOnThreadInterrupt( String reason )
    {
        // try to get current connection if it has been acquired
        Connection connection = null;
        try
        {
            connection = Futures.getNow( connectionStage );
        }
        catch ( Throwable ignore )
        {
            // ignore errors because handing interruptions is best effort
        }

        if ( connection != null )
        {
            connection.terminateAndRelease( reason );
        }
    }

    private CompletionStage<Void> ensureNoOpenTxBeforeRunningQuery()
    {
        return ensureNoOpenTx( "Statements cannot be run directly on a session with an open transaction; " +
                               "either run from within the transaction or use a different session." );
    }

    private CompletionStage<Void> ensureNoOpenTxBeforeStartingTx()
    {
        return ensureNoOpenTx( "You cannot begin a transaction on a session with an open transaction; " +
                               "either run from within the transaction or use a different session." );
    }

    private CompletionStage<Void> ensureNoOpenTx( String errorMessage )
    {
        return existingTransactionOrNull().thenAccept( tx ->
        {
            if ( tx != null )
            {
                throw new ClientException( errorMessage );
            }
        } );
    }

    private CompletionStage<ExplicitTransaction> existingTransactionOrNull()
    {
        return transactionStage
                .exceptionally( error -> null ) // handle previous connection acquisition and tx begin failures
                .thenApply( tx -> tx != null && tx.isOpen() ? tx : null );
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
