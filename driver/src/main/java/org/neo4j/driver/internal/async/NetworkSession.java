/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.driver.internal.async;

import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Query;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.TransactionNestingException;
import org.neo4j.driver.internal.BookmarkHolder;
import org.neo4j.driver.internal.DatabaseName;
import org.neo4j.driver.internal.FailableCursor;
import org.neo4j.driver.internal.cursor.AsyncResultCursor;
import org.neo4j.driver.internal.cursor.RxResultCursor;
import org.neo4j.driver.internal.cursor.ResultCursorFactory;
import org.neo4j.driver.internal.logging.PrefixedLogger;
import org.neo4j.driver.internal.retry.RetryLogic;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionProvider;
import org.neo4j.driver.internal.util.Futures;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;

public class NetworkSession
{
    private static final String LOG_NAME = "Session";

    private final ConnectionProvider connectionProvider;
    private final NetworkSessionConnectionContext connectionContext;
    private final AccessMode mode;
    private final RetryLogic retryLogic;
    protected final Logger logger;

    private final BookmarkHolder bookmarkHolder;
    private final long fetchSize;
    private volatile CompletionStage<UnmanagedTransaction> transactionStage = completedWithNull();
    private volatile CompletionStage<Connection> connectionStage = completedWithNull();
    private volatile CompletionStage<? extends FailableCursor> resultCursorStage = completedWithNull();

    private final AtomicBoolean open = new AtomicBoolean( true );

    public NetworkSession( ConnectionProvider connectionProvider, RetryLogic retryLogic, DatabaseName databaseName, AccessMode mode,
            BookmarkHolder bookmarkHolder, long fetchSize, Logging logging )
    {
        this.connectionProvider = connectionProvider;
        this.mode = mode;
        this.retryLogic = retryLogic;
        this.logger = new PrefixedLogger( "[" + hashCode() + "]", logging.getLog( LOG_NAME ) );
        this.bookmarkHolder = bookmarkHolder;
        this.connectionContext = new NetworkSessionConnectionContext( databaseName, bookmarkHolder.getBookmark() );
        this.fetchSize = fetchSize;
    }

    public CompletionStage<ResultCursor> runAsync(Query query, TransactionConfig config, boolean waitForRunResponse )
    {
        CompletionStage<AsyncResultCursor> newResultCursorStage =
                buildResultCursorFactory(query, config, waitForRunResponse ).thenCompose( ResultCursorFactory::asyncResult );

        resultCursorStage = newResultCursorStage.exceptionally( error -> null );
        return newResultCursorStage.thenApply( cursor -> cursor ); // convert the return type
    }

    public CompletionStage<RxResultCursor> runRx(Query query, TransactionConfig config )
    {
        CompletionStage<RxResultCursor> newResultCursorStage =
                buildResultCursorFactory(query, config, true ).thenCompose( ResultCursorFactory::rxResult );

        resultCursorStage = newResultCursorStage.exceptionally( error -> null );
        return newResultCursorStage;
    }

    public CompletionStage<UnmanagedTransaction> beginTransactionAsync( TransactionConfig config )
    {
        return this.beginTransactionAsync( mode, config );
    }

    public CompletionStage<UnmanagedTransaction> beginTransactionAsync( AccessMode mode, TransactionConfig config )
    {
        ensureSessionIsOpen();

        // create a chain that acquires connection and starts a transaction
        CompletionStage<UnmanagedTransaction> newTransactionStage = ensureNoOpenTxBeforeStartingTx()
                .thenCompose( ignore -> acquireConnection( mode ) )
                .thenCompose( connection ->
                {
                    UnmanagedTransaction tx = new UnmanagedTransaction( connection, bookmarkHolder, fetchSize );
                    return tx.beginAsync( bookmarkHolder.getBookmark(), config );
                } );

        // update the reference to the only known transaction
        CompletionStage<UnmanagedTransaction> currentTransactionStage = transactionStage;

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

    public CompletionStage<Void> resetAsync()
    {
        return existingTransactionOrNull()
                .thenAccept( tx ->
                {
                    if ( tx != null )
                    {
                        tx.markTerminated( null );
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

    public RetryLogic retryLogic()
    {
        return retryLogic;
    }

    public Bookmark lastBookmark()
    {
        return bookmarkHolder.getBookmark();
    }

    public CompletionStage<Void> releaseConnectionAsync()
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

    public CompletionStage<Connection> connectionAsync()
    {
        return connectionStage;
    }

    public boolean isOpen()
    {
        return open.get();
    }

    public CompletionStage<Void> closeAsync()
    {
        if ( open.compareAndSet( true, false ) )
        {
            return resultCursorStage.thenCompose( cursor ->
            {
                if ( cursor != null )
                {
                    // there exists a cursor with potentially unconsumed error, try to extract and propagate it
                    return cursor.discardAllFailureAsync();
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

    protected CompletionStage<Boolean> currentConnectionIsOpen()
    {
        return connectionStage.handle( ( connection, error ) ->
                error == null && // no acquisition error
                connection != null && // some connection has actually been acquired
                connection.isOpen() ); // and it's still open
    }

    private CompletionStage<ResultCursorFactory> buildResultCursorFactory(Query query, TransactionConfig config, boolean waitForRunResponse )
    {
        ensureSessionIsOpen();

        return ensureNoOpenTxBeforeRunningQuery()
                .thenCompose( ignore -> acquireConnection( mode ) )
                .thenCompose( connection -> {
                    try
                    {
                        ResultCursorFactory factory = connection.protocol()
                                .runInAutoCommitTransaction( connection, query, bookmarkHolder, config, waitForRunResponse, fetchSize );
                        return completedFuture( factory );
                    }
                    catch ( Throwable e )
                    {
                        return Futures.failedFuture( e );
                    }
                } );
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
            return cursor.pullAllFailureAsync();
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
            return connectionProvider.acquireConnection( connectionContext.contextWithMode( mode ) );
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
                releaseConnectionAsync().thenApply( ignore -> txCloseError ) );
    }

    private CompletionStage<Void> ensureNoOpenTxBeforeRunningQuery()
    {
        return ensureNoOpenTx( "Queries cannot be run directly on a session with an open transaction; " +
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
                throw new TransactionNestingException( errorMessage );
            }
        } );
    }

    private CompletionStage<UnmanagedTransaction> existingTransactionOrNull()
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

    /**
     * The {@link NetworkSessionConnectionContext#mode} can be mutable for a session connection context
     */
    private static class NetworkSessionConnectionContext implements ConnectionContext
    {
        private final DatabaseName databaseName;
        private AccessMode mode;

        // This bookmark is only used for rediscovery.
        // It has to be the initial bookmark given at the creation of the session.
        // As only that bookmark could carry extra system bookmarks
        private final Bookmark rediscoveryBookmark;

        private NetworkSessionConnectionContext( DatabaseName databaseName, Bookmark bookmark )
        {
            this.databaseName = databaseName;
            this.rediscoveryBookmark = bookmark;
        }

        private ConnectionContext contextWithMode( AccessMode mode )
        {
            this.mode = mode;
            return this;
        }

        @Override
        public DatabaseName databaseName()
        {
            return databaseName;
        }

        @Override
        public AccessMode mode()
        {
            return mode;
        }

        @Override
        public Bookmark rediscoveryBookmark()
        {
            return rediscoveryBookmark;
        }
    }

}
