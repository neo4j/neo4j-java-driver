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
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.driver.internal.async.AsyncConnection;
import org.neo4j.driver.internal.async.Futures;
import org.neo4j.driver.internal.async.InternalStatementResultCursor;
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
    private volatile CompletionStage<ExplicitTransaction> transactionStage = completedFuture( null );
    private volatile CompletionStage<AsyncConnection> connectionStage = completedFuture( null );
    private volatile CompletionStage<InternalStatementResultCursor> lastResultStage = completedFuture( null );

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
        StatementResultCursor cursor = getBlocking( runAsync( statement, false ) );
        return new InternalStatementResult( cursor );
    }

    @Override
    public CompletionStage<StatementResultCursor> runAsync( Statement statement )
    {
        //noinspection unchecked
        return (CompletionStage) runAsync( statement, true );
    }

    @Override
    public boolean isOpen()
    {
        return open.get();
    }

    @Override
    public void close()
    {
        if ( open.compareAndSet( true, false ) )
        {
            // todo: should closeAsync() also do this waiting for buffered result?
            // todo: unit test result buffering?
            getBlocking( lastResultStage
                    .exceptionally( error -> null )
                    .thenCompose( this::ensureBuffered )
                    .thenCompose( error -> forceReleaseResources().thenApply( ignore ->
                    {
                        if ( error != null )
                        {
                            throw new CompletionException( error );
                        }
                        return null;
                    } ) ) );
        }
    }

    @Override
    public CompletionStage<Void> closeAsync()
    {
        // todo: wait for buffered result?
        if ( open.compareAndSet( true, false ) )
        {
            return forceReleaseResources();
        }
        return completedFuture( null );
    }

    // todo: test this method
    CompletionStage<Throwable> ensureBuffered( InternalStatementResultCursor cursor )
    {
        if ( cursor == null )
        {
            return completedFuture( null );
        }
        return cursor.resultBuffered();
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
        getBlocking( forceReleaseResources() );
    }

    @Override
    public TypeSystem typeSystem()
    {
        return InternalTypeSystem.TYPE_SYSTEM;
    }

    CompletionStage<Boolean> currentConnectionIsOpen()
    {
        if ( connectionStage == null )
        {
            return completedFuture( false );
        }
        return connectionStage.handle( ( connection, error ) ->
                error == null && // no acquisition error
                connection != null && // some connection has actually been acquired
                connection.isInUse() ); // and it's still being used
    }

    private <T> T transaction( AccessMode mode, TransactionWork<T> work )
    {
        return getBlocking( transactionAsync( mode, tx ->
        {
            try
            {
                // todo: given lambda can't be executed in even loop thread because it deadlocks
                // todo: event loop executes a blocking operation and waits for itself to read from the network
                // todo: this is most likely what happens...

                // todo: use of supplyAsync is a hack and it makes blocking API very different from 1.4
                // todo: because we now execute function in FJP.commonPool()

                // todo: bring back blocking retries with sleeps and etc. so that we execute TxWork in caller thread
                return CompletableFuture.supplyAsync( () -> work.execute( tx ) );
//                T result = work.execute( tx );
//                return completedFuture( result );
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

            txFuture.whenComplete( ( tx, completionError ) ->
            {
                Throwable error = Futures.completionErrorCause( completionError );
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
            Throwable error = Futures.completionErrorCause( completionError );
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

    private <T> void closeTxAfterSucceededTransactionWork( ExplicitTransaction tx, CompletableFuture<T> resultFuture,
            T result )
    {
        if ( tx.isOpen() )
        {
            tx.success();
            tx.closeAsync().whenComplete( ( ignore, completionError ) ->
            {
                Throwable commitError = Futures.completionErrorCause( completionError );
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

    private CompletionStage<InternalStatementResultCursor> runAsync( Statement statement, boolean waitForRunResponse )
    {
        ensureSessionIsOpen();

        lastResultStage = ensureNoOpenTxBeforeRunningQuery()
                .thenCompose( ignore -> acquireConnection( mode ) )
                .thenCompose( connection ->
                {
                    if ( waitForRunResponse )
                    {
                        return QueryRunner.runAsAsync( connection, statement );
                    }
                    else
                    {
                        return QueryRunner.runAsBlocking( connection, statement );
                    }
                } );

        return lastResultStage;
    }

    private CompletionStage<ExplicitTransaction> beginTransactionAsync( AccessMode mode )
    {
        ensureSessionIsOpen();

        transactionStage = ensureNoOpenTxBeforeStartingTx()
                .thenCompose( ignore -> acquireConnection( mode ) )
                .thenCompose( connection ->
                {
                    ExplicitTransaction tx = new ExplicitTransaction( connection, NetworkSession.this );
                    return tx.beginAsync( bookmark );
                } );

        return transactionStage;
    }

    private CompletionStage<AsyncConnection> acquireConnection( AccessMode mode )
    {
        // memorize in local so same instance is transformed and used in callbacks
        CompletionStage<AsyncConnection> currentAsyncConnectionStage = connectionStage;

        connectionStage = currentAsyncConnectionStage
                .exceptionally( error -> null ) // handle previous acquisition failures
                .thenCompose( connection ->
                {
                    if ( connection != null && connection.tryMarkInUse() )
                    {
                        // previous acquisition attempt was successful and connection has not been released yet
                        // continue using same connection
                        return currentAsyncConnectionStage;
                    }
                    else
                    {
                        // previous acquisition attempt failed or connection has been released
                        // acquire new connection
                        return connectionProvider.acquireConnection( mode );
                    }
                } );

        return connectionStage;
    }

    private CompletionStage<Void> forceReleaseResources()
    {
        return rollbackTransaction().thenCompose( ignore -> forceReleaseConnection() );
    }

    private CompletionStage<Void> rollbackTransaction()
    {
        return transactionStage
                .exceptionally( error -> null ) // handle previous acquisition failures
                .thenCompose( tx ->
                {
                    if ( tx != null && tx.isOpen() )
                    {
                        return tx.rollbackAsync();
                    }
                    return completedFuture( null );
                } );
    }

    private CompletionStage<Void> forceReleaseConnection()
    {
        return connectionStage
                .exceptionally( error -> null ) // handle previous acquisition failures
                .thenCompose( connection ->
                {
                    if ( connection != null )
                    {
                        return connection.forceRelease();
                    }
                    return completedFuture( null );
                } ).exceptionally( error ->
                {
                    logger.error( "Failed to rollback active transaction", error );
                    return null;
                } );
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
        return transactionStage.exceptionally( error -> null )
                .thenAccept( tx ->
                {
                    if ( tx != null && tx.isOpen() )
                    {
                        throw new ClientException( errorMessage );
                    }
                } );
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
