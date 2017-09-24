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

import org.neo4j.driver.ResultResourcesHandler;
import org.neo4j.driver.internal.async.AsyncConnection;
import org.neo4j.driver.internal.async.Futures;
import org.neo4j.driver.internal.async.QueryRunner;
import org.neo4j.driver.internal.logging.DelegatingLogger;
import org.neo4j.driver.internal.retry.RetryLogic;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionProvider;
import org.neo4j.driver.internal.spi.PooledConnection;
import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.internal.util.Supplier;
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
import static org.neo4j.driver.v1.Values.value;

public class NetworkSession implements Session, SessionResourcesHandler, ResultResourcesHandler
{
    private static final String LOG_NAME = "Session";

    private final ConnectionProvider connectionProvider;
    private final AccessMode mode;
    private final RetryLogic retryLogic;
    protected final Logger logger;

    private volatile Bookmark bookmark = Bookmark.empty();
    private PooledConnection currentConnection;
    private ExplicitTransaction currentTransaction;
    private volatile CompletionStage<ExplicitTransaction> asyncTransactionStage;

    private CompletionStage<AsyncConnection> asyncConnectionStage;

    private final AtomicBoolean isOpen = new AtomicBoolean( true );

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
        ensureSessionIsOpen();
        ensureNoOpenTransactionBeforeRunningSession();

        syncAndCloseCurrentConnection();
        currentConnection = acquireConnection( mode );

        return run( currentConnection, statement, this );
    }

    @Override
    public CompletionStage<StatementResultCursor> runAsync( final Statement statement )
    {
        ensureSessionIsOpen();
        ensureNoOpenTransactionBeforeRunningSession();

        return acquireAsyncConnection( mode ).thenCompose( connection ->
                QueryRunner.runAsync( connection, statement ) );
    }

    public static StatementResult run( Connection connection, Statement statement,
            ResultResourcesHandler resourcesHandler )
    {
        InternalStatementResult result = new InternalStatementResult( statement, connection, resourcesHandler );
        connection.run( statement.text(), statement.parameters().asMap( Values.ofValue() ),
                result.runResponseHandler() );
        connection.pullAll( result.pullAllResponseHandler() );
        connection.flush();
        return result;
    }

    @Deprecated
    @Override
    public synchronized void reset()
    {
        ensureSessionIsOpen();
        ensureNoUnrecoverableError();

        if ( currentTransaction != null )
        {
            currentTransaction.markToClose();
            setBookmark( currentTransaction.bookmark() );
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
                    logger.error( "Failed to close transaction", e );
                }
            }
        }

        syncAndCloseCurrentConnection();

        try
        {
            closeAsync().toCompletableFuture().get();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public CompletionStage<Void> closeAsync()
    {
        if ( asyncConnectionStage != null )
        {
            return asyncConnectionStage.thenCompose( AsyncConnection::forceRelease );
        }
        else if ( asyncTransactionStage != null )
        {
            return asyncTransactionStage.thenCompose( ExplicitTransaction::rollbackAsync );
        }
        else
        {
            return completedFuture( null );
        }
    }

    @Override
    public synchronized Transaction beginTransaction()
    {
        return beginTransaction( mode );
    }

    @Deprecated
    @Override
    public synchronized Transaction beginTransaction( String bookmark )
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
    public TypeSystem typeSystem()
    {
        return InternalTypeSystem.TYPE_SYSTEM;
    }

    @Override
    public synchronized void onResultConsumed()
    {
        closeCurrentConnection();
    }

    @Override
    public void resultFetched()
    {
        closeCurrentConnection();
    }

    @Override
    public void resultFailed( Throwable error )
    {
        resultFetched();
    }

    @Override
    public synchronized void onTransactionClosed( ExplicitTransaction tx )
    {
        if ( currentTransaction != null && currentTransaction == tx )
        {
            closeCurrentConnection();
            setBookmark( currentTransaction.bookmark() );
            currentTransaction = null;
        }
    }

    public void asyncTransactionClosed( ExplicitTransaction tx )
    {
        setBookmark( tx.bookmark() );
        asyncTransactionStage = null;
    }

    @Override
    public synchronized void onConnectionError( boolean recoverable )
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

    private <T> T transaction( final AccessMode mode, final TransactionWork<T> work )
    {
        return retryLogic.retry( new Supplier<T>()
        {
            @Override
            public T get()
            {
                try ( Transaction tx = beginTransaction( mode ) )
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
            }
        } );
    }

    private <T> CompletionStage<T> transactionAsync( AccessMode mode, TransactionWork<CompletionStage<T>> work )
    {
        return retryLogic.retryAsync( () ->
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
            return Futures.failedFuture( workError );
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

    private synchronized Transaction beginTransaction( AccessMode mode )
    {
        ensureSessionIsOpen();
        ensureNoOpenTransactionBeforeOpeningTransaction();

        syncAndCloseCurrentConnection();
        currentConnection = acquireConnection( mode );

        ExplicitTransaction tx = new ExplicitTransaction( currentConnection, this );
        tx.begin( bookmark );
        currentTransaction = tx;
        currentConnection.setResourcesHandler( this );
        return currentTransaction;
    }

    private synchronized CompletionStage<ExplicitTransaction> beginTransactionAsync( AccessMode mode )
    {
        ensureSessionIsOpen();
        ensureNoOpenTransactionBeforeOpeningTransaction();

        asyncTransactionStage = acquireAsyncConnection( mode ).thenCompose( connection ->
        {
            ExplicitTransaction tx = new ExplicitTransaction( connection, NetworkSession.this );
            return tx.beginAsync( bookmark );
        } );

        return asyncTransactionStage;
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
        if ( currentTransaction != null || asyncTransactionStage != null )
        {
            throw new ClientException( "Statements cannot be run directly on a session with an open transaction;" +
                                       " either run from within the transaction or use a different session." );
        }
    }

    //should be called from a synchronized block
    private void ensureNoOpenTransactionBeforeOpeningTransaction()
    {
        if ( currentTransaction != null || asyncTransactionStage != null )
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

    private PooledConnection acquireConnection( AccessMode mode )
    {
        PooledConnection connection = connectionProvider.acquireConnection( mode );
        logger.debug( "Acquired connection " + connection.hashCode() );
        return connection;
    }

    private CompletionStage<AsyncConnection> acquireAsyncConnection( final AccessMode mode )
    {
        if ( asyncConnectionStage == null )
        {
            asyncConnectionStage = connectionProvider.acquireAsyncConnection( mode );
        }
        else
        {
            // memorize in local so same instance is transformed and used in callbacks
            CompletionStage<AsyncConnection> currentAsyncConnectionStage = asyncConnectionStage;

            asyncConnectionStage = currentAsyncConnectionStage.thenCompose( connection ->
            {
                if ( connection.tryMarkInUse() )
                {
                    return currentAsyncConnectionStage;
                }
                else
                {
                    return connectionProvider.acquireAsyncConnection( mode );
                }
            } );
        }

        return asyncConnectionStage;
    }

    boolean currentConnectionIsOpen()
    {
        return currentConnection != null && currentConnection.isOpen();
    }

    private void syncAndCloseCurrentConnection()
    {
        closeCurrentConnection( true );
    }

    private void closeCurrentConnection()
    {
        closeCurrentConnection( false );
    }

    private void closeCurrentConnection( boolean sync )
    {
        if ( currentConnection == null )
        {
            return;
        }

        PooledConnection connection = currentConnection;
        currentConnection = null;
        try
        {
            if ( sync && connection.isOpen() )
            {
                connection.sync();
            }
        }
        finally
        {
            connection.close();
            logger.debug( "Released connection " + connection.hashCode() );
        }
    }
}
