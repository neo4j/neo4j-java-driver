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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;

import org.neo4j.driver.internal.async.QueryRunner;
import org.neo4j.driver.internal.async.ResultCursorsHolder;
import org.neo4j.driver.internal.handlers.BeginTxResponseHandler;
import org.neo4j.driver.internal.handlers.CommitTxResponseHandler;
import org.neo4j.driver.internal.handlers.NoOpResponseHandler;
import org.neo4j.driver.internal.handlers.RollbackTxResponseHandler;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.PullAllMessage;
import org.neo4j.driver.internal.messaging.RunMessage;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.StatementResultCursor;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.exceptions.ClientException;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.internal.util.Futures.failedFuture;

public class ExplicitTransaction extends AbstractStatementRunner implements Transaction
{
    private static final String BEGIN_QUERY = "BEGIN";

    private static final Message BEGIN_MESSAGE = new RunMessage( BEGIN_QUERY );
    private static final Message COMMIT_MESSAGE = new RunMessage( "COMMIT" );
    private static final Message ROLLBACK_MESSAGE = new RunMessage( "ROLLBACK" );

    private enum State
    {
        /** The transaction is running with no explicit success or failure marked */
        ACTIVE,

        /** Running, user marked for success, meaning it'll value committed */
        MARKED_SUCCESS,

        /** User marked as failed, meaning it'll be rolled back. */
        MARKED_FAILED,

        /**
         * This transaction has been terminated either because of explicit {@link Session#reset()} or because of a
         * fatal connection error.
         */
        TERMINATED,

        /** This transaction has successfully committed */
        COMMITTED,

        /** This transaction has been rolled back */
        ROLLED_BACK
    }

    private final Connection connection;
    private final NetworkSession session;
    private final ResultCursorsHolder resultCursors;

    private volatile Bookmark bookmark = Bookmark.empty();
    private volatile State state = State.ACTIVE;

    public ExplicitTransaction( Connection connection, NetworkSession session )
    {
        this.connection = connection;
        this.session = session;
        this.resultCursors = new ResultCursorsHolder();
    }

    public CompletionStage<ExplicitTransaction> beginAsync( Bookmark initialBookmark )
    {
        if ( initialBookmark.isEmpty() )
        {
            connection.write(
                    BEGIN_MESSAGE, NoOpResponseHandler.INSTANCE,
                    PullAllMessage.PULL_ALL, NoOpResponseHandler.INSTANCE );
            return completedFuture( this );
        }
        else
        {
            CompletableFuture<ExplicitTransaction> beginFuture = new CompletableFuture<>();
            connection.writeAndFlush(
                    new RunMessage( BEGIN_QUERY, initialBookmark.asBeginTransactionParameters() ), NoOpResponseHandler.INSTANCE,
                    PullAllMessage.PULL_ALL, new BeginTxResponseHandler<>( beginFuture, this ) );

            return beginFuture.handle( ( tx, beginError ) ->
            {
                if ( beginError != null )
                {
                    // release connection if begin failed, transaction can't be started
                    connection.release();
                    throw Futures.asCompletionException( beginError );
                }
                return tx;
            } );
        }
    }

    @Override
    public void success()
    {
        if ( state == State.ACTIVE )
        {
            state = State.MARKED_SUCCESS;
        }
    }

    @Override
    public void failure()
    {
        if ( state == State.ACTIVE || state == State.MARKED_SUCCESS )
        {
            state = State.MARKED_FAILED;
        }
    }

    @Override
    public void close()
    {
        Futures.blockingGet( closeAsync(),
                () -> terminateConnectionOnThreadInterrupt( "Thread interrupted while closing the transaction" ) );
    }

    CompletionStage<Void> closeAsync()
    {
        if ( state == State.MARKED_SUCCESS )
        {
            return commitAsync();
        }
        else if ( state != State.COMMITTED && state != State.ROLLED_BACK )
        {
            return rollbackAsync();
        }
        else
        {
            return completedWithNull();
        }
    }

    @Override
    public CompletionStage<Void> commitAsync()
    {
        if ( state == State.COMMITTED )
        {
            return completedWithNull();
        }
        else if ( state == State.ROLLED_BACK )
        {
            return failedFuture( new ClientException( "Can't commit, transaction has been rolled back" ) );
        }
        else
        {
            return resultCursors.retrieveNotConsumedError()
                    .thenCompose( error -> doCommitAsync().handle( handleCommitOrRollback( error ) ) )
                    .whenComplete( ( ignore, error ) -> transactionClosed( State.COMMITTED ) );
        }
    }

    @Override
    public CompletionStage<Void> rollbackAsync()
    {
        if ( state == State.COMMITTED )
        {
            return failedFuture( new ClientException( "Can't rollback, transaction has been committed" ) );
        }
        else if ( state == State.ROLLED_BACK )
        {
            return completedWithNull();
        }
        else
        {
            return resultCursors.retrieveNotConsumedError()
                    .thenCompose( error -> doRollbackAsync().handle( handleCommitOrRollback( error ) ) )
                    .whenComplete( ( ignore, error ) -> transactionClosed( State.ROLLED_BACK ) );
        }
    }

    @Override
    public StatementResult run( Statement statement )
    {
        StatementResultCursor cursor = Futures.blockingGet( run( statement, false ),
                () -> terminateConnectionOnThreadInterrupt( "Thread interrupted while running query in transaction" ) );
        return new InternalStatementResult( connection, cursor );
    }

    @Override
    public CompletionStage<StatementResultCursor> runAsync( Statement statement )
    {
        //noinspection unchecked
        return (CompletionStage) run( statement, true );
    }

    private CompletionStage<InternalStatementResultCursor> run( Statement statement, boolean waitForRunResponse )
    {
        ensureCanRunQueries();
        CompletionStage<InternalStatementResultCursor> cursorStage =
                QueryRunner.runInTransaction( connection, statement, this, waitForRunResponse );
        resultCursors.add( cursorStage );
        return cursorStage;
    }

    private void ensureCanRunQueries()
    {
        if ( state == State.COMMITTED )
        {
            throw new ClientException( "Cannot run more statements in this transaction, it has been committed" );
        }
        else if ( state == State.ROLLED_BACK )
        {
            throw new ClientException( "Cannot run more statements in this transaction, it has been rolled back" );
        }
        else if ( state == State.MARKED_FAILED )
        {
            throw new ClientException(
                    "Cannot run more statements in this transaction, because previous statements in the " +
                    "transaction has failed and the transaction has been rolled back. Please start a new " +
                    "transaction to run another statement."
            );
        }
        else if ( state == State.TERMINATED )
        {
            throw new ClientException( "Cannot run more statements in this transaction, it has been terminated" );
        }
    }

    @Override
    public boolean isOpen()
    {
        return state != State.COMMITTED && state != State.ROLLED_BACK;
    }

    public void markTerminated()
    {
        state = State.TERMINATED;
    }

    public Bookmark bookmark()
    {
        return bookmark;
    }

    public void setBookmark( Bookmark bookmark )
    {
        if ( bookmark != null && !bookmark.isEmpty() )
        {
            this.bookmark = bookmark;
        }
    }

    private CompletionStage<Void> doCommitAsync()
    {
        if ( state == State.TERMINATED )
        {
            return failedFuture( new ClientException( "Can't commit, transaction has been terminated" ) );
        }

        CompletableFuture<Void> commitFuture = new CompletableFuture<>();
        ResponseHandler pullAllHandler = new CommitTxResponseHandler( commitFuture, this );
        connection.writeAndFlush(
                COMMIT_MESSAGE, NoOpResponseHandler.INSTANCE,
                PullAllMessage.PULL_ALL, pullAllHandler );
        return commitFuture;
    }

    private CompletionStage<Void> doRollbackAsync()
    {
        if ( state == State.TERMINATED )
        {
            return completedWithNull();
        }

        CompletableFuture<Void> rollbackFuture = new CompletableFuture<>();
        ResponseHandler pullAllHandler = new RollbackTxResponseHandler( rollbackFuture );
        connection.writeAndFlush(
                ROLLBACK_MESSAGE, NoOpResponseHandler.INSTANCE,
                PullAllMessage.PULL_ALL, pullAllHandler );
        return rollbackFuture;
    }

    private BiFunction<Void,Throwable,Void> handleCommitOrRollback( Throwable cursorFailure )
    {
        return ( ignore, commitOrRollbackError ) ->
        {
            CompletionException combinedError = Futures.combineErrors( cursorFailure, commitOrRollbackError );
            if ( combinedError != null )
            {
                throw combinedError;
            }
            return null;
        };
    }

    private void transactionClosed( State newState )
    {
        state = newState;
        connection.release(); // release in background
        session.setBookmark( bookmark );
    }

    private void terminateConnectionOnThreadInterrupt( String reason )
    {
        connection.terminateAndRelease( reason );
    }
}
