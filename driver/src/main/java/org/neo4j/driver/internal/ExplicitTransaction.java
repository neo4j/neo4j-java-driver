/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;

import org.neo4j.driver.internal.async.ResultCursorsHolder;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.react.result.InternalStatementResultCursor;
import org.neo4j.driver.react.result.RxStatementResultCursor;
import org.neo4j.driver.react.result.StatementResultCursorFactory;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.StatementResultCursor;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionConfig;
import org.neo4j.driver.v1.exceptions.ClientException;

import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.internal.util.Futures.failedFuture;

public class ExplicitTransaction extends AbstractStatementRunner implements Transaction
{
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
    private final BoltProtocol protocol;
    private final NetworkSession session;
    private final ResultCursorsHolder resultCursors;

    private volatile State state = State.ACTIVE;

    public ExplicitTransaction( Connection connection, NetworkSession session )
    {
        this.connection = connection;
        this.protocol = connection.protocol();
        this.session = session;
        this.resultCursors = new ResultCursorsHolder();
    }

    public CompletionStage<ExplicitTransaction> beginAsync( Bookmarks initialBookmarks, TransactionConfig config )
    {
        return protocol.beginTransaction( connection, initialBookmarks, config )
                .handle( ( ignore, beginError ) ->
                {
                    if ( beginError != null )
                    {
                        // release connection if begin failed, transaction can't be started
                        connection.release();
                        throw Futures.asCompletionException( beginError );
                    }
                    return this;
                } );
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
                protocol.runInExplicitTransaction( connection, statement, this, waitForRunResponse )
                        .thenApply( StatementResultCursorFactory::asyncResult );
        resultCursors.add( cursorStage );
        return cursorStage;
    }

    public CompletionStage<RxStatementResultCursor> runRx( Statement statement )
    {
        ensureCanRunQueries();
        CompletionStage<RxStatementResultCursor> cursorStage =
                protocol.runInExplicitTransaction( connection, statement, this, false )
                        .thenApply( StatementResultCursorFactory::rxResult );
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
            throw new ClientException( "Cannot run more statements in this transaction, it has been marked for failure. " +
                                       "Please either rollback or close this transaction" );
        }
        else if ( state == State.TERMINATED )
        {
            throw new ClientException( "Cannot run more statements in this transaction, " +
                                       "it has either experienced an fatal error or was explicitly terminated" );
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

    private CompletionStage<Void> doCommitAsync()
    {
        if ( state == State.TERMINATED )
        {
            return failedFuture( new ClientException( "Transaction can't be committed. " +
                                                      "It has been rolled back either because of an error or explicit termination" ) );
        }
        return protocol.commitTransaction( connection )
                .thenApply( newBookmarks ->
                {
                    session.setBookmarks( newBookmarks );
                    return null;
                } );
    }

    private CompletionStage<Void> doRollbackAsync()
    {
        if ( state == State.TERMINATED )
        {
            return completedWithNull();
        }
        return protocol.rollbackTransaction( connection );
    }

    private static BiFunction<Void,Throwable,Void> handleCommitOrRollback( Throwable cursorFailure )
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
    }

    private void terminateConnectionOnThreadInterrupt( String reason )
    {
        connection.terminateAndRelease( reason );
    }
}
