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
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import org.neo4j.driver.internal.async.QueryRunner;
import org.neo4j.driver.internal.async.ResultCursorsHolder;
import org.neo4j.driver.internal.handlers.BeginTxResponseHandler;
import org.neo4j.driver.internal.handlers.CommitTxResponseHandler;
import org.neo4j.driver.internal.handlers.NoOpResponseHandler;
import org.neo4j.driver.internal.handlers.RollbackTxResponseHandler;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.StatementResultCursor;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.types.TypeSystem;

import static java.util.Collections.emptyMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.internal.util.Futures.failedFuture;
import static org.neo4j.driver.v1.Values.value;

public class ExplicitTransaction implements Transaction
{
    private static final String BEGIN_QUERY = "BEGIN";
    private static final String COMMIT_QUERY = "COMMIT";
    private static final String ROLLBACK_QUERY = "ROLLBACK";

    private enum State
    {
        /** The transaction is running with no explicit success or failure marked */
        ACTIVE( true ),

        /** Running, user marked for success, meaning it'll value committed */
        MARKED_SUCCESS( true ),

        /** User marked as failed, meaning it'll be rolled back. */
        MARKED_FAILED( true ),

        /**
         * This transaction has been explicitly terminated by calling {@link Session#reset()}.
         */
        TERMINATED( false ),

        /** This transaction has successfully committed */
        COMMITTED( false ),

        /** This transaction has been rolled back */
        ROLLED_BACK( false );

        final boolean txOpen;

        State( boolean txOpen )
        {
            this.txOpen = txOpen;
        }
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
            connection.run( BEGIN_QUERY, emptyMap(), NoOpResponseHandler.INSTANCE, NoOpResponseHandler.INSTANCE );
            return completedFuture( this );
        }
        else
        {
            CompletableFuture<ExplicitTransaction> beginFuture = new CompletableFuture<>();
            connection.runAndFlush( BEGIN_QUERY, initialBookmark.asBeginTransactionParameters(),
                    NoOpResponseHandler.INSTANCE, new BeginTxResponseHandler<>( beginFuture, this ) );

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
        else if ( state == State.ACTIVE || state == State.MARKED_FAILED || state == State.TERMINATED )
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
        else if ( state == State.TERMINATED )
        {
            return failedFuture(
                    new ClientException( "Can't commit, transaction has been terminated by `Session#reset()`" ) );
        }
        else
        {
            return resultCursors.retrieveNotConsumedError()
                    .thenCompose( error -> doCommitAsync().handle( handleCommitOrRollback( error ) ) )
                    .whenComplete( transactionClosed( State.COMMITTED ) );
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
        else if ( state == State.TERMINATED )
        {
            // transaction has been terminated by RESET and should be rolled back by the database
            state = State.ROLLED_BACK;
            return completedWithNull();
        }
        else
        {
            return resultCursors.retrieveNotConsumedError()
                    .thenCompose( error -> doRollbackAsync().handle( handleCommitOrRollback( error ) ) )
                    .whenComplete( transactionClosed( State.ROLLED_BACK ) );
        }
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
    public StatementResult run( String statementText )
    {
        return run( statementText, Values.EmptyMap );
    }

    @Override
    public CompletionStage<StatementResultCursor> runAsync( String statementTemplate )
    {
        return runAsync( statementTemplate, Values.EmptyMap );
    }

    @Override
    public StatementResult run( String statementText, Map<String,Object> statementParameters )
    {
        Value params = statementParameters == null ? Values.EmptyMap : value( statementParameters );
        return run( statementText, params );
    }

    @Override
    public CompletionStage<StatementResultCursor> runAsync( String statementTemplate,
            Map<String,Object> statementParameters )
    {
        Value params = statementParameters == null ? Values.EmptyMap : value( statementParameters );
        return runAsync( statementTemplate, params );
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
                QueryRunner.runInTransaction( connection, statement,
                        this, waitForRunResponse );
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
            throw new ClientException(
                    "Cannot run more statements in this transaction, it has been terminated by `Session#reset()`" );
        }
    }

    @Override
    public boolean isOpen()
    {
        return state.txOpen;
    }

    @Override
    public TypeSystem typeSystem()
    {
        return InternalTypeSystem.TYPE_SYSTEM;
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
        CompletableFuture<Void> commitFuture = new CompletableFuture<>();
        ResponseHandler pullAllHandler = new CommitTxResponseHandler( commitFuture, this );
        connection.runAndFlush( COMMIT_QUERY, emptyMap(), NoOpResponseHandler.INSTANCE, pullAllHandler );
        return commitFuture;
    }

    private CompletionStage<Void> doRollbackAsync()
    {
        CompletableFuture<Void> rollbackFuture = new CompletableFuture<>();
        ResponseHandler pullAllHandler = new RollbackTxResponseHandler( rollbackFuture );
        connection.runAndFlush( ROLLBACK_QUERY, emptyMap(), NoOpResponseHandler.INSTANCE, pullAllHandler );
        return rollbackFuture;
    }

    private BiFunction<Void,Throwable,Void> handleCommitOrRollback( Throwable cursorFailure )
    {
        return ( ignore, commitOrRollbackError ) ->
        {
            if ( cursorFailure != null && commitOrRollbackError != null )
            {
                Throwable cause1 = Futures.completionExceptionCause( cursorFailure );
                Throwable cause2 = Futures.completionExceptionCause( commitOrRollbackError );
                if ( cause1 != cause2 )
                {
                    cause1.addSuppressed( cause2 );
                }
                throw Futures.asCompletionException( cause1 );
            }
            else if ( cursorFailure != null )
            {
                throw Futures.asCompletionException( cursorFailure );
            }
            else if ( commitOrRollbackError != null )
            {
                throw Futures.asCompletionException( commitOrRollbackError );
            }
            else
            {
                return null;
            }
        };
    }

    private BiConsumer<Object,Throwable> transactionClosed( State newState )
    {
        return ( ignore, error ) ->
        {
            state = newState;
            connection.release(); // release in background
            session.setBookmark( bookmark );
        };
    }

    private void terminateConnectionOnThreadInterrupt( String reason )
    {
        connection.terminateAndRelease( reason );
    }
}
