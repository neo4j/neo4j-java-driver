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

import org.neo4j.driver.internal.async.AsyncConnection;
import org.neo4j.driver.internal.async.QueryRunner;
import org.neo4j.driver.internal.handlers.BeginTxResponseHandler;
import org.neo4j.driver.internal.handlers.CommitTxResponseHandler;
import org.neo4j.driver.internal.handlers.NoOpResponseHandler;
import org.neo4j.driver.internal.handlers.RollbackTxResponseHandler;
import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.v1.Record;
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
import static org.neo4j.driver.internal.async.Futures.failedFuture;
import static org.neo4j.driver.internal.async.Futures.getBlocking;
import static org.neo4j.driver.internal.util.ErrorUtil.isRecoverable;
import static org.neo4j.driver.v1.Values.value;

public class ExplicitTransaction implements Transaction
{
    private static final String BEGIN_QUERY = "BEGIN";
    private static final String COMMIT_QUERY = "COMMIT";
    private static final String ROLLBACK_QUERY = "ROLLBACK";

    private enum State
    {
        /** The transaction is running with no explicit success or failure marked */
        ACTIVE,

        /** Running, user marked for success, meaning it'll value committed */
        MARKED_SUCCESS,

        /** User marked as failed, meaning it'll be rolled back. */
        MARKED_FAILED,

        /**
         * An error has occurred, transaction can no longer be used and no more messages will be sent for this
         * transaction.
         */
        FAILED,

        /** This transaction has successfully committed */
        COMMITTED,

        /** This transaction has been rolled back */
        ROLLED_BACK
    }

    private final AsyncConnection connection;
    private final NetworkSession session;

    private volatile Bookmark bookmark = Bookmark.empty();
    private volatile State state = State.ACTIVE;

    public ExplicitTransaction( AsyncConnection connection, NetworkSession session )
    {
        this.connection = connection;
        this.session = session;
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
            return beginFuture;
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
        if ( state == State.MARKED_SUCCESS )
        {
            getBlocking( commitAsync() );
        }
        else if ( state == State.MARKED_FAILED || state == State.ACTIVE )
        {
            getBlocking( rollbackAsync() );
        }
        else if ( state == State.FAILED )
        {
            // unrecoverable error happened, transaction should've been rolled back on the server
            // update state so that this transaction does not remain open
            state = State.ROLLED_BACK;
        }
    }

    @Override
    public CompletionStage<Void> commitAsync()
    {
        if ( state == State.COMMITTED )
        {
            return completedFuture( null );
        }
        else if ( state == State.ROLLED_BACK )
        {
            return failedFuture( new ClientException( "Can't commit, transaction has already been rolled back" ) );
        }
        else
        {
            return doCommitAsync().whenComplete( releaseConnectionAndNotifySession() );
        }
    }

    @Override
    public CompletionStage<Void> rollbackAsync()
    {
        if ( state == State.COMMITTED )
        {
            return failedFuture( new ClientException( "Can't rollback, transaction has already been committed" ) );
        }
        else if ( state == State.ROLLED_BACK )
        {
            return completedFuture( null );
        }
        else
        {
            return doRollbackAsync().whenComplete( releaseConnectionAndNotifySession() );
        }
    }

    private BiConsumer<Void,Throwable> releaseConnectionAndNotifySession()
    {
        return ( ignore, error ) ->
        {
            connection.release();
            session.asyncTransactionClosed( ExplicitTransaction.this );
        };
    }

    private CompletionStage<Void> doCommitAsync()
    {
        CompletableFuture<Void> commitFuture = new CompletableFuture<>();
        connection.runAndFlush( COMMIT_QUERY, emptyMap(), NoOpResponseHandler.INSTANCE,
                new CommitTxResponseHandler( commitFuture, this ) );

        return commitFuture.thenApply( ignore ->
        {
            ExplicitTransaction.this.state = State.COMMITTED;
            return null;
        } );
    }

    private CompletionStage<Void> doRollbackAsync()
    {
        CompletableFuture<Void> rollbackFuture = new CompletableFuture<>();
        connection.runAndFlush( ROLLBACK_QUERY, emptyMap(), NoOpResponseHandler.INSTANCE,
                new RollbackTxResponseHandler( rollbackFuture ) );

        return rollbackFuture.thenApply( ignore ->
        {
            ExplicitTransaction.this.state = State.ROLLED_BACK;
            return null;
        } );
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
        ensureNotFailed();
        StatementResultCursor cursor = getBlocking( QueryRunner.runSync( connection, statement, this ) );
        return new CursorBasedStatementResult( cursor );
    }

    @Override
    public CompletionStage<StatementResultCursor> runAsync( Statement statement )
    {
        ensureNotFailed();
        return QueryRunner.runAsync( connection, statement, this );
    }

    @Override
    public boolean isOpen()
    {
        return state != State.COMMITTED && state != State.ROLLED_BACK;
    }

    private void ensureNotFailed()
    {
        if ( state == State.FAILED || state == State.MARKED_FAILED || state == State.ROLLED_BACK )
        {
            throw new ClientException(
                    "Cannot run more statements in this transaction, because previous statements in the " +
                    "transaction has failed and the transaction has been rolled back. Please start a new " +
                    "transaction to run another statement."
            );
        }
    }

    @Override
    public TypeSystem typeSystem()
    {
        return InternalTypeSystem.TYPE_SYSTEM;
    }

    public void resultFailed( Throwable error )
    {
        if ( isRecoverable( error ) )
        {
            failure();
        }
        else
        {
            markToClose();
        }
    }

    public void markToClose()
    {
        state = State.FAILED;
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
}
