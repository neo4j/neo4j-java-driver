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

import java.util.Collections;
import java.util.Map;

import org.neo4j.driver.ResultResourcesHandler;
import org.neo4j.driver.internal.async.AsyncConnection;
import org.neo4j.driver.internal.async.InternalFuture;
import org.neo4j.driver.internal.async.InternalPromise;
import org.neo4j.driver.internal.async.QueryRunner;
import org.neo4j.driver.internal.async.StatementResultCursor;
import org.neo4j.driver.internal.async.Task;
import org.neo4j.driver.internal.handlers.BeginTxResponseHandler;
import org.neo4j.driver.internal.handlers.BookmarkResponseHandler;
import org.neo4j.driver.internal.handlers.CommitTxResponseHandler;
import org.neo4j.driver.internal.handlers.NoOpResponseHandler;
import org.neo4j.driver.internal.handlers.RollbackTxResponseHandler;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.Neo4jException;
import org.neo4j.driver.v1.types.TypeSystem;
import org.neo4j.driver.v1.util.Function;

import static org.neo4j.driver.internal.util.ErrorUtil.isRecoverable;
import static org.neo4j.driver.v1.Values.ofValue;
import static org.neo4j.driver.v1.Values.value;

public class ExplicitTransaction implements Transaction, ResultResourcesHandler
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
         * An error has occurred, transaction can no longer be used and no more messages will be sent for this
         * transaction.
         */
        FAILED,

        /** This transaction has successfully committed */
        COMMITTED,

        /** This transaction has been rolled back */
        ROLLED_BACK
    }

    private final SessionResourcesHandler resourcesHandler;
    private final Connection connection;
    private final AsyncConnection asyncConnection;
    private final NetworkSession session;

    private volatile Bookmark bookmark = Bookmark.empty();
    private volatile State state = State.ACTIVE;

    public ExplicitTransaction( Connection connection, SessionResourcesHandler resourcesHandler )
    {
        this.connection = connection;
        this.asyncConnection = null;
        this.session = null;
        this.resourcesHandler = resourcesHandler;
    }

    public ExplicitTransaction( AsyncConnection asyncConnection, NetworkSession session )
    {
        this.connection = null;
        this.asyncConnection = asyncConnection;
        this.session = session;
        this.resourcesHandler = SessionResourcesHandler.NO_OP;
    }

    public void begin( Bookmark initialBookmark )
    {
        Map<String,Value> parameters = initialBookmark.asBeginTransactionParameters();

        connection.run( "BEGIN", parameters, NoOpResponseHandler.INSTANCE );
        connection.pullAll( NoOpResponseHandler.INSTANCE );

        if ( !initialBookmark.isEmpty() )
        {
            connection.sync();
        }
    }

    public InternalFuture<ExplicitTransaction> beginAsync( Bookmark initialBookmark )
    {
        InternalPromise<ExplicitTransaction> beginTxPromise = asyncConnection.newPromise();

        Map<String,Value> parameters = initialBookmark.asBeginTransactionParameters();
        asyncConnection.run( "BEGIN", parameters, NoOpResponseHandler.INSTANCE );

        if ( initialBookmark.isEmpty() )
        {
            asyncConnection.pullAll( NoOpResponseHandler.INSTANCE );
            beginTxPromise.setSuccess( this );
        }
        else
        {
            asyncConnection.pullAll( new BeginTxResponseHandler<>( beginTxPromise, this ) );
            asyncConnection.flush();
        }

        return beginTxPromise;
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
        try
        {
            if ( connection != null && connection.isOpen() )
            {
                if ( state == State.MARKED_SUCCESS )
                {
                    try
                    {
                        connection.run( "COMMIT", Collections.<String,Value>emptyMap(), NoOpResponseHandler.INSTANCE );
                        connection.pullAll( new BookmarkResponseHandler( this ) );
                        connection.sync();
                        state = State.COMMITTED;
                    }
                    catch ( Throwable e )
                    {
                        // failed to commit
                        try
                        {
                            rollbackTx();
                        }
                        catch ( Throwable ignored )
                        {
                            // best effort.
                        }
                        throw e;
                    }
                }
                else if ( state == State.MARKED_FAILED || state == State.ACTIVE )
                {
                    rollbackTx();
                }
                else if ( state == State.FAILED )
                {
                    // unrecoverable error happened, transaction should've been rolled back on the server
                    // update state so that this transaction does not remain open
                    state = State.ROLLED_BACK;
                }
            }
        }
        finally
        {
            resourcesHandler.onTransactionClosed( this );
        }
    }

    private void rollbackTx()
    {
        connection.run( "ROLLBACK", Collections.<String,Value>emptyMap(), NoOpResponseHandler.INSTANCE );
        connection.pullAll( new BookmarkResponseHandler( this ) );
        connection.sync();
        state = State.ROLLED_BACK;
    }

    @Override
    public Task<Void> commitAsync()
    {
        return internalCommitAsync().asTask();
    }

    InternalFuture<Void> internalCommitAsync()
    {
        if ( state == State.COMMITTED )
        {
            return asyncConnection.<Void>newPromise().succeeded( null );
        }
        else if ( state == State.ROLLED_BACK )
        {
            return asyncConnection.<Void>newPromise().failed(
                    new ClientException( "Can't commit, transaction has already been rolled back" ) );
        }
        else
        {
            return doCommitAsync().whenComplete( releaseConnectionAndNotifySession() );
        }
    }

    @Override
    public Task<Void> rollbackAsync()
    {
        return internalRollbackAsync().asTask();
    }

    InternalFuture<Void> internalRollbackAsync()
    {
        if ( state == State.COMMITTED )
        {
            return asyncConnection.<Void>newPromise()
                    .failed( new ClientException( "Can't rollback, transaction has already been committed" ) );
        }
        else if ( state == State.ROLLED_BACK )
        {
            return asyncConnection.<Void>newPromise().succeeded( null );
        }
        else
        {
            return doRollbackAsync().whenComplete( releaseConnectionAndNotifySession() );
        }
    }

    private Runnable releaseConnectionAndNotifySession()
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                asyncConnection.release();
                session.asyncTransactionClosed( ExplicitTransaction.this );
            }
        };
    }

    private InternalFuture<Void> doCommitAsync()
    {
        InternalPromise<Void> commitTxPromise = asyncConnection.newPromise();

        asyncConnection.run( "COMMIT", Collections.<String,Value>emptyMap(), NoOpResponseHandler.INSTANCE );
        asyncConnection.pullAll( new CommitTxResponseHandler( commitTxPromise, this ) );
        asyncConnection.flush();

        return commitTxPromise.thenApply( new Function<Void,Void>()
        {
            @Override
            public Void apply( Void ignore )
            {
                ExplicitTransaction.this.state = State.COMMITTED;
                return null;
            }
        } );
    }

    private InternalFuture<Void> doRollbackAsync()
    {
        InternalPromise<Void> rollbackTxPromise = asyncConnection.newPromise();
        asyncConnection.run( "ROLLBACK", Collections.<String,Value>emptyMap(), NoOpResponseHandler.INSTANCE );
        asyncConnection.pullAll( new RollbackTxResponseHandler( rollbackTxPromise ) );
        asyncConnection.flush();

        return rollbackTxPromise.thenApply( new Function<Void,Void>()
        {
            @Override
            public Void apply( Void ignore )
            {
                ExplicitTransaction.this.state = State.ROLLED_BACK;
                return null;
            }
        } );
    }

    @Override
    public StatementResult run( String statementText, Value statementParameters )
    {
        return run( new Statement( statementText, statementParameters ) );
    }

    @Override
    public Task<StatementResultCursor> runAsync( String statementText, Value parameters )
    {
        return runAsync( new Statement( statementText, parameters ) );
    }

    @Override
    public StatementResult run( String statementText )
    {
        return run( statementText, Values.EmptyMap );
    }

    @Override
    public Task<StatementResultCursor> runAsync( String statementTemplate )
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
    public Task<StatementResultCursor> runAsync( String statementTemplate, Map<String,Object> statementParameters )
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
    public Task<StatementResultCursor> runAsync( String statementTemplate, Record statementParameters )
    {
        Value params = statementParameters == null ? Values.EmptyMap : value( statementParameters.asMap() );
        return runAsync( statementTemplate, params );
    }

    @Override
    public StatementResult run( Statement statement )
    {
        ensureNotFailed();

        try
        {
            InternalStatementResult result =
                    new InternalStatementResult( statement, connection, ResultResourcesHandler.NO_OP );
            connection.run( statement.text(),
                    statement.parameters().asMap( ofValue() ),
                    result.runResponseHandler() );
            connection.pullAll( result.pullAllResponseHandler() );
            connection.flush();
            return result;
        }
        catch ( Neo4jException e )
        {
            // Failed to send messages to the server probably due to IOException in the socket.
            // So we should stop sending more messages in this transaction
            state = State.FAILED;
            throw e;
        }
    }

    @Override
    public Task<StatementResultCursor> runAsync( Statement statement )
    {
        ensureNotFailed();
        return QueryRunner.runAsync( asyncConnection, statement, this ).asTask();
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

    @Override
    public void resultFetched()
    {
        // no resources to release when result is fully fetched
    }

    @Override
    public void resultFailed( Throwable error )
    {
        // RUN failed, this transaction should not commit
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
