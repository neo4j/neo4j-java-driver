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

import io.netty.util.concurrent.Promise;

import java.util.Collections;
import java.util.Map;

import org.neo4j.driver.internal.handlers.BeginTxResponseHandler;
import org.neo4j.driver.internal.handlers.CommitTxResponseHandler;
import org.neo4j.driver.internal.handlers.NoOpResponseHandler;
import org.neo4j.driver.internal.handlers.RollbackTxResponseHandler;
import org.neo4j.driver.internal.netty.AsyncConnection;
import org.neo4j.driver.internal.netty.InternalListenableFuture;
import org.neo4j.driver.internal.netty.InternalStatementResultCursor;
import org.neo4j.driver.internal.netty.ListenableFuture;
import org.neo4j.driver.internal.netty.StatementResultCursor;
import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.TypeSystem;

import static org.neo4j.driver.v1.Values.value;

public class AsyncExplicitTransaction implements Transaction
{
    private final SessionResourcesHandler resourcesHandler;
    private final AsyncConnection asyncConnection;

    private Bookmark bookmark = Bookmark.empty();

    public AsyncExplicitTransaction( AsyncConnection asyncConnection, SessionResourcesHandler resourcesHandler )
    {
        this.asyncConnection = asyncConnection;
        this.resourcesHandler = resourcesHandler;
    }

    public ListenableFuture<Transaction> beginAsync( Bookmark initialBookmark )
    {
        Map<String,Value> parameters = initialBookmark.asBeginTransactionParameters();

        Promise<Transaction> beginTxPromise = asyncConnection.newPromise();
        asyncConnection.run( "BEGIN", parameters, NoOpResponseHandler.INSTANCE );
        asyncConnection.pullAll( new BeginTxResponseHandler( beginTxPromise, this ) );
        asyncConnection.flush();

        return new InternalListenableFuture<>( beginTxPromise );
    }

    @Override
    public void success()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void failure()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<Void> commitAsync()
    {
        Promise<Void> commitTxPromise = asyncConnection.newPromise();
        asyncConnection.run( "COMMIT", Collections.<String,Value>emptyMap(), NoOpResponseHandler.INSTANCE );
        asyncConnection.pullAll( new CommitTxResponseHandler( commitTxPromise, resourcesHandler, this ) );
        asyncConnection.flush();

        return new InternalListenableFuture<>( commitTxPromise );
    }

    @Override
    public ListenableFuture<Void> rollbackAsync()
    {
        Promise<Void> rollbackTxPromise = asyncConnection.newPromise();
        asyncConnection.run( "ROLLBACK", Collections.<String,Value>emptyMap(), NoOpResponseHandler.INSTANCE );
        asyncConnection.pullAll( new RollbackTxResponseHandler( rollbackTxPromise, resourcesHandler, this ) );
        asyncConnection.flush();

        return new InternalListenableFuture<>( rollbackTxPromise );
    }

    @Override
    public StatementResult run( String statementText, Value statementParameters )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public StatementResultCursor runAsync( String statementText, Value parameters )
    {
        return runAsync( new Statement( statementText, parameters ) );
    }

    @Override
    public StatementResult run( String statementText )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public StatementResultCursor runAsync( String statementText )
    {
        return runAsync( statementText, Values.EmptyMap );
    }

    @Override
    public StatementResult run( String statementText, Map<String,Object> statementParameters )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public StatementResultCursor runAsync( String statementText, Map<String,Object> statementParameters )
    {
        Value params = statementParameters == null ? Values.EmptyMap : value( statementParameters );
        return runAsync( statementText, params );
    }

    @Override
    public StatementResult run( String statementTemplate, Record statementParameters )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public StatementResultCursor runAsync( String statementTemplate, Record statementParameters )
    {
        Value params = statementParameters == null ? Values.EmptyMap : value( statementParameters.asMap() );
        return runAsync( statementTemplate, params );
    }

    @Override
    public StatementResult run( Statement statement )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public StatementResultCursor runAsync( Statement statement )
    {
        InternalStatementResultCursor resultCursor =
                new InternalStatementResultCursor( asyncConnection, SessionResourcesHandler.NO_OP );

        String query = statement.text();
        Map<String,Value> params = statement.parameters().asMap( Values.ofValue() );

        asyncConnection.run( query, params, resultCursor.runResponseHandler() );
        asyncConnection.pullAll( resultCursor.pullAllResponseHandler() );
        asyncConnection.flush();

        return resultCursor;
    }

    @Override
    public boolean isOpen()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TypeSystem typeSystem()
    {
        return InternalTypeSystem.TYPE_SYSTEM;
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
