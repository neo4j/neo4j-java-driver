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
package org.neo4j.driver.internal.async;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.neo4j.driver.internal.handlers.PullAllResponseHandler;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.util.Consumer;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Response;
import org.neo4j.driver.v1.StatementResultCursor;
import org.neo4j.driver.v1.summary.ResultSummary;

import static java.util.Objects.requireNonNull;

public class InternalStatementResultCursor implements StatementResultCursor
{
    private final AsyncConnection connection;
    private final RunResponseHandler runResponseHandler;
    private final PullAllResponseHandler pullAllHandler;

    private InternalFuture<Record> peekedRecordResponse;

    public InternalStatementResultCursor( AsyncConnection connection, RunResponseHandler runResponseHandler,
            PullAllResponseHandler pullAllHandler )
    {
        this.connection = requireNonNull( connection );
        this.runResponseHandler = requireNonNull( runResponseHandler );
        this.pullAllHandler = requireNonNull( pullAllHandler );
    }

    @Override
    public List<String> keys()
    {
        List<String> keys = runResponseHandler.statementKeys();
        return keys == null ? Collections.<String>emptyList() : Collections.unmodifiableList( keys );
    }

    @Override
    public Response<ResultSummary> summaryAsync()
    {
        return pullAllHandler.summaryAsync();
    }

    @Override
    public Response<Record> nextAsync()
    {
        return internalNextAsync();
    }

    @Override
    public Response<Record> peekAsync()
    {
        if ( peekedRecordResponse == null )
        {
            peekedRecordResponse = pullAllHandler.nextAsync();
        }
        return peekedRecordResponse;
    }

    @Override
    public Response<Void> forEachAsync( final Consumer<Record> action )
    {
        InternalPromise<Void> result = connection.newPromise();
        internalForEachAsync( action, result );
        return result;
    }

    @Override
    public Response<List<Record>> listAsync()
    {
        InternalPromise<List<Record>> result = connection.newPromise();
        internalListAsync( new ArrayList<Record>(), result );
        return result;
    }

    private void internalForEachAsync( final Consumer<Record> action, final InternalPromise<Void> result )
    {
        final InternalFuture<Record> recordFuture = internalNextAsync();

        recordFuture.addListener( new FutureListener<Record>()
        {
            @Override
            public void operationComplete( Future<Record> future )
            {
                if ( future.isCancelled() )
                {
                    result.cancel( true );
                }
                else if ( future.isSuccess() )
                {
                    Record record = future.getNow();
                    if ( record != null )
                    {
                        action.accept( record );
                        internalForEachAsync( action, result );
                    }
                    else
                    {
                        result.setSuccess( null );
                    }
                }
                else
                {
                    result.setFailure( future.cause() );
                }
            }
        } );
    }

    private void internalListAsync( final List<Record> records, final InternalPromise<List<Record>> result )
    {
        final InternalFuture<Record> recordFuture = internalNextAsync();

        recordFuture.addListener( new FutureListener<Record>()
        {
            @Override
            public void operationComplete( Future<Record> future )
            {
                if ( future.isCancelled() )
                {
                    result.cancel( true );
                }
                else if ( future.isSuccess() )
                {
                    Record record = future.getNow();
                    if ( record != null )
                    {
                        records.add( record );
                        internalListAsync( records, result );
                    }
                    else
                    {
                        result.setSuccess( records );
                    }
                }
                else
                {
                    result.setFailure( future.cause() );
                }
            }
        } );
    }

    private InternalFuture<Record> internalNextAsync()
    {
        if ( peekedRecordResponse != null )
        {
            InternalFuture<Record> result = peekedRecordResponse;
            peekedRecordResponse = null;
            return result;
        }
        else
        {
            return pullAllHandler.nextAsync();
        }
    }
}
