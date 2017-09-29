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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Function;

import org.neo4j.driver.internal.handlers.PullAllResponseHandler;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResultCursor;
import org.neo4j.driver.v1.exceptions.NoSuchRecordException;
import org.neo4j.driver.v1.summary.ResultSummary;

import static java.util.Objects.requireNonNull;

public class InternalStatementResultCursor implements StatementResultCursor
{
    private final RunResponseHandler runResponseHandler;
    private final PullAllResponseHandler pullAllHandler;

    private CompletionStage<Record> peekedRecordFuture;

    public InternalStatementResultCursor( RunResponseHandler runResponseHandler, PullAllResponseHandler pullAllHandler )
    {
        this.runResponseHandler = requireNonNull( runResponseHandler );
        this.pullAllHandler = requireNonNull( pullAllHandler );
    }

    @Override
    public List<String> keys()
    {
        List<String> keys = runResponseHandler.statementKeys();
        return keys == null ? Collections.emptyList() : Collections.unmodifiableList( keys );
    }

    @Override
    public CompletionStage<ResultSummary> summaryAsync()
    {
        return pullAllHandler.summaryAsync();
    }

    @Override
    public CompletionStage<Record> nextAsync()
    {
        if ( peekedRecordFuture != null )
        {
            CompletionStage<Record> result = peekedRecordFuture;
            peekedRecordFuture = null;
            return result;
        }
        else
        {
            return pullAllHandler.nextAsync();
        }
    }

    @Override
    public CompletionStage<Record> peekAsync()
    {
        if ( peekedRecordFuture == null )
        {
            peekedRecordFuture = pullAllHandler.nextAsync();
        }
        return peekedRecordFuture;
    }

    @Override
    public CompletionStage<Record> singleAsync()
    {
        return nextAsync().thenCompose( firstRecord ->
        {
            if ( firstRecord == null )
            {
                throw new NoSuchRecordException( "Cannot retrieve a single record, because this cursor is empty." );
            }
            return nextAsync().thenApply( secondRecord ->
            {
                if ( secondRecord != null )
                {
                    throw new NoSuchRecordException( "Expected a cursor with a single record, but this cursor " +
                                                     "contains at least one more. Ensure your query returns only " +
                                                     "one record." );
                }
                return firstRecord;
            } );
        } );
    }

    @Override
    public CompletionStage<ResultSummary> consumeAsync()
    {
        return forEachAsync( record ->
        {
        } );
    }

    @Override
    public CompletionStage<ResultSummary> forEachAsync( Consumer<Record> action )
    {
        CompletableFuture<Void> resultFuture = new CompletableFuture<>();
        internalForEachAsync( action, resultFuture );
        return resultFuture.thenCompose( ignore -> summaryAsync() );
    }

    @Override
    public CompletionStage<List<Record>> listAsync()
    {
        return listAsync( Function.identity() );
    }

    @Override
    public <T> CompletionStage<List<T>> listAsync( Function<Record,T> mapFunction )
    {
        CompletableFuture<List<T>> resultFuture = new CompletableFuture<>();
        internalListAsync( new ArrayList<>(), resultFuture, mapFunction );
        return resultFuture;
    }

    private void internalForEachAsync( Consumer<Record> action, CompletableFuture<Void> resultFuture )
    {
        CompletionStage<Record> recordFuture = nextAsync();

        // use async completion listener because of recursion, otherwise it is possible for
        // the caller thread to get StackOverflowError when result is large and buffered
        recordFuture.whenCompleteAsync( ( record, error ) ->
        {
            if ( error != null )
            {
                resultFuture.completeExceptionally( error );
            }
            else if ( record != null )
            {
                try
                {
                    action.accept( record );
                }
                catch ( Throwable actionError )
                {
                    resultFuture.completeExceptionally( actionError );
                    return;
                }
                internalForEachAsync( action, resultFuture );
            }
            else
            {
                resultFuture.complete( null );
            }
        } );
    }

    private <T> void internalListAsync( List<T> result, CompletableFuture<List<T>> resultFuture,
            Function<Record,T> mapFunction )
    {
        CompletionStage<Record> recordFuture = nextAsync();

        // use async completion listener because of recursion, otherwise it is possible for
        // the caller thread to get StackOverflowError when result is large and buffered
        recordFuture.whenCompleteAsync( ( record, error ) ->
        {
            if ( error != null )
            {
                resultFuture.completeExceptionally( error );
            }
            else if ( record != null )
            {
                T value;
                try
                {
                    value = mapFunction.apply( record );
                }
                catch ( Throwable mapError )
                {
                    resultFuture.completeExceptionally( mapError );
                    return;
                }
                result.add( value );
                internalListAsync( result, resultFuture, mapFunction );
            }
            else
            {
                resultFuture.complete( result );
            }
        } );
    }
}
