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
package org.neo4j.driver.internal.cursor;

import org.reactivestreams.Subscription;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

import org.neo4j.driver.internal.FailableCursor;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.handlers.pulln.BasicPullResponseHandler;
import org.neo4j.driver.Record;
import org.neo4j.driver.summary.ResultSummary;

import static org.neo4j.driver.internal.handlers.pulln.AbstractBasicPullResponseHandler.DISCARD_RECORD_CONSUMER;

public class RxStatementResultCursor implements Subscription, FailableCursor
{
    private final RunResponseHandler runHandler;
    private final BasicPullResponseHandler pullHandler;
    private final Throwable runResponseError;
    private final CompletableFuture<ResultSummary> summaryFuture = new CompletableFuture<>();
    boolean isRecordHandlerInstalled = false;

    public RxStatementResultCursor( RunResponseHandler runHandler, BasicPullResponseHandler pullHandler )
    {
        this( null, runHandler, pullHandler );
    }

    public RxStatementResultCursor( Throwable runError, RunResponseHandler runHandler, BasicPullResponseHandler pullHandler )
    {
        Objects.requireNonNull( runHandler );
        Objects.requireNonNull( pullHandler );
        assertRunResponseArrived( runHandler );

        this.runResponseError = runError;
        this.runHandler = runHandler;
        this.pullHandler = pullHandler;
        installSummaryConsumer();
    }

    public List<String> keys()
    {
        return runHandler.statementKeys();
    }

    public void installRecordConsumer( BiConsumer<Record,Throwable> recordConsumer )
    {
        if ( isRecordHandlerInstalled )
        {
            return;
        }
        isRecordHandlerInstalled = true;
        pullHandler.installRecordConsumer( recordConsumer );
        assertRunCompletedSuccessfully();
    }

    public void request( long n )
    {
        pullHandler.request( n );
    }

    @Override
    public void cancel()
    {
        pullHandler.cancel();
    }

    @Override
    public CompletionStage<Throwable> failureAsync()
    {
        // calling this method will enforce discarding record stream and finish running cypher query
        return summaryAsync().thenApply( summary -> (Throwable) null ).exceptionally( error -> error );
    }

    public CompletionStage<ResultSummary> summaryAsync()
    {
        if ( !isDone() ) // the summary is called before record streaming
        {
            installRecordConsumer( DISCARD_RECORD_CONSUMER );
            cancel();
        }

        return this.summaryFuture;
    }

    public boolean isDone()
    {
        return summaryFuture.isDone();
    }

    private void assertRunCompletedSuccessfully()
    {
        if ( runResponseError != null )
        {
            pullHandler.onFailure( runResponseError );
        }
    }

    private void installSummaryConsumer()
    {
        pullHandler.installSummaryConsumer( ( summary, error ) -> {
            if ( error != null )
            {
                summaryFuture.completeExceptionally( error );
            }
            else if ( summary != null )
            {
                summaryFuture.complete( summary );
            }
            //else (null, null) to indicate a has_more success
        } );
    }

    private void assertRunResponseArrived( RunResponseHandler runHandler )
    {
        if ( !runHandler.runFuture().isDone() )
        {
            throw new IllegalStateException( "Should wait for response of RUN before allowing PULL." );
        }
    }
}
