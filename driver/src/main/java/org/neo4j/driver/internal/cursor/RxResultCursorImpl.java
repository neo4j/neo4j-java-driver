/*
 * Copyright (c) "Neo4j"
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

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

import org.neo4j.driver.Record;
import org.neo4j.driver.exceptions.TransactionNestingException;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.handlers.pulln.PullResponseHandler;
import org.neo4j.driver.summary.ResultSummary;

import static org.neo4j.driver.internal.cursor.RxResultCursorImpl.RecordConsumerStatus.DISCARD_INSTALLED;
import static org.neo4j.driver.internal.cursor.RxResultCursorImpl.RecordConsumerStatus.INSTALLED;
import static org.neo4j.driver.internal.cursor.RxResultCursorImpl.RecordConsumerStatus.NOT_INSTALLED;
import static org.neo4j.driver.internal.util.ErrorUtil.newResultConsumedError;

public class RxResultCursorImpl implements RxResultCursor
{
    static final BiConsumer<Record,Throwable> DISCARD_RECORD_CONSUMER = ( record, throwable ) -> {/*do nothing*/};
    private final RunResponseHandler runHandler;
    private final PullResponseHandler pullHandler;
    private final Throwable runResponseError;
    private final CompletableFuture<ResultSummary> summaryFuture = new CompletableFuture<>();
    private boolean resultConsumed;
    private RecordConsumerStatus consumerStatus = NOT_INSTALLED;

    public RxResultCursorImpl(RunResponseHandler runHandler, PullResponseHandler pullHandler )
    {
        this( null, runHandler, pullHandler );
    }

    public RxResultCursorImpl(Throwable runError, RunResponseHandler runHandler, PullResponseHandler pullHandler )
    {
        Objects.requireNonNull( runHandler );
        Objects.requireNonNull( pullHandler );
        assertRunResponseArrived( runHandler );

        this.runResponseError = runError;
        this.runHandler = runHandler;
        this.pullHandler = pullHandler;
        installSummaryConsumer();
    }

    @Override
    public List<String> keys()
    {
        return runHandler.queryKeys().keys();
    }

    @Override
    public void installRecordConsumer( BiConsumer<Record,Throwable> recordConsumer )
    {
        if ( resultConsumed )
        {
            throw newResultConsumedError();
        }

        if ( consumerStatus.isInstalled() )
        {
            return;
        }
        consumerStatus = recordConsumer == DISCARD_RECORD_CONSUMER ?
                         DISCARD_INSTALLED : INSTALLED;
        pullHandler.installRecordConsumer( recordConsumer );
        assertRunCompletedSuccessfully();
    }

    @Override
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
    public CompletionStage<Throwable> discardAllFailureAsync()
    {
        // calling this method will enforce discarding record stream and finish running cypher query
        return summaryAsync().thenApply( summary -> (Throwable) null ).exceptionally( error -> error );
    }

    @Override
    public CompletionStage<Throwable> pullAllFailureAsync()
    {
        if ( consumerStatus.isInstalled() && !isDone() )
        {
            return CompletableFuture.completedFuture( new TransactionNestingException(
                    "You cannot run another query or begin a new transaction in the same session before you've fully consumed the previous run result." ) );
        }
        // It is safe to discard records as either the streaming has not started at all, or the streaming is fully finished.
        return discardAllFailureAsync();
    }

    @Override
    public CompletionStage<ResultSummary> summaryAsync()
    {
        if ( !isDone() && !resultConsumed ) // the summary is called before record streaming
        {
            installRecordConsumer( DISCARD_RECORD_CONSUMER );
            cancel();
            resultConsumed = true;
        }
        return this.summaryFuture;
    }

    @Override
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
            if ( error != null && consumerStatus.isDiscardConsumer() )
            {
                // We will only report the error to summary if there is no user record consumer installed
                // When a user record consumer is installed, the error will be reported to record consumer instead.
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

    enum RecordConsumerStatus
    {
        NOT_INSTALLED( false, false ),
        INSTALLED( true, false ),
        DISCARD_INSTALLED( true, true );

        private final boolean isInstalled;
        private final boolean isDiscardConsumer;

        RecordConsumerStatus( boolean isInstalled, boolean isDiscardConsumer )
        {
            this.isInstalled = isInstalled;
            this.isDiscardConsumer = isDiscardConsumer;
        }

        boolean isInstalled()
        {
            return isInstalled;
        }

        boolean isDiscardConsumer()
        {
            return isDiscardConsumer;
        }
    }
}
