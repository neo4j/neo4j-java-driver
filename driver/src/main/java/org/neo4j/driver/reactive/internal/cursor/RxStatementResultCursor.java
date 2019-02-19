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
package org.neo4j.driver.reactive.internal.cursor;

import org.reactivestreams.Subscription;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

import org.neo4j.driver.internal.FailableCursor;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.handlers.pulln.BasicPullResponseHandler;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.summary.ResultSummary;

public class RxStatementResultCursor implements Subscription, FailableCursor
{
    private final RunResponseHandler runHandler;
    private final BasicPullResponseHandler pullHandler;
    private final Throwable runResponseError;

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
    }

    private void assertRunResponseArrived( RunResponseHandler runHandler )
    {
        if ( !runHandler.runFuture().isDone() )
        {
            throw new IllegalStateException( "Should wait for response of RUN before allowing PULL_N." );
        }
    }

    public synchronized List<String> keys()
    {
        return runHandler.statementKeys();
    }

    public synchronized void installSummaryConsumer( BiConsumer<ResultSummary,Throwable> summaryConsumer )
    {
        pullHandler.installSummaryConsumer( summaryConsumer );
    }

    public synchronized void installRecordConsumer( BiConsumer<Record,Throwable> recordConsumer )
    {
        pullHandler.installRecordConsumer( recordConsumer );
        assertRunCompletedSuccessfully();
    }

    public synchronized void request( long n )
    {
        pullHandler.request( n );
    }

    @Override
    public synchronized void cancel()
    {
        pullHandler.cancel();
    }

    @Override
    public synchronized CompletionStage<Throwable> failureAsync()
    {
        // TODO need to discuss with session.close/result.keys/result.summary
        // TODO shall this method serve as another place that we can enforce termination of streaming?
        // TODO shall this method canceling the streaming and enforce draining of all messages?
        // If we do not have result.keys and result.summary as publishers, then we shall never need to enforce streaming.
        // If we have a session.close, then we shall fully implement this in some way as this is used by session to sync.
        return Futures.completedWithNull();
    }

    private void assertRunCompletedSuccessfully()
    {
        if ( runResponseError != null )
        {
            pullHandler.onFailure( runResponseError );
        }
    }
}
