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
package org.neo4j.driver.react.result;

import org.reactivestreams.Subscription;

import java.util.List;
import java.util.concurrent.CompletableFuture;
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

    public RxStatementResultCursor( RunResponseHandler runHandler, BasicPullResponseHandler pullHandler )
    {
        this.runHandler = runHandler;
        this.pullHandler = pullHandler;
    }

    public CompletableFuture<List<String>> keys()
    {
        return runHandler.runFuture().thenApply( ignored -> runHandler.statementKeys() );
    }

    public void installSummaryConsumer( BiConsumer<ResultSummary,Throwable> successConsumer )
    {
        runHandler.runFuture().whenComplete( (ignored, error)-> {
            if ( error != null )
            {
                successConsumer.accept( null, error );
            }
        } );
        pullHandler.installSummaryConsumer( successConsumer );
    }

    public void installRecordConsumer( BiConsumer<Record, Throwable> recordConsumer )
    {
        runHandler.runFuture().whenComplete( (ignored, error)-> {
            if ( error != null )
            {
                recordConsumer.accept( null, error );
            }
        } );
        pullHandler.installRecordConsumer( recordConsumer );
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
        // TODO remove this method from reactive if not needed
        return Futures.completedWithNull();
    }
}
