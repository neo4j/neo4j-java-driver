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
package org.neo4j.driver.internal.handlers.pulln;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.summary.ResultSummary;

import static java.util.Objects.requireNonNull;

public class AsyncPullResponseHandler implements PullResponseHandler
{
    private final Queue<Record> records = new LinkedList<>();
    private CompletableFuture<Record> recordFuture;

    private final BasicPullResponseHandler delegate;
    private final PullController controller;

    private ResultSummary summary;
    private CompletableFuture<Throwable> failureFuture = new CompletableFuture<>();

    public AsyncPullResponseHandler( BasicPullResponseHandler handler )
    {
        requireNonNull( handler );
        // First finish init handler
        handler.installSummaryConsumer( this::consumeSummary );
        handler.installRecordConsumer( this::consumeRecord );

        // Then they are good to be used
        this.delegate = handler;
        this.controller = new AutoPullController( handler );
    }

    @Override
    public synchronized Queue<Record> queue()
    {
        return records;
    }

    @Override
    public synchronized void request( long n )
    {
        delegate.request( n );
    }

    @Override
    public synchronized void cancel()
    {
        delegate.cancel();
    }

    @Override
    public synchronized CompletableFuture<Record> nextRecord()
    {
        return peekRecord().thenApply( ignored -> {
            Record record = records.poll();
            controller.afterDequeueRecord( records.size(), delegate.isStreamingPaused() );
            return record;
        } );
    }

    @Override
    public synchronized CompletableFuture<Record> peekRecord()
    {
        if ( recordFuture != null )
        {
            return recordFuture;
        }

        recordFuture = new CompletableFuture<>();
        if ( delegate.isFinishedOrCanceled() && records.size() == 0 ) // if no more record
        {
            CompletableFuture<Record> ret = recordFuture;
            completeRecordFuture( null );
            return ret;
        }
        else
        {
            CompletableFuture<Record> ret = recordFuture;
            Record record = records.peek();
            if ( record != null ) // peeked
            {
                recordFuture = null;
                ret.complete( record );
            }
            // else will be filled once the record arrives
            return ret;
        }
    }

    @Override
    public synchronized CompletableFuture<ResultSummary> summary()
    {
        CompletableFuture<ResultSummary> summaryFuture = new CompletableFuture<>();
        failureFuture.thenAccept( error -> {
            if ( error != null )
            {
                consumeFinishFuture();
                summaryFuture.completeExceptionally( error );
            }
            else
            {
                summaryFuture.complete( summary );
            }
        } );
        return summaryFuture;
    }

    synchronized void consumeRecord( Record record, Throwable throwable )
    {
        if ( record != null )
        {
            records.add( record );
            completeRecordFuture( record );
            controller.afterEnqueueRecord( record.size() );
        }
        else if ( throwable != null )
        {
            failureFuture.complete( throwable );
            failRecordFuture( throwable );
        }
        else
        {
            // done streaming
            failureFuture.complete( null );
            completeRecordFuture( null );
        }
    }

    synchronized void consumeSummary( ResultSummary summary, Throwable throwable )
    {
        if ( summary != null )
        {
            this.summary = summary;
        }
        if ( summary == null && throwable == null )
        {
            // has_more
            controller.handleSuccessWithHasMore();
        }
    }

    private synchronized void failRecordFuture( Throwable error )
    {
        if ( recordFuture != null )
        {
            CompletableFuture<Record> future = recordFuture;
            recordFuture = null;
            consumeFinishFuture();
            future.completeExceptionally( error );
        }
    }

    private synchronized void consumeFinishFuture()
    {
        failureFuture = Futures.completedWithNull();
    }

    private synchronized void completeRecordFuture( Record record )
    {
        if ( record == null ) // streaming is finished
        {
            failureFuture.thenAccept( error -> {
                if ( error != null )
                {
                    failRecordFuture( error );
                }
                else
                {
                    succeedRecordFuture( null );
                }
            } );
        }
        else
        {
            succeedRecordFuture( record );
        }
    }

    private synchronized void succeedRecordFuture( Record record )
    {
        if ( recordFuture != null )
        {
            CompletableFuture<Record> future = recordFuture;
            recordFuture = null;
            future.complete( record );
        }
    }
}
