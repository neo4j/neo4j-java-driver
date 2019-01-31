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

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.summary.ResultSummary;

public class AsyncPullResponseHandler implements PullResponseHandler
{
    private final Queue<Record> records = new LinkedList<>();
    private CompletableFuture<Record> recordFuture;

    private final BasicPullResponseHandler delegate;
    private final PullController controller;

    private final CompletableFuture<ResultSummary> summaryFuture = new CompletableFuture<>();

    public AsyncPullResponseHandler( BasicPullResponseHandler handler )
    {
        this( handler, new AutoPullController( handler ) );
    }

    private AsyncPullResponseHandler( BasicPullResponseHandler handler, PullController controller )
    {
        this.delegate = handler;
        this.controller = controller;

        delegate.installSummaryConsumer( this::consumeSummary );
        delegate.installRecordConsumer( this::consumeRecord );
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
            failRecordFuture( throwable );
        }
        else
        {
            // done streaming
            completeRecordFuture( null );
        }
    }

    synchronized void consumeSummary( ResultSummary summary, Throwable throwable )
    {
        if ( summary != null )
        {
            summaryFuture.complete( summary );
        }
        else if ( throwable != null )
        {
            summaryFuture.completeExceptionally( throwable );
        }
        else
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
            future.completeExceptionally( error );
        }
    }

    private synchronized void completeRecordFuture( Record record )
    {
        if( recordFuture != null )
        {
            CompletableFuture<Record> future = recordFuture;
            recordFuture = null;
            future.complete( record );
        }
    }
}
