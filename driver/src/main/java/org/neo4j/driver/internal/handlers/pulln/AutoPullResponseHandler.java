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
package org.neo4j.driver.internal.handlers.pulln;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.internal.handlers.PullAllResponseHandler;
import org.neo4j.driver.internal.handlers.PullResponseCompletionListener;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.Iterables;
import org.neo4j.driver.internal.util.MetadataExtractor;
import org.neo4j.driver.summary.ResultSummary;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.neo4j.driver.internal.handlers.pulln.FetchSizeUtil.UNLIMITED_FETCH_SIZE;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.internal.util.Futures.failedFuture;

/**
 * Built on top of {@link BasicPullResponseHandler} to be able to pull in batches.
 * It is exposed as {@link PullAllResponseHandler} as it can automatically pull when running out of records locally.
 */
public class AutoPullResponseHandler extends BasicPullResponseHandler implements PullAllResponseHandler
{
    private static final Queue<Record> UNINITIALIZED_RECORDS = Iterables.emptyQueue();
    private final long fetchSize;
    private final long lowRecordWatermark;
    private final long highRecordWatermark;

    // initialized lazily when first record arrives
    private Queue<Record> records = UNINITIALIZED_RECORDS;

    private ResultSummary summary;
    private Throwable failure;
    private boolean isAutoPullEnabled = true;

    private CompletableFuture<Record> recordFuture;
    private CompletableFuture<ResultSummary> summaryFuture;

    public AutoPullResponseHandler(Query query, RunResponseHandler runResponseHandler, Connection connection, MetadataExtractor metadataExtractor,
                                   PullResponseCompletionListener completionListener, long fetchSize )
    {
        super(query, runResponseHandler, connection, metadataExtractor, completionListener );
        this.fetchSize = fetchSize;

        //For pull everything ensure conditions for disabling auto pull are never met
        if ( fetchSize == UNLIMITED_FETCH_SIZE )
        {
            this.highRecordWatermark = Long.MAX_VALUE;
            this.lowRecordWatermark = Long.MAX_VALUE;
        }
        else
        {
            this.highRecordWatermark = (long) (fetchSize * 0.7);
            this.lowRecordWatermark = (long) (fetchSize * 0.3);
        }

        installRecordAndSummaryConsumers();
    }

    private void installRecordAndSummaryConsumers()
    {
        installRecordConsumer( ( record, error ) -> {
            if ( record != null )
            {
                enqueueRecord( record );
                completeRecordFuture( record );
            }
            //  if ( error != null ) Handled by summary.error already
            if ( record == null && error == null )
            {
                // complete
                completeRecordFuture( null );
            }
        } );

        installSummaryConsumer( ( summary, error ) -> {
            if ( error != null )
            {
                handleFailure( error );
            }
            if ( summary != null )
            {
                this.summary = summary;
                completeSummaryFuture( summary );
            }

            if ( error == null && summary == null ) // has_more
            {
                if ( isAutoPullEnabled )
                {
                    request( fetchSize );
                }
            }
        } );
    }

    private void handleFailure( Throwable error )
    {
        // error has not been propagated to the user, remember it
        if ( !failRecordFuture( error ) && !failSummaryFuture( error ) )
        {
            failure = error;
        }
    }

    public synchronized CompletionStage<Record> peekAsync()
    {
        Record record = records.peek();
        if ( record == null )
        {
            if ( isDone() )
            {
                return completedWithValueIfNoFailure( null );
            }

            if ( recordFuture == null )
            {
                recordFuture = new CompletableFuture<>();
            }
            return recordFuture;
        }
        else
        {
            return completedFuture( record );
        }
    }

    public synchronized CompletionStage<Record> nextAsync()
    {
        return peekAsync().thenApply( ignore -> dequeueRecord() );
    }

    public synchronized CompletionStage<ResultSummary> consumeAsync()
    {
        records.clear();
        if ( isDone() )
        {
            return completedWithValueIfNoFailure( summary );
        }
        else
        {
            cancel();
            if ( summaryFuture == null )
            {
                summaryFuture = new CompletableFuture<>();
            }

            return summaryFuture;
        }
    }

    public synchronized <T> CompletionStage<List<T>> listAsync( Function<Record,T> mapFunction )
    {
        return pullAllAsync().thenApply( summary -> recordsAsList( mapFunction ) );
    }

    @Override
    public synchronized CompletionStage<Throwable> pullAllFailureAsync()
    {
        return pullAllAsync().handle( ( ignore, error ) -> error );
    }

    @Override
    public void prePopulateRecords()
    {
        request( fetchSize );
    }

    private synchronized CompletionStage<ResultSummary> pullAllAsync()
    {
        if ( isDone() )
        {
            return completedWithValueIfNoFailure( summary );
        }
        else
        {
            request( UNLIMITED_FETCH_SIZE );
            if ( summaryFuture == null )
            {
                summaryFuture = new CompletableFuture<>();
            }

            return summaryFuture;
        }
    }

    private void enqueueRecord( Record record )
    {
        if ( records == UNINITIALIZED_RECORDS )
        {
            records = new ArrayDeque<>();
        }

        records.add( record );

        // too many records in the queue, pause auto request gathering
        if ( records.size() > highRecordWatermark )
        {
            isAutoPullEnabled = false;
        }
    }

    private Record dequeueRecord()
    {
        Record record = records.poll();

        if ( records.size() <= lowRecordWatermark )
        {
            //if not in streaming state we need to restart streaming
            if ( state() != State.STREAMING_STATE )
            {
                request( fetchSize );
            }
            isAutoPullEnabled = true;
        }

        return record;
    }

    private <T> List<T> recordsAsList( Function<Record,T> mapFunction )
    {
        if ( !isDone() )
        {
            throw new IllegalStateException( "Can't get records as list because SUCCESS or FAILURE did not arrive" );
        }

        List<T> result = new ArrayList<>( records.size() );
        while ( !records.isEmpty() )
        {
            Record record = records.poll();
            result.add( mapFunction.apply( record ) );
        }
        return result;
    }

    private Throwable extractFailure()
    {
        if ( failure == null )
        {
            throw new IllegalStateException( "Can't extract failure because it does not exist" );
        }

        Throwable error = failure;
        failure = null; // propagate failure only once
        return error;
    }

    private void completeRecordFuture( Record record )
    {
        if ( recordFuture != null )
        {
            CompletableFuture<Record> future = recordFuture;
            recordFuture = null;
            future.complete( record );
        }
    }

    private void completeSummaryFuture( ResultSummary summary )
    {
        if ( summaryFuture != null )
        {
            CompletableFuture<ResultSummary> future = summaryFuture;
            summaryFuture = null;
            future.complete( summary );
        }
    }

    private boolean failRecordFuture( Throwable error )
    {
        if ( recordFuture != null )
        {
            CompletableFuture<Record> future = recordFuture;
            recordFuture = null;
            future.completeExceptionally( error );
            return true;
        }
        return false;
    }

    private boolean failSummaryFuture( Throwable error )
    {
        if ( summaryFuture != null )
        {
            CompletableFuture<ResultSummary> future = summaryFuture;
            summaryFuture = null;
            future.completeExceptionally( error );
            return true;
        }
        return false;
    }

    private <T> CompletionStage<T> completedWithValueIfNoFailure( T value )
    {
        if ( failure != null )
        {
            return failedFuture( extractFailure() );
        }
        else if ( value == null )
        {
            return completedWithNull();
        }
        else
        {
            return completedFuture( value );
        }
    }
}
