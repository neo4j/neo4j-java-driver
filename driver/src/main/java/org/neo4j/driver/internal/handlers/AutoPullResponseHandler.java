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
package org.neo4j.driver.internal.handlers;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.neo4j.driver.Record;
import org.neo4j.driver.Statement;
import org.neo4j.driver.internal.handlers.pulln.BasicPullResponseHandler;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.internal.util.Iterables;
import org.neo4j.driver.internal.util.MetadataExtractor;
import org.neo4j.driver.summary.ResultSummary;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.internal.util.Futures.failedFuture;

public class AutoPullResponseHandler extends BasicPullResponseHandler implements PullAllResponseHandler
{
    private static final Queue<Record> UNINITIALIZED_RECORDS = Iterables.emptyQueue();

    private static final int RECORD_BUFFER_LOW_WATERMARK = Integer.getInteger( "recordBufferLowWatermark", 300 );
    private static final int RECORD_BUFFER_HIGH_WATERMARK = Integer.getInteger( "recordBufferHighWatermark", 1000 );
    private static final long BATCH_SIZE = 1000;

    // initialized lazily when first record arrives
    private Queue<Record> records = UNINITIALIZED_RECORDS;

//    private boolean autoReadManagementEnabled = true;
    private Throwable failure;

    private CompletableFuture<Record> recordFuture;
    private final CompletableFuture<ResultSummary> summaryFuture = new CompletableFuture<>();

    public AutoPullResponseHandler( Statement statement, RunResponseHandler runResponseHandler, Connection connection, MetadataExtractor metadataExtractor,
            PullResponseCompletionListener completionListener )
    {
        super( statement, runResponseHandler, connection, metadataExtractor, completionListener );
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
            else if ( error != null )
            {
                // Handled by summary.error already
            }
            else
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
                summaryFuture.complete( summary );
            }
            else
            {
                // has_more
                request( BATCH_SIZE );
            }
        } );
    }

    private void handleFailure( Throwable error )
    {
        boolean isFailureReported = failRecordFuture( error );
        if ( !isFailureReported )
        {
            // error has not been propagated to the user, remember it
            failure = error;
        }
    }
//
//    @Override
//    public boolean canManageAutoRead()
//    {
//        return true;
//    }

//    @Override
//    public synchronized void disableAutoReadManagement()
//    {
//        autoReadManagementEnabled = false;
//    }

    public synchronized CompletionStage<Record> peekAsync()
    {
        Record record = records.peek();
        if ( record == null )
        {
            if ( failure != null )
            {
                return failedFuture( extractFailure() );
            }

            if ( isDone() )
            {
                return completedWithNull();
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

    public synchronized CompletionStage<ResultSummary> summaryAsync()
    {
        if ( !isDone() )
        {
            cancel();
        }

        return summaryFuture.thenApply( summary ->
        {
            if ( failure != null )
            {
                throw Futures.asCompletionException( extractFailure() );
            }
            return summary;
        } );
    }

    public synchronized <T> CompletionStage<List<T>> listAsync( Function<Record,T> mapFunction )
    {
        return pullAllAsync().thenApply( error ->
        {
            if ( error != null )
            {
                throw Futures.asCompletionException( error );
            }
            return recordsAsList( mapFunction );
        } );
    }

    @Override
    public void prePull()
    {
        request( BATCH_SIZE );
    }

    private synchronized CompletionStage<Throwable> pullAllAsync()
    {
        if ( !isDone() )
        {
//            enableAutoRead();
            request( Long.MAX_VALUE );
        }
        return summaryFuture.thenApply( summary -> {
            if ( failure != null )
            {
                return extractFailure();
            }
            return null;
        } );
    }

    private void enqueueRecord( Record record )
    {
        if ( records == UNINITIALIZED_RECORDS )
        {
            records = new ArrayDeque<>();
        }

        records.add( record );

//        boolean shouldBufferAllRecords = failureFuture != null;
//        // when failure is requested we have to buffer all remaining records and then return the error
//        // do not disable auto-read in this case, otherwise records will not be consumed and trailing
//        // SUCCESS or FAILURE message will not arrive as well, so callers will get stuck waiting for the error
//        if ( !shouldBufferAllRecords && records.size() > RECORD_BUFFER_HIGH_WATERMARK )
//        {
//            // more than high watermark records are already queued, tell connection to stop auto-reading from network
//            // this is needed to deal with slow consumers, we do not want to buffer all records in memory if they are
//            // fetched from network faster than consumed
//            disableAutoRead();
//        }
    }

    private Record dequeueRecord()
    {
        Record record = records.poll();

//        if ( records.size() < RECORD_BUFFER_LOW_WATERMARK )
//        {
//            // less than low watermark records are now available in the buffer, tell connection to pre-fetch more
//            // and populate queue with new records from network
//            enableAutoRead();
//        }

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


//    private void enableAutoRead()
//    {
//        if ( autoReadManagementEnabled )
//        {
//            connection.enableAutoRead();
//        }
//    }
//
//    private void disableAutoRead()
//    {
//        if ( autoReadManagementEnabled )
//        {
//            connection.disableAutoRead();
//        }
//    }
}
