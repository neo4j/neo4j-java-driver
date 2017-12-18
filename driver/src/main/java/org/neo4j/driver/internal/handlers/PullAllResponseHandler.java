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
package org.neo4j.driver.internal.handlers;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.internal.util.MetadataUtil;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.summary.ResultSummary;

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.internal.util.Futures.failedFuture;

public abstract class PullAllResponseHandler implements ResponseHandler
{
    static final int RECORD_BUFFER_LOW_WATERMARK = Integer.getInteger( "recordBufferLowWatermark", 300 );
    static final int RECORD_BUFFER_HIGH_WATERMARK = Integer.getInteger( "recordBufferHighWatermark", 1000 );

    private final Statement statement;
    private final RunResponseHandler runResponseHandler;
    protected final Connection connection;

    private final Queue<Record> records = new ArrayDeque<>();

    private boolean finished;
    private Throwable failure;
    private ResultSummary summary;

    private CompletableFuture<Record> recordFuture;
    private CompletableFuture<ResultSummary> summaryFuture;
    private CompletableFuture<Throwable> failureFuture;

    public PullAllResponseHandler( Statement statement, RunResponseHandler runResponseHandler, Connection connection )
    {
        this.statement = requireNonNull( statement );
        this.runResponseHandler = requireNonNull( runResponseHandler );
        this.connection = requireNonNull( connection );
    }

    @Override
    public synchronized void onSuccess( Map<String,Value> metadata )
    {
        finished = true;
        summary = extractResultSummary( metadata );

        afterSuccess();

        completeRecordFuture( null );
        completeSummaryFuture( summary );
        completeFailureFuture( null );
    }

    protected abstract void afterSuccess();

    @Override
    public synchronized void onFailure( Throwable error )
    {
        finished = true;
        summary = extractResultSummary( emptyMap() );

        afterFailure( error );

        boolean failedRecordFuture = failRecordFuture( error );
        if ( failedRecordFuture )
        {
            // error propagated through record future, complete other two
            completeSummaryFuture( summary );
            completeFailureFuture( null );
        }
        else
        {
            boolean failedSummaryFuture = failSummaryFuture( error );
            if ( failedSummaryFuture )
            {
                // error propagated through summary future, complete other one
                completeFailureFuture( null );
            }
            else
            {
                boolean completedFailureFuture = completeFailureFuture( error );
                if ( !completedFailureFuture )
                {
                    // error has not been propagated to the user, remember it
                    failure = error;
                }
            }
        }
    }

    protected abstract void afterFailure( Throwable error );

    @Override
    public synchronized void onRecord( Value[] fields )
    {
        Record record = new InternalRecord( runResponseHandler.statementKeys(), fields );
        queueRecord( record );
        completeRecordFuture( record );
    }

    public synchronized CompletionStage<Record> peekAsync()
    {
        Record record = records.peek();
        if ( record == null )
        {
            if ( failure != null )
            {
                return failedFuture( extractFailure() );
            }

            if ( finished )
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
        if ( failure != null )
        {
            return failedFuture( extractFailure() );
        }
        else if ( summary != null )
        {
            return completedFuture( summary );
        }
        else
        {
            if ( summaryFuture == null )
            {
                // neither SUCCESS nor FAILURE message has arrived, register future to be notified when it arrives
                // future will be completed with summary on SUCCESS and completed exceptionally on FAILURE
                // enable auto-read, otherwise we might not read SUCCESS/FAILURE if records are not consumed
                connection.enableAutoRead();
                summaryFuture = new CompletableFuture<>();
            }
            return summaryFuture;
        }
    }

    public synchronized CompletionStage<Throwable> failureAsync()
    {
        if ( failure != null )
        {
            return completedFuture( extractFailure() );
        }
        else if ( finished )
        {
            return completedWithNull();
        }
        else
        {
            if ( failureFuture == null )
            {
                // neither SUCCESS nor FAILURE message has arrived, register future to be notified when it arrives
                // future will be completed with null on SUCCESS and completed with Throwable on FAILURE
                // enable auto-read, otherwise we might not read SUCCESS/FAILURE if records are not consumed
                connection.enableAutoRead();
                failureFuture = new CompletableFuture<>();
            }
            return failureFuture;
        }
    }

    private void queueRecord( Record record )
    {
        records.add( record );

        boolean shouldBufferAllRecords = summaryFuture != null || failureFuture != null;
        // when summary or failure is requested we have to buffer all remaining records and then return summary/failure
        // do not disable auto-read in this case, otherwise records will not be consumed and trailing
        // SUCCESS or FAILURE message will not arrive as well, so callers will get stuck waiting for summary/failure
        if ( !shouldBufferAllRecords && records.size() > RECORD_BUFFER_HIGH_WATERMARK )
        {
            // more than high watermark records are already queued, tell connection to stop auto-reading from network
            // this is needed to deal with slow consumers, we do not want to buffer all records in memory if they are
            // fetched from network faster than consumed
            connection.disableAutoRead();
        }
    }

    private Record dequeueRecord()
    {
        Record record = records.poll();

        if ( records.size() < RECORD_BUFFER_LOW_WATERMARK )
        {
            // less than low watermark records are now available in the buffer, tell connection to pre-fetch more
            // and populate queue with new records from network
            connection.enableAutoRead();
        }

        return record;
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

    private void completeSummaryFuture( ResultSummary summary )
    {
        if ( summaryFuture != null )
        {
            CompletableFuture<ResultSummary> future = summaryFuture;
            summaryFuture = null;
            future.complete( summary );
        }
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

    private boolean completeFailureFuture( Throwable error )
    {
        if ( failureFuture != null )
        {
            CompletableFuture<Throwable> future = failureFuture;
            failureFuture = null;
            future.complete( error );
            return true;
        }
        return false;
    }

    private ResultSummary extractResultSummary( Map<String,Value> metadata )
    {
        long resultAvailableAfter = runResponseHandler.resultAvailableAfter();
        return MetadataUtil.extractSummary( statement, connection, resultAvailableAfter, metadata );
    }
}
