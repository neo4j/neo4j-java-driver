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

import java.util.Map;
import java.util.function.BiConsumer;

import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.messaging.request.DiscardNMessage;
import org.neo4j.driver.internal.messaging.request.PullNMessage;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.MetadataExtractor;
import org.neo4j.driver.internal.value.BooleanValue;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.summary.ResultSummary;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * In this class we have a hidden state machine.
 * Here is how it looks like:
 * |                    | Done | Failed | Streaming                      | Ready              | Canceled       |
 * |--------------------|------|--------|--------------------------------|--------------------|----------------|
 * | request            | X    | X      | toRequest++ ->Streaming        | PULL ->Streaming   | X              |
 * | cancel             | X    | X      | ->Canceled                     | DISCARD ->Canceled | ->Canceled     |
 * | onSuccess has_more | X    | X      | ->Ready request if toRequest>0 | X                  | ->Ready cancel |
 * | onSuccess          | X    | X      | summary ->Done                 | X                  | summary ->Done |
 * | onRecord           | X    | X      | yield record ->Streaming       | X                  | ->Canceled     |
 * | onFailure          | X    | X      | ->Failed                       | X                  | ->Failed       |
 *
 * Currently the error state (marked with X on the table above) is not enforced.
 */
public abstract class AbstractBasicPullResponseHandler implements BasicPullResponseHandler
{
    private final Statement statement;
    private final RunResponseHandler runResponseHandler;
    protected final MetadataExtractor metadataExtractor;
    protected final Connection connection;

    private Status status = Status.Ready;
    private long toRequest;
    private BiConsumer<Record,Throwable> recordConsumer = null;
    private BiConsumer<ResultSummary, Throwable> summaryConsumer = null;

    protected abstract void afterSuccess( Map<String,Value> metadata );

    protected abstract void afterFailure( Throwable error );

    public AbstractBasicPullResponseHandler( Statement statement, RunResponseHandler runResponseHandler, Connection connection, MetadataExtractor metadataExtractor )
    {
        this.statement = requireNonNull( statement );
        this.runResponseHandler = requireNonNull( runResponseHandler );
        this.metadataExtractor = requireNonNull( metadataExtractor );
        this.connection = requireNonNull( connection );
    }

    @Override
    public synchronized void onSuccess( Map<String,Value> metadata )
    {
        assertRecordAndSummaryConsumerInstalled();
        if ( metadata.getOrDefault( "has_more", BooleanValue.FALSE ).asBoolean() )
        {
            handleSuccessWithHasMore();
        }
        else
        {
            handleSuccessWithSummary( metadata );
        }
    }

    @Override
    public synchronized void onFailure( Throwable error )
    {
        assertRecordAndSummaryConsumerInstalled();
        status = Status.Failed;
        afterFailure( error );
        summaryConsumer.accept( null, error );
        recordConsumer.accept( null, error );
    }

    @Override
    public synchronized void onRecord( Value[] fields )
    {
        assertRecordAndSummaryConsumerInstalled();
        if ( isStreaming() )
        {
            Record record = new InternalRecord( runResponseHandler.statementKeys(), fields );
            recordConsumer.accept( record, null );
        }
    }

    @Override
    public synchronized void request( long size )
    {
        assertRecordAndSummaryConsumerInstalled();
        if ( isStreamingPaused() )
        {
            connection.writeAndFlush( new PullNMessage( size, runResponseHandler.statementId() ), this );
            status = Status.Streaming;
        }
        else if ( isStreaming() )
        {
            addToRequest( size );
        }
    }

    @Override
    public synchronized void cancel()
    {
        assertRecordAndSummaryConsumerInstalled();
        if ( isStreamingPaused() )
        {
            // Reactive API does not provide a way to discard N. Only discard all.
            connection.writeAndFlush( new DiscardNMessage( Long.MAX_VALUE, runResponseHandler.statementId() ), this );
            status = Status.Canceled;
        }
        else if ( isStreaming() )
        {
            status = Status.Canceled;
        }
        // no need to change status if it is already done
    }

    @Override
    public synchronized void installSummaryConsumer( BiConsumer<ResultSummary, Throwable> summaryConsumer )
    {
        if( this.summaryConsumer != null )
        {
            throw new IllegalStateException( "Summary consumer already installed." );
        }
        this.summaryConsumer = summaryConsumer;
    }

    @Override
    public synchronized void installRecordConsumer( BiConsumer<Record,Throwable> recordConsumer )
    {
        if( this.recordConsumer != null )
        {
            throw new IllegalStateException( "Record consumer already installed." );
        }
        this.recordConsumer = recordConsumer;
    }

    private void handleSuccessWithSummary( Map<String,Value> metadata )
    {
        status = Status.Done;
        afterSuccess( metadata );
        // record consumer use (null, null) to identify the end of record stream
        recordConsumer.accept( null, null );
        extractResultSummary( metadata );
    }

    private void handleSuccessWithHasMore()
    {
        if ( this.status == Status.Canceled )
        {
            this.status = Status.Ready; // cancel request accepted.
            cancel();
        }
        else if ( this.status == Status.Streaming )
        {
            this.status = Status.Ready;
            if ( toRequest > 0 )
            {
                request( toRequest );
                toRequest = 0;
            }
            // summary consumer use (null, null) to identify done handling of success with has_more
            summaryConsumer.accept( null, null );
        }
    }

    private void extractResultSummary( Map<String,Value> metadata )
    {
        long resultAvailableAfter = runResponseHandler.resultAvailableAfter();
        ResultSummary summary = metadataExtractor.extractSummary( statement, connection, resultAvailableAfter, metadata );
        summaryConsumer.accept( summary, null );
    }

    @Override
    public boolean isFinishedOrCanceled()
    {
        return status == Status.Done || status == Status.Failed || status == Status.Canceled;
    }

    @Override
    public boolean isStreamingPaused()
    {
        return status == Status.Ready;
    }

    private boolean isStreaming()
    {
        return status == Status.Streaming;
    }

    private void addToRequest( long toAdd )
    {
        if ( toAdd <= 0 )
        {
            throw new IllegalArgumentException( "Cannot request record amount that is less than or equal to 0. Request amount: " + toAdd );
        }
        toRequest += toAdd;
        if ( toRequest <= 0 ) // toAdd is already at least 1, we hit buffer overflow
        {
            toRequest = Long.MAX_VALUE;
        }
    }

    private void assertRecordAndSummaryConsumerInstalled()
    {
        if( recordConsumer == null || summaryConsumer == null )
        {
            System.out.println("record consumer or summary consumer not set.");
            throw new IllegalStateException( format("Access record stream without record consumer and/or summary consumer. " +
                    "Record consumer=%s, Summary consumer=%s", recordConsumer, summaryConsumer) );
        }
    }

    protected Status status()
    {
        return this.status;
    }

    protected void status( Status status )
    {
        this.status = status;
    }
}
