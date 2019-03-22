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
import org.neo4j.driver.internal.messaging.request.PullMessage;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.MetadataExtractor;
import org.neo4j.driver.internal.value.BooleanValue;
import org.neo4j.driver.Record;
import org.neo4j.driver.Statement;
import org.neo4j.driver.Value;
import org.neo4j.driver.summary.ResultSummary;

import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static org.neo4j.driver.internal.messaging.request.DiscardMessage.newDiscardAllMessage;

/**
 * In this class we have a hidden state machine.
 * Here is how it looks like:
 * |                    | DONE | FAILED | STREAMING                      | READY              | CANCELED       |
 * |--------------------|------|--------|--------------------------------|--------------------|----------------|
 * | request            | X    | X      | toRequest++ ->STREAMING        | PULL ->STREAMING   | X              |
 * | cancel             | X    | X      | ->CANCELED                     | DISCARD ->CANCELED | ->CANCELED     |
 * | onSuccess has_more | X    | X      | ->READY request if toRequest>0 | X                  | ->READY cancel |
 * | onSuccess          | X    | X      | summary ->DONE                 | X                  | summary ->DONE |
 * | onRecord           | X    | X      | yield record ->STREAMING       | X                  | ->CANCELED     |
 * | onFailure          | X    | X      | ->FAILED                       | X                  | ->FAILED       |
 *
 * Currently the error state (marked with X on the table above) might not be enforced.
 */
public abstract class AbstractBasicPullResponseHandler implements BasicPullResponseHandler
{
    public static final BiConsumer<Record,Throwable> DISCARD_RECORD_CONSUMER = ( record, throwable ) -> {/*do nothing*/};

    private final Statement statement;
    protected final RunResponseHandler runResponseHandler;
    protected final MetadataExtractor metadataExtractor;
    protected final Connection connection;

    private Status status = Status.READY;
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
        status = Status.FAILED;
        afterFailure( error );

        complete( extractResultSummary( emptyMap() ), error );
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
            connection.writeAndFlush( new PullMessage( size, runResponseHandler.statementId() ), this );
            status = Status.STREAMING;
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
            connection.writeAndFlush( newDiscardAllMessage( runResponseHandler.statementId() ), this );
            status = Status.CANCELED;
        }
        else if ( isStreaming() )
        {
            status = Status.CANCELED;
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

    private boolean isStreaming()
    {
        return status == Status.STREAMING;
    }

    private boolean isStreamingPaused()
    {
        return status == Status.READY;
    }

    private boolean isFinished()
    {
        return status == Status.DONE || status == Status.FAILED;
    }

    private void handleSuccessWithSummary( Map<String,Value> metadata )
    {
        status = Status.DONE;
        afterSuccess( metadata );
        ResultSummary summary = extractResultSummary( metadata );

        complete( summary, null );
    }

    private void handleSuccessWithHasMore()
    {
        if ( this.status == Status.CANCELED )
        {
            this.status = Status.READY; // cancel request accepted.
            cancel();
        }
        else if ( this.status == Status.STREAMING )
        {
            this.status = Status.READY;
            if ( toRequest > 0 )
            {
                request( toRequest );
                toRequest = 0;
            }
            // summary consumer use (null, null) to identify done handling of success with has_more
            summaryConsumer.accept( null, null );
        }
    }

    private ResultSummary extractResultSummary( Map<String,Value> metadata )
    {
        long resultAvailableAfter = runResponseHandler.resultAvailableAfter();
        return metadataExtractor.extractSummary( statement, connection, resultAvailableAfter, metadata );
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
        if( isFinished() )
        {
            // no need to check if we've finished.
            return;
        }
        if( recordConsumer == null || summaryConsumer == null )
        {
            throw new IllegalStateException( format("Access record stream without record consumer and/or summary consumer. " +
                    "Record consumer=%s, Summary consumer=%s", recordConsumer, summaryConsumer) );
        }
    }

    private void complete( ResultSummary summary, Throwable error )
    {
        // we first inform the summary consumer to ensure when streaming finished, summary is definitely available.
        if ( recordConsumer == DISCARD_RECORD_CONSUMER )
        {
            // we will report the error to summary if there is no record consumer
            summaryConsumer.accept( summary, error );
        }
        else
        {
            // we will not inform the error to summary as the error will be reported to record consumer
            summaryConsumer.accept( summary, null );
        }

        // record consumer use (null, null) to identify the end of record stream
        recordConsumer.accept( null, error );
        dispose();
    }

    private void dispose()
    {
        // release the reference to the consumers who hold the reference to subscribers which shall be released when subscription is completed.
        this.recordConsumer = null;
        this.summaryConsumer = null;
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
