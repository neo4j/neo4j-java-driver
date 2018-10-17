/*
 * Copyright (c) 2002-2009 "Neo4j,"
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
import org.neo4j.driver.internal.messaging.request.DiscardAllMessage;
import org.neo4j.driver.internal.messaging.request.PullNMessage;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.MetadataExtractor;
import org.neo4j.driver.internal.value.BooleanValue;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.summary.ResultSummary;

import static java.util.Objects.requireNonNull;

public abstract class AbstractBasicPullResponseHandler implements BasicPullResponseHandler
{
    private final Statement statement;
    private final RunResponseHandler runResponseHandler;
    protected final MetadataExtractor metadataExtractor;
    protected final Connection connection;

    private Status status = Status.Paused;
    private long toRequest;
    private BiConsumer<Record,Throwable> recordConsumer = NULL_RECORD_CONSUMER;
    private BiConsumer<ResultSummary, Throwable> summaryConsumer = NULL_SUCCESS_CONSUMER;

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
        if ( metadata.getOrDefault( "has_more", BooleanValue.FALSE ).asBoolean() )
        {
            handleSuccessWithHasMore( metadata );
        }
        else
        {
            handleSuccessWithSummary( metadata );
        }
    }

    @Override
    public synchronized void onFailure( Throwable error )
    {
        recordConsumer.accept( null, error );
        summaryConsumer.accept( null, error );
        status = Status.Failed;
        afterFailure( error );
    }

    @Override
    public synchronized void onRecord( Value[] fields )
    {
        if ( isFinishedOrCanceled() )
        {
            recordConsumer.accept( null, null );
        }
        else
        {
            Record record = new InternalRecord( runResponseHandler.statementKeys(), fields );
            recordConsumer.accept( record, null );
        }
    }

    @Override
    public synchronized void request( long size )
    {
        if ( isStreamingPaused() )
        {
            connection.writeAndFlush( new PullNMessage( size, runResponseHandler.statementId() ), this );
            this.status = Status.Streaming;
        }
        else if ( !isFinishedOrCanceled() ) // is streaming
        {
            this.toRequest = safeAdd( this.toRequest, size );
        }
        else
        {
            recordConsumer.accept( null, null );
        }
    }

    @Override
    public synchronized void cancel()
    {
        if ( isStreamingPaused() )
        {
            connection.writeAndFlush( DiscardAllMessage.DISCARD_ALL, this );
        }

        if ( !isFinishedOrCanceled() ) // no need to change status if it is already done
        {
            status = Status.Cancelled;
        }
    }

    @Override
    public synchronized void installSummaryConsumer( BiConsumer<ResultSummary, Throwable> summaryConsumer )
    {
        this.summaryConsumer = summaryConsumer;
    }

    @Override
    public Status status()
    {
        return status;
    }

    @Override
    public synchronized void installRecordConsumer( BiConsumer<Record,Throwable> recordConsumer )
    {
        this.recordConsumer = recordConsumer;
    }

    private void handleSuccessWithSummary( Map<String,Value> metadata )
    {
        recordConsumer.accept( null, null );
        extractResultSummary( metadata );
        status = Status.Done;
        afterSuccess( metadata );
    }

    private void handleSuccessWithHasMore( Map<String,Value> metadata )
    {
        if ( this.status == Status.Cancelled )
        {
            this.status = Status.Paused;
            cancel();
        }
        else if ( this.status == Status.Streaming )
        {
            this.status = Status.Paused;
            if ( toRequest > 0 )
            {
                request( toRequest );
                toRequest = 0;
            }
            summaryConsumer.accept( null, null );
        }
    }

    private void extractResultSummary( Map<String,Value> metadata )
    {
        long resultAvailableAfter = runResponseHandler.resultAvailableAfter();
        ResultSummary summary = metadataExtractor.extractSummary( statement, connection, resultAvailableAfter, metadata );
        summaryConsumer.accept( summary, null );
    }

    private boolean isFinishedOrCanceled()
    {
        return status == Status.Done || status == Status.Failed || status == Status.Cancelled;
    }

    private boolean isStreamingPaused()
    {
        return status == Status.Paused;
    }

    private static long safeAdd( long n1, long n2 )
    {
        long newValue = n1 + n2;
        if ( newValue < 0 )
        {
            newValue = Long.MAX_VALUE;
        }
        return newValue;
    }
}
