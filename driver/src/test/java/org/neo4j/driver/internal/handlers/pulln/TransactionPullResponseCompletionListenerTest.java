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

import java.util.Collections;
import java.util.function.BiConsumer;

import org.neo4j.driver.Record;
import org.neo4j.driver.Statement;
import org.neo4j.driver.internal.async.ExplicitTransaction;
import org.neo4j.driver.internal.handlers.PullResponseCompletionListener;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.messaging.v4.BoltProtocolV4;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.summary.ResultSummary;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.driver.internal.handlers.pulln.PullResponseHandler.Status.FAILED;
import static org.neo4j.driver.internal.handlers.pulln.PullResponseHandler.Status.SUCCEEDED;

public class TransactionPullResponseCompletionListenerTest extends BasicPullResponseHandlerTestBase
{
    @Override
    protected void shouldHandleSuccessWithSummary( PullResponseHandler.Status status )
    {
        // Given
        Connection conn = mockConnection();
        BiConsumer<Record,Throwable> recordConsumer = mock( BiConsumer.class );
        BiConsumer<ResultSummary,Throwable> summaryConsumer = mock( BiConsumer.class );
        BasicPullResponseHandler handler = newResponseHandlerWithStatus( conn, recordConsumer, summaryConsumer, status);

        // When
        handler.onSuccess( Collections.emptyMap() );

        // Then
        assertThat( handler.status(), equalTo( SUCCEEDED ) );
        verify( recordConsumer ).accept( null, null );
        verify( summaryConsumer ).accept( any( ResultSummary.class ), eq( null ) );
    }

    @Override
    protected void shouldHandleFailure( PullResponseHandler.Status status )
    {
        // Given
        Connection conn = mockConnection();
        BiConsumer<Record,Throwable> recordConsumer = mock( BiConsumer.class );
        BiConsumer<ResultSummary,Throwable> summaryConsumer = mock( BiConsumer.class );
        ExplicitTransaction tx = mock( ExplicitTransaction.class );
        BasicPullResponseHandler handler = newTxResponseHandler( conn, recordConsumer, summaryConsumer, tx, status );

        // When
        RuntimeException error = new RuntimeException( "I am an error" );
        handler.onFailure( error );

        // Then
        assertThat( handler.status(), equalTo( FAILED ) );
        verify( tx ).markTerminated();
        verify( recordConsumer ).accept( null, error );
        verify( summaryConsumer ).accept( any( ResultSummary.class ), eq( null ) );
    }

    @Override
    protected BasicPullResponseHandler newResponseHandlerWithStatus( Connection conn, BiConsumer<Record,Throwable> recordConsumer,
            BiConsumer<ResultSummary,Throwable> summaryConsumer, PullResponseHandler.Status status )
    {
        ExplicitTransaction tx = mock( ExplicitTransaction.class );
        return newTxResponseHandler( conn, recordConsumer, summaryConsumer, tx, status );
    }

    private static BasicPullResponseHandler newTxResponseHandler( Connection conn, BiConsumer<Record,Throwable> recordConsumer,
            BiConsumer<ResultSummary,Throwable> summaryConsumer, ExplicitTransaction tx, PullResponseHandler.Status status )
    {
        RunResponseHandler runHandler = mock( RunResponseHandler.class );
        BasicPullResponseHandler handler =
                new BasicPullResponseHandler( mock( Statement.class ), runHandler, conn, BoltProtocolV4.METADATA_EXTRACTOR, mock(
                        PullResponseCompletionListener.class ) );

        handler.installRecordConsumer( recordConsumer );
        handler.installSummaryConsumer( summaryConsumer );

        handler.status( status );
        return handler;
    }
}
