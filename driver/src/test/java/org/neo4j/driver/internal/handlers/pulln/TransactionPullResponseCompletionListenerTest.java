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

import java.util.Collections;
import java.util.function.BiConsumer;

import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.internal.async.UnmanagedTransaction;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.handlers.TransactionPullResponseCompletionListener;
import org.neo4j.driver.internal.messaging.v4.BoltProtocolV4;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.summary.ResultSummary;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TransactionPullResponseCompletionListenerTest extends BasicPullResponseHandlerTestBase
{
    @Override
    protected void shouldHandleSuccessWithSummary( BasicPullResponseHandler.State state )
    {
        // Given
        Connection conn = mockConnection();
        BiConsumer<Record,Throwable> recordConsumer = mock( BiConsumer.class );
        BiConsumer<ResultSummary,Throwable> summaryConsumer = mock( BiConsumer.class );
        BasicPullResponseHandler handler = newResponseHandlerWithStatus( conn, recordConsumer, summaryConsumer, state );

        // When
        handler.onSuccess( Collections.emptyMap() );

        // Then
        assertThat( handler.state(), equalTo( BasicPullResponseHandler.State.SUCCEEDED_STATE ));
        verify( recordConsumer ).accept( null, null );
        verify( summaryConsumer ).accept( any( ResultSummary.class ), eq( null ) );
    }

    @Override
    protected void shouldHandleFailure( BasicPullResponseHandler.State state )
    {
        // Given
        Connection conn = mockConnection();
        BiConsumer<Record,Throwable> recordConsumer = mock( BiConsumer.class );
        BiConsumer<ResultSummary,Throwable> summaryConsumer = mock( BiConsumer.class );
        UnmanagedTransaction tx = mock( UnmanagedTransaction.class );
        BasicPullResponseHandler handler = newTxResponseHandler( conn, recordConsumer, summaryConsumer, tx, state );

        // When
        RuntimeException error = new RuntimeException( "I am an error" );
        handler.onFailure( error );

        // Then
        assertThat( handler.state(), equalTo( BasicPullResponseHandler.State.FAILURE_STATE ) );
        verify( tx ).markTerminated( error );
        verify( recordConsumer ).accept( null, error );
        verify( summaryConsumer ).accept( any( ResultSummary.class ), eq( error ) );
    }

    @Override
    protected BasicPullResponseHandler newResponseHandlerWithStatus( Connection conn, BiConsumer<Record,Throwable> recordConsumer,
                                                                     BiConsumer<ResultSummary,Throwable> summaryConsumer, BasicPullResponseHandler.State state )
    {
        UnmanagedTransaction tx = mock( UnmanagedTransaction.class );
        return newTxResponseHandler( conn, recordConsumer, summaryConsumer, tx, state );
    }

    private static BasicPullResponseHandler newTxResponseHandler( Connection conn, BiConsumer<Record,Throwable> recordConsumer,
                                                                  BiConsumer<ResultSummary,Throwable> summaryConsumer, UnmanagedTransaction tx,
                                                                  BasicPullResponseHandler.State state )
    {
        RunResponseHandler runHandler = mock( RunResponseHandler.class );
        TransactionPullResponseCompletionListener listener = new TransactionPullResponseCompletionListener( tx );
        BasicPullResponseHandler handler =
                new BasicPullResponseHandler( mock( Query.class ), runHandler, conn, BoltProtocolV4.METADATA_EXTRACTOR, listener );

        handler.installRecordConsumer( recordConsumer );
        handler.installSummaryConsumer( summaryConsumer );

        handler.state( state );
        return handler;
    }
}
