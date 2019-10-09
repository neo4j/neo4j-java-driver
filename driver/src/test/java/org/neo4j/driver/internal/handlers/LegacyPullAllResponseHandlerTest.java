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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.neo4j.driver.Record;
import org.neo4j.driver.Statement;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.summary.ResultSummary;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.Values.values;
import static org.neo4j.driver.internal.messaging.v1.BoltProtocolV1.METADATA_EXTRACTOR;
import static org.neo4j.driver.util.TestUtil.await;

class LegacyPullAllResponseHandlerTest extends PullAllResponseHandlerTestBase<LegacyPullAllResponseHandler>
{
    @Test
    void shouldReturnNoFailureWhenAlreadySucceeded()
    {
        LegacyPullAllResponseHandler handler = newHandler();
        handler.onSuccess( emptyMap() );

        Throwable failure = await( handler.failureAsync() );

        assertNull( failure );
    }

    @Test
    void shouldReturnNoFailureWhenSucceededAfterFailureRequested()
    {
        LegacyPullAllResponseHandler handler = newHandler();

        CompletableFuture<Throwable> failureFuture = handler.failureAsync().toCompletableFuture();
        assertFalse( failureFuture.isDone() );

        handler.onSuccess( emptyMap() );

        assertTrue( failureFuture.isDone() );
        assertNull( await( failureFuture ) );
    }

    @Test
    void shouldReturnFailureWhenAlreadyFailed()
    {
        LegacyPullAllResponseHandler handler = newHandler();

        RuntimeException failure = new RuntimeException( "Ops" );
        handler.onFailure( failure );

        Throwable receivedFailure = await( handler.failureAsync() );
        assertEquals( failure, receivedFailure );
    }

    @Test
    void shouldReturnFailureWhenFailedAfterFailureRequested()
    {
        LegacyPullAllResponseHandler handler = newHandler();

        CompletableFuture<Throwable> failureFuture = handler.failureAsync().toCompletableFuture();
        assertFalse( failureFuture.isDone() );

        IOException failure = new IOException( "Broken pipe" );
        handler.onFailure( failure );

        assertTrue( failureFuture.isDone() );
        assertEquals( failure, await( failureFuture ) );
    }

    @Test
    void shouldReturnFailureWhenRequestedMultipleTimes()
    {
        LegacyPullAllResponseHandler handler = newHandler();

        CompletableFuture<Throwable> failureFuture1 = handler.failureAsync().toCompletableFuture();
        CompletableFuture<Throwable> failureFuture2 = handler.failureAsync().toCompletableFuture();

        assertFalse( failureFuture1.isDone() );
        assertFalse( failureFuture2.isDone() );

        RuntimeException failure = new RuntimeException( "Unable to contact database" );
        handler.onFailure( failure );

        assertTrue( failureFuture1.isDone() );
        assertTrue( failureFuture2.isDone() );

        assertEquals( failure, await( failureFuture1 ) );
        assertEquals( failure, await( failureFuture2 ) );
    }

    @Test
    void shouldReturnFailureOnlyOnceWhenFailedBeforeFailureRequested()
    {
        LegacyPullAllResponseHandler handler = newHandler();

        ServiceUnavailableException failure = new ServiceUnavailableException( "Connection terminated" );
        handler.onFailure( failure );

        assertEquals( failure, await( handler.failureAsync() ) );
        assertNull( await( handler.failureAsync() ) );
    }

    @Test
    void shouldReturnFailureOnlyOnceWhenFailedAfterFailureRequested()
    {
        LegacyPullAllResponseHandler handler = newHandler();

        CompletionStage<Throwable> failureFuture = handler.failureAsync();

        SessionExpiredException failure = new SessionExpiredException( "Network unreachable" );
        handler.onFailure( failure );
        assertEquals( failure, await( failureFuture ) );

        assertNull( await( handler.failureAsync() ) );
    }

    @Test
    void shouldReturnSummaryWhenAlreadyFailedAndFailureConsumed()
    {
        Statement statement = new Statement( "CREATE ()" );
        LegacyPullAllResponseHandler handler = newHandler( statement );

        ServiceUnavailableException failure = new ServiceUnavailableException( "Neo4j unreachable" );
        handler.onFailure( failure );

        assertEquals( failure, await( handler.failureAsync() ) );

        ResultSummary summary = await( handler.summaryAsync() );
        assertNotNull( summary );
        assertEquals( statement, summary.statement() );
    }

    @Test
    void shouldDisableAutoReadWhenTooManyRecordsArrive()
    {
        Connection connection = connectionMock();
        LegacyPullAllResponseHandler handler = newHandler( asList( "key1", "key2" ), connection );

        for ( int i = 0; i < LegacyPullAllResponseHandler.RECORD_BUFFER_HIGH_WATERMARK + 1; i++ )
        {
            handler.onRecord( values( 100, 200 ) );
        }

        verify( connection ).disableAutoRead();
    }

    @Test
    void shouldEnableAutoReadWhenRecordsRetrievedFromBuffer()
    {
        Connection connection = connectionMock();
        List<String> keys = asList( "key1", "key2" );
        LegacyPullAllResponseHandler handler = newHandler( keys, connection );

        int i;
        for ( i = 0; i < LegacyPullAllResponseHandler.RECORD_BUFFER_HIGH_WATERMARK + 1; i++ )
        {
            handler.onRecord( values( 100, 200 ) );
        }

        verify( connection, never() ).enableAutoRead();
        verify( connection ).disableAutoRead();

        while ( i-- > LegacyPullAllResponseHandler.RECORD_BUFFER_LOW_WATERMARK - 1 )
        {
            Record record = await( handler.nextAsync() );
            assertNotNull( record );
            assertEquals( keys, record.keys() );
            assertEquals( 100, record.get( "key1" ).asInt() );
            assertEquals( 200, record.get( "key2" ).asInt() );
        }
        verify( connection ).enableAutoRead();
    }

    @Test
    void shouldNotDisableAutoReadWhenSummaryRequested()
    {
        Connection connection = connectionMock();
        List<String> keys = asList( "key1", "key2" );
        LegacyPullAllResponseHandler handler = newHandler( keys, connection );

        CompletableFuture<ResultSummary> summaryFuture = handler.summaryAsync().toCompletableFuture();
        assertFalse( summaryFuture.isDone() );

        int recordCount = LegacyPullAllResponseHandler.RECORD_BUFFER_HIGH_WATERMARK + 10;
        for ( int i = 0; i < recordCount; i++ )
        {
            handler.onRecord( values( "a", "b" ) );
        }

        verify( connection, never() ).disableAutoRead();

        handler.onSuccess( emptyMap() );
        assertTrue( summaryFuture.isDone() );

        ResultSummary summary = await( summaryFuture );
        assertNotNull( summary );

        for ( int i = 0; i < recordCount; i++ )
        {
            Record record = await( handler.nextAsync() );
            assertNotNull( record );
            assertEquals( keys, record.keys() );
            assertEquals( "a", record.get( "key1" ).asString() );
            assertEquals( "b", record.get( "key2" ).asString() );
        }

        assertNull( await( handler.nextAsync() ) );
    }

    @Test
    void shouldNotDisableAutoReadWhenFailureRequested()
    {
        Connection connection = connectionMock();
        List<String> keys = asList( "key1", "key2" );
        LegacyPullAllResponseHandler handler = newHandler( keys, connection );

        CompletableFuture<Throwable> failureFuture = handler.failureAsync().toCompletableFuture();
        assertFalse( failureFuture.isDone() );

        int recordCount = LegacyPullAllResponseHandler.RECORD_BUFFER_HIGH_WATERMARK + 5;
        for ( int i = 0; i < recordCount; i++ )
        {
            handler.onRecord( values( 123, 456 ) );
        }

        verify( connection, never() ).disableAutoRead();

        IllegalStateException error = new IllegalStateException( "Wrong config" );
        handler.onFailure( error );

        assertTrue( failureFuture.isDone() );
        assertEquals( error, await( failureFuture ) );

        for ( int i = 0; i < recordCount; i++ )
        {
            Record record = await( handler.nextAsync() );
            assertNotNull( record );
            assertEquals( keys, record.keys() );
            assertEquals( 123, record.get( "key1" ).asInt() );
            assertEquals( 456, record.get( "key2" ).asInt() );
        }

        assertNull( await( handler.nextAsync() ) );
    }

    @Test
    void shouldEnableAutoReadOnConnectionWhenFailureRequestedButNotAvailable() throws Exception
    {
        Connection connection = connectionMock();
        LegacyPullAllResponseHandler handler = newHandler( asList( "key1", "key2" ), connection );

        handler.onRecord( values( 1, 2 ) );
        handler.onRecord( values( 3, 4 ) );

        verify( connection, never() ).enableAutoRead();
        verify( connection, never() ).disableAutoRead();

        CompletableFuture<Throwable> failureFuture = handler.failureAsync().toCompletableFuture();
        assertFalse( failureFuture.isDone() );

        verify( connection ).enableAutoRead();
        verify( connection, never() ).disableAutoRead();

        assertNotNull( await( handler.nextAsync() ) );
        assertNotNull( await( handler.nextAsync() ) );

        RuntimeException error = new RuntimeException( "Oh my!" );
        handler.onFailure( error );

        assertTrue( failureFuture.isDone() );
        assertEquals( error, failureFuture.get() );
    }

    @Test
    void shouldNotDisableAutoReadWhenAutoReadManagementDisabled()
    {
        Connection connection = connectionMock();
        LegacyPullAllResponseHandler handler = newHandler( asList( "key1", "key2" ), connection );
        handler.disableAutoReadManagement();

        for ( int i = 0; i < LegacyPullAllResponseHandler.RECORD_BUFFER_HIGH_WATERMARK + 1; i++ )
        {
            handler.onRecord( values( 100, 200 ) );
        }

        verify( connection, never() ).disableAutoRead();
    }

    @Test
    void shouldReturnEmptyListInListAsyncAfterFailure()
    {
        LegacyPullAllResponseHandler handler = newHandler();

        RuntimeException error = new RuntimeException( "Hi" );
        handler.onFailure( error );

        // consume the error
        assertEquals( error, await( handler.failureAsync() ) );
        assertEquals( emptyList(), await( handler.listAsync( Function.identity() ) ) );
    }

    protected LegacyPullAllResponseHandler newHandler( Statement statement, List<String> statementKeys,
            Connection connection )
    {
        RunResponseHandler runResponseHandler = new RunResponseHandler( new CompletableFuture<>(), METADATA_EXTRACTOR );
        runResponseHandler.onSuccess( singletonMap( "fields", value( statementKeys ) ) );
        return new LegacyPullAllResponseHandler( statement, runResponseHandler, connection, METADATA_EXTRACTOR, mock( PullResponseCompletionListener.class ) );
    }

    private static void assertNoRecordsCanBeFetched( LegacyPullAllResponseHandler handler )
    {
        assertNull( await( handler.peekAsync() ) );
        assertNull( await( handler.nextAsync() ) );
        assertEquals( emptyList(), await( handler.listAsync( Function.identity() ) ) );
    }
}
