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
import java.nio.channels.ClosedChannelException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.ServerVersion;
import org.neo4j.driver.Record;
import org.neo4j.driver.Statement;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.summary.StatementType;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.messaging.v1.BoltProtocolV1.METADATA_EXTRACTOR;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.Values.values;
import static org.neo4j.driver.util.TestUtil.anyServerVersion;
import static org.neo4j.driver.util.TestUtil.await;

class AbstractPullAllResponseHandlerTest
{
    @Test
    void shouldReturnNoFailureWhenAlreadySucceeded()
    {
        AbstractPullAllResponseHandler handler = newHandler();
        handler.onSuccess( emptyMap() );

        Throwable failure = await( handler.failureAsync() );

        assertNull( failure );
    }

    @Test
    void shouldReturnNoFailureWhenSucceededAfterFailureRequested()
    {
        AbstractPullAllResponseHandler handler = newHandler();

        CompletableFuture<Throwable> failureFuture = handler.failureAsync().toCompletableFuture();
        assertFalse( failureFuture.isDone() );

        handler.onSuccess( emptyMap() );

        assertTrue( failureFuture.isDone() );
        assertNull( await( failureFuture ) );
    }

    @Test
    void shouldReturnFailureWhenAlreadyFailed()
    {
        AbstractPullAllResponseHandler handler = newHandler();

        RuntimeException failure = new RuntimeException( "Ops" );
        handler.onFailure( failure );

        Throwable receivedFailure = await( handler.failureAsync() );
        assertEquals( failure, receivedFailure );
    }

    @Test
    void shouldReturnFailureWhenFailedAfterFailureRequested()
    {
        AbstractPullAllResponseHandler handler = newHandler();

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
        AbstractPullAllResponseHandler handler = newHandler();

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
        AbstractPullAllResponseHandler handler = newHandler();

        ServiceUnavailableException failure = new ServiceUnavailableException( "Connection terminated" );
        handler.onFailure( failure );

        assertEquals( failure, await( handler.failureAsync() ) );
        assertNull( await( handler.failureAsync() ) );
    }

    @Test
    void shouldReturnFailureOnlyOnceWhenFailedAfterFailureRequested()
    {
        AbstractPullAllResponseHandler handler = newHandler();

        CompletionStage<Throwable> failureFuture = handler.failureAsync();

        SessionExpiredException failure = new SessionExpiredException( "Network unreachable" );
        handler.onFailure( failure );
        assertEquals( failure, await( failureFuture ) );

        assertNull( await( handler.failureAsync() ) );
    }

    @Test
    void shouldReturnSummaryWhenAlreadySucceeded()
    {
        Statement statement = new Statement( "CREATE () RETURN 42" );
        AbstractPullAllResponseHandler handler = newHandler( statement );
        handler.onSuccess( singletonMap( "type", value( "rw" ) ) );

        ResultSummary summary = await( handler.summaryAsync() );

        assertEquals( statement, summary.statement() );
        assertEquals( StatementType.READ_WRITE, summary.statementType() );
    }

    @Test
    void shouldReturnSummaryWhenSucceededAfterSummaryRequested()
    {
        Statement statement = new Statement( "RETURN 'Hi!" );
        AbstractPullAllResponseHandler handler = newHandler( statement );

        CompletableFuture<ResultSummary> summaryFuture = handler.summaryAsync().toCompletableFuture();
        assertFalse( summaryFuture.isDone() );

        handler.onSuccess( singletonMap( "type", value( "r" ) ) );

        assertTrue( summaryFuture.isDone() );
        ResultSummary summary = await( summaryFuture );

        assertEquals( statement, summary.statement() );
        assertEquals( StatementType.READ_ONLY, summary.statementType() );
    }

    @Test
    void shouldReturnFailureWhenSummaryRequestedWhenAlreadyFailed()
    {
        AbstractPullAllResponseHandler handler = newHandler();

        RuntimeException failure = new RuntimeException( "Computer is burning" );
        handler.onFailure( failure );

        RuntimeException e = assertThrows( RuntimeException.class, () -> await( handler.summaryAsync() ) );
        assertEquals( failure, e );
    }

    @Test
    void shouldReturnFailureWhenFailedAfterSummaryRequested()
    {
        AbstractPullAllResponseHandler handler = newHandler();

        CompletableFuture<ResultSummary> summaryFuture = handler.summaryAsync().toCompletableFuture();
        assertFalse( summaryFuture.isDone() );

        IOException failure = new IOException( "FAILED to write" );
        handler.onFailure( failure );

        assertTrue( summaryFuture.isDone() );
        Exception e = assertThrows( Exception.class, () -> await( summaryFuture ) );
        assertEquals( failure, e );
    }

    @Test
    void shouldFailSummaryWhenRequestedMultipleTimes()
    {
        AbstractPullAllResponseHandler handler = newHandler();

        CompletableFuture<ResultSummary> summaryFuture1 = handler.summaryAsync().toCompletableFuture();
        CompletableFuture<ResultSummary> summaryFuture2 = handler.summaryAsync().toCompletableFuture();
        assertFalse( summaryFuture1.isDone() );
        assertFalse( summaryFuture2.isDone() );

        ClosedChannelException failure = new ClosedChannelException();
        handler.onFailure( failure );

        assertTrue( summaryFuture1.isDone() );
        assertTrue( summaryFuture2.isDone() );

        Exception e1 = assertThrows( Exception.class, () -> await( summaryFuture1 ) );
        assertEquals( failure, e1 );

        Exception e2 = assertThrows( Exception.class, () -> await( summaryFuture2 ) );
        assertEquals( failure, e2 );
    }

    @Test
    void shouldPropagateFailureOnlyOnceFromSummary()
    {
        Statement statement = new Statement( "CREATE INDEX ON :Person(name)" );
        AbstractPullAllResponseHandler handler = newHandler( statement );

        IllegalStateException failure = new IllegalStateException( "Some state is illegal :(" );
        handler.onFailure( failure );

        RuntimeException e = assertThrows( RuntimeException.class, () -> await( handler.summaryAsync() ) );
        assertEquals( failure, e );

        ResultSummary summary = await( handler.summaryAsync() );
        assertNotNull( summary );
        assertEquals( statement, summary.statement() );
    }

    @Test
    void shouldReturnSummaryWhenAlreadyFailedAndFailureConsumed()
    {
        Statement statement = new Statement( "CREATE ()" );
        AbstractPullAllResponseHandler handler = newHandler( statement );

        ServiceUnavailableException failure = new ServiceUnavailableException( "Neo4j unreachable" );
        handler.onFailure( failure );

        assertEquals( failure, await( handler.failureAsync() ) );

        ResultSummary summary = await( handler.summaryAsync() );
        assertNotNull( summary );
        assertEquals( statement, summary.statement() );
    }

    @Test
    void shouldPeekSingleAvailableRecord()
    {
        List<String> keys = asList( "key1", "key2" );
        AbstractPullAllResponseHandler handler = newHandler( keys );
        handler.onRecord( values( "a", "b" ) );

        Record record = await( handler.peekAsync() );

        assertEquals( keys, record.keys() );
        assertEquals( "a", record.get( "key1" ).asString() );
        assertEquals( "b", record.get( "key2" ).asString() );
    }

    @Test
    void shouldPeekFirstRecordWhenMultipleAvailable()
    {
        List<String> keys = asList( "key1", "key2", "key3" );
        AbstractPullAllResponseHandler handler = newHandler( keys );

        handler.onRecord( values( "a1", "b1", "c1" ) );
        handler.onRecord( values( "a2", "b2", "c2" ) );
        handler.onRecord( values( "a3", "b3", "c3" ) );

        Record record = await( handler.peekAsync() );

        assertEquals( keys, record.keys() );
        assertEquals( "a1", record.get( "key1" ).asString() );
        assertEquals( "b1", record.get( "key2" ).asString() );
        assertEquals( "c1", record.get( "key3" ).asString() );
    }

    @Test
    void shouldPeekRecordThatBecomesAvailableLater()
    {
        List<String> keys = asList( "key1", "key2" );
        AbstractPullAllResponseHandler handler = newHandler( keys );

        CompletableFuture<Record> recordFuture = handler.peekAsync().toCompletableFuture();
        assertFalse( recordFuture.isDone() );

        handler.onRecord( values( 24, 42 ) );
        assertTrue( recordFuture.isDone() );

        Record record = await( recordFuture );
        assertEquals( keys, record.keys() );
        assertEquals( 24, record.get( "key1" ).asInt() );
        assertEquals( 42, record.get( "key2" ).asInt() );
    }

    @Test
    void shouldPeekAvailableNothingAfterSuccess()
    {
        List<String> keys = asList( "key1", "key2", "key3" );
        AbstractPullAllResponseHandler handler = newHandler( keys );

        handler.onRecord( values( 1, 2, 3 ) );
        handler.onSuccess( emptyMap() );

        Record record = await( handler.peekAsync() );
        assertEquals( keys, record.keys() );
        assertEquals( 1, record.get( "key1" ).asInt() );
        assertEquals( 2, record.get( "key2" ).asInt() );
        assertEquals( 3, record.get( "key3" ).asInt() );
    }

    @Test
    void shouldPeekNothingAfterSuccess()
    {
        AbstractPullAllResponseHandler handler = newHandler();
        handler.onSuccess( emptyMap() );

        assertNull( await( handler.peekAsync() ) );
    }

    @Test
    void shouldPeekWhenRequestedMultipleTimes()
    {
        List<String> keys = asList( "key1", "key2" );
        AbstractPullAllResponseHandler handler = newHandler( keys );

        CompletableFuture<Record> recordFuture1 = handler.peekAsync().toCompletableFuture();
        CompletableFuture<Record> recordFuture2 = handler.peekAsync().toCompletableFuture();
        CompletableFuture<Record> recordFuture3 = handler.peekAsync().toCompletableFuture();

        assertFalse( recordFuture1.isDone() );
        assertFalse( recordFuture2.isDone() );
        assertFalse( recordFuture3.isDone() );

        handler.onRecord( values( 2, 1 ) );

        assertTrue( recordFuture1.isDone() );
        assertTrue( recordFuture2.isDone() );
        assertTrue( recordFuture3.isDone() );

        Record record1 = await( recordFuture1 );
        Record record2 = await( recordFuture2 );
        Record record3 = await( recordFuture3 );

        assertEquals( keys, record1.keys() );
        assertEquals( keys, record2.keys() );
        assertEquals( keys, record3.keys() );

        assertEquals( 2, record1.get( "key1" ).asInt() );
        assertEquals( 1, record1.get( "key2" ).asInt() );

        assertEquals( 2, record2.get( "key1" ).asInt() );
        assertEquals( 1, record2.get( "key2" ).asInt() );

        assertEquals( 2, record3.get( "key1" ).asInt() );
        assertEquals( 1, record3.get( "key2" ).asInt() );
    }

    @Test
    void shouldPropagateNotConsumedFailureInPeek()
    {
        AbstractPullAllResponseHandler handler = newHandler();

        RuntimeException failure = new RuntimeException( "Something is wrong" );
        handler.onFailure( failure );

        RuntimeException e = assertThrows( RuntimeException.class, () -> await( handler.peekAsync() ) );
        assertEquals( failure, e );
    }

    @Test
    void shouldPropagateFailureInPeekWhenItBecomesAvailable()
    {
        AbstractPullAllResponseHandler handler = newHandler();

        CompletableFuture<Record> recordFuture = handler.peekAsync().toCompletableFuture();
        assertFalse( recordFuture.isDone() );

        RuntimeException failure = new RuntimeException( "Error" );
        handler.onFailure( failure );

        RuntimeException e = assertThrows( RuntimeException.class, () -> await( recordFuture ) );
        assertEquals( failure, e );
    }

    @Test
    void shouldPropagateFailureInPeekOnlyOnce()
    {
        AbstractPullAllResponseHandler handler = newHandler();

        RuntimeException failure = new RuntimeException( "Something is wrong" );
        handler.onFailure( failure );

        RuntimeException e = assertThrows( RuntimeException.class, () -> await( handler.peekAsync() ) );
        assertEquals( failure, e );
        assertNull( await( handler.peekAsync() ) );
    }

    @Test
    void shouldReturnSingleAvailableRecordInNextAsync()
    {
        List<String> keys = asList( "key1", "key2" );
        AbstractPullAllResponseHandler handler = newHandler( keys );
        handler.onRecord( values( "1", "2" ) );

        Record record = await( handler.nextAsync() );

        assertNotNull( record );
        assertEquals( keys, record.keys() );
        assertEquals( "1", record.get( "key1" ).asString() );
        assertEquals( "2", record.get( "key2" ).asString() );
    }

    @Test
    void shouldReturnNoRecordsWhenNoneAvailableInNextAsync()
    {
        AbstractPullAllResponseHandler handler = newHandler( asList( "key1", "key2" ) );
        handler.onSuccess( emptyMap() );

        assertNull( await( handler.nextAsync() ) );
    }

    @Test
    void shouldReturnNoRecordsWhenSuccessComesAfterNextAsync()
    {
        AbstractPullAllResponseHandler handler = newHandler( asList( "key1", "key2" ) );

        CompletableFuture<Record> recordFuture = handler.nextAsync().toCompletableFuture();
        assertFalse( recordFuture.isDone() );

        handler.onSuccess( emptyMap() );
        assertTrue( recordFuture.isDone() );

        assertNull( await( recordFuture ) );
    }

    @Test
    void shouldPullAllAvailableRecordsWithNextAsync()
    {
        List<String> keys = asList( "key1", "key2", "key3" );
        AbstractPullAllResponseHandler handler = newHandler( keys );

        handler.onRecord( values( 1, 2, 3 ) );
        handler.onRecord( values( 11, 22, 33 ) );
        handler.onRecord( values( 111, 222, 333 ) );
        handler.onRecord( values( 1111, 2222, 3333 ) );
        handler.onSuccess( emptyMap() );

        Record record1 = await( handler.nextAsync() );
        assertNotNull( record1 );
        assertEquals( keys, record1.keys() );
        assertEquals( 1, record1.get( "key1" ).asInt() );
        assertEquals( 2, record1.get( "key2" ).asInt() );
        assertEquals( 3, record1.get( "key3" ).asInt() );

        Record record2 = await( handler.nextAsync() );
        assertNotNull( record2 );
        assertEquals( keys, record2.keys() );
        assertEquals( 11, record2.get( "key1" ).asInt() );
        assertEquals( 22, record2.get( "key2" ).asInt() );
        assertEquals( 33, record2.get( "key3" ).asInt() );

        Record record3 = await( handler.nextAsync() );
        assertNotNull( record3 );
        assertEquals( keys, record3.keys() );
        assertEquals( 111, record3.get( "key1" ).asInt() );
        assertEquals( 222, record3.get( "key2" ).asInt() );
        assertEquals( 333, record3.get( "key3" ).asInt() );

        Record record4 = await( handler.nextAsync() );
        assertNotNull( record4 );
        assertEquals( keys, record4.keys() );
        assertEquals( 1111, record4.get( "key1" ).asInt() );
        assertEquals( 2222, record4.get( "key2" ).asInt() );
        assertEquals( 3333, record4.get( "key3" ).asInt() );

        assertNull( await( handler.nextAsync() ) );
        assertNull( await( handler.nextAsync() ) );
    }

    @Test
    void shouldReturnRecordInNextAsyncWhenItBecomesAvailableLater()
    {
        List<String> keys = asList( "key1", "key2" );
        AbstractPullAllResponseHandler handler = newHandler( keys );

        CompletableFuture<Record> recordFuture = handler.nextAsync().toCompletableFuture();
        assertFalse( recordFuture.isDone() );

        handler.onRecord( values( 24, 42 ) );
        assertTrue( recordFuture.isDone() );

        Record record = await( recordFuture );
        assertNotNull( record );
        assertEquals( keys, record.keys() );
        assertEquals( 24, record.get( "key1" ).asInt() );
        assertEquals( 42, record.get( "key2" ).asInt() );
    }

    @Test
    void shouldReturnSameRecordOnceWhenRequestedMultipleTimesInNextAsync()
    {
        List<String> keys = asList( "key1", "key2" );
        AbstractPullAllResponseHandler handler = newHandler( keys );

        CompletableFuture<Record> recordFuture1 = handler.nextAsync().toCompletableFuture();
        CompletableFuture<Record> recordFuture2 = handler.nextAsync().toCompletableFuture();
        assertFalse( recordFuture1.isDone() );
        assertFalse( recordFuture2.isDone() );

        handler.onRecord( values( "A", "B" ) );
        assertTrue( recordFuture1.isDone() );
        assertTrue( recordFuture2.isDone() );

        Record record1 = await( recordFuture1 );
        Record record2 = await( recordFuture2 );

        // record should be returned only once because #nextAsync() polls it
        assertTrue( record1 != null || record2 != null );
        Record record = record1 != null ? record1 : record2;

        assertNotNull( record );
        assertEquals( keys, record.keys() );
        assertEquals( "A", record.get( "key1" ).asString() );
        assertEquals( "B", record.get( "key2" ).asString() );
    }

    @Test
    void shouldPropagateExistingFailureInNextAsync()
    {
        AbstractPullAllResponseHandler handler = newHandler();
        RuntimeException error = new RuntimeException( "FAILED to read" );
        handler.onFailure( error );

        RuntimeException e = assertThrows( RuntimeException.class, () -> await( handler.nextAsync() ) );
        assertEquals( error, e );
    }

    @Test
    void shouldPropagateFailureInNextAsyncWhenFailureMessagesArrivesLater()
    {
        AbstractPullAllResponseHandler handler = newHandler();

        CompletableFuture<Record> recordFuture = handler.nextAsync().toCompletableFuture();
        assertFalse( recordFuture.isDone() );

        RuntimeException error = new RuntimeException( "Network failed" );
        handler.onFailure( error );

        assertTrue( recordFuture.isDone() );
        RuntimeException e = assertThrows( RuntimeException.class, () -> await( recordFuture ) );
        assertEquals( error, e );
    }

    @Test
    void shouldDisableAutoReadWhenTooManyRecordsArrive()
    {
        Connection connection = connectionMock();
        AbstractPullAllResponseHandler handler = newHandler( asList( "key1", "key2" ), connection );

        for ( int i = 0; i < AbstractPullAllResponseHandler.RECORD_BUFFER_HIGH_WATERMARK + 1; i++ )
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
        AbstractPullAllResponseHandler handler = newHandler( keys, connection );

        int i;
        for ( i = 0; i < AbstractPullAllResponseHandler.RECORD_BUFFER_HIGH_WATERMARK + 1; i++ )
        {
            handler.onRecord( values( 100, 200 ) );
        }

        verify( connection, never() ).enableAutoRead();
        verify( connection ).disableAutoRead();

        while ( i-- > AbstractPullAllResponseHandler.RECORD_BUFFER_LOW_WATERMARK - 1 )
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
        AbstractPullAllResponseHandler handler = newHandler( keys, connection );

        CompletableFuture<ResultSummary> summaryFuture = handler.summaryAsync().toCompletableFuture();
        assertFalse( summaryFuture.isDone() );

        int recordCount = AbstractPullAllResponseHandler.RECORD_BUFFER_HIGH_WATERMARK + 10;
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
        AbstractPullAllResponseHandler handler = newHandler( keys, connection );

        CompletableFuture<Throwable> failureFuture = handler.failureAsync().toCompletableFuture();
        assertFalse( failureFuture.isDone() );

        int recordCount = AbstractPullAllResponseHandler.RECORD_BUFFER_HIGH_WATERMARK + 5;
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
        AbstractPullAllResponseHandler handler = newHandler( asList( "key1", "key2" ), connection );

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
    void shouldEnableAutoReadOnConnectionWhenSummaryRequestedButNotAvailable() throws Exception
    {
        Connection connection = connectionMock();
        AbstractPullAllResponseHandler handler = newHandler( asList( "key1", "key2", "key3" ), connection );

        handler.onRecord( values( 1, 2, 3 ) );
        handler.onRecord( values( 4, 5, 6 ) );

        verify( connection, never() ).enableAutoRead();
        verify( connection, never() ).disableAutoRead();

        CompletableFuture<ResultSummary> summaryFuture = handler.summaryAsync().toCompletableFuture();
        assertFalse( summaryFuture.isDone() );

        verify( connection ).enableAutoRead();
        verify( connection, never() ).disableAutoRead();

        assertNotNull( await( handler.nextAsync() ) );
        assertNotNull( await( handler.nextAsync() ) );

        handler.onSuccess( emptyMap() );

        assertTrue( summaryFuture.isDone() );
        assertNotNull( summaryFuture.get() );
    }

    @Test
    void shouldNotDisableAutoReadWhenAutoReadManagementDisabled()
    {
        Connection connection = connectionMock();
        AbstractPullAllResponseHandler handler = newHandler( asList( "key1", "key2" ), connection );
        handler.disableAutoReadManagement();

        for ( int i = 0; i < AbstractPullAllResponseHandler.RECORD_BUFFER_HIGH_WATERMARK + 1; i++ )
        {
            handler.onRecord( values( 100, 200 ) );
        }

        verify( connection, never() ).disableAutoRead();
    }

    @Test
    void shouldPropagateFailureFromListAsync()
    {
        AbstractPullAllResponseHandler handler = newHandler();
        RuntimeException error = new RuntimeException( "Hi!" );
        handler.onFailure( error );

        RuntimeException e = assertThrows( RuntimeException.class, () -> await( handler.listAsync( Function.identity() ) ) );
        assertEquals( error, e );
    }

    @Test
    void shouldPropagateFailureAfterRecordFromListAsync()
    {
        AbstractPullAllResponseHandler handler = newHandler( asList( "key1", "key2" ) );

        handler.onRecord( values( "a", "b" ) );

        RuntimeException error = new RuntimeException( "Hi!" );
        handler.onFailure( error );

        RuntimeException e = assertThrows( RuntimeException.class, () -> await( handler.listAsync( Function.identity() ) ) );
        assertEquals( error, e );
    }

    @Test
    void shouldFailListAsyncWhenTransformationFunctionThrows()
    {
        AbstractPullAllResponseHandler handler = newHandler( asList( "key1", "key2" ) );
        handler.onRecord( values( 1, 2 ) );
        handler.onRecord( values( 3, 4 ) );
        handler.onSuccess( emptyMap() );

        RuntimeException error = new RuntimeException( "Hi!" );

        CompletionStage<List<Integer>> stage = handler.listAsync( record ->
        {
            if ( record.get( 1 ).asInt() == 4 )
            {
                throw error;
            }
            return 42;
        } );

        RuntimeException e = assertThrows( RuntimeException.class, () -> await( stage ) );
        assertEquals( error, e );
    }

    @Test
    void shouldReturnEmptyListInListAsyncAfterSuccess()
    {
        AbstractPullAllResponseHandler handler = newHandler();

        handler.onSuccess( emptyMap() );

        assertEquals( emptyList(), await( handler.listAsync( Function.identity() ) ) );
    }

    @Test
    void shouldReturnEmptyListInListAsyncAfterFailure()
    {
        AbstractPullAllResponseHandler handler = newHandler();

        RuntimeException error = new RuntimeException( "Hi" );
        handler.onFailure( error );

        // consume the error
        assertEquals( error, await( handler.failureAsync() ) );
        assertEquals( emptyList(), await( handler.listAsync( Function.identity() ) ) );
    }

    @Test
    void shouldReturnTransformedListInListAsync()
    {
        AbstractPullAllResponseHandler handler = newHandler( singletonList( "key1" ) );

        handler.onRecord( values( 1 ) );
        handler.onRecord( values( 2 ) );
        handler.onRecord( values( 3 ) );
        handler.onRecord( values( 4 ) );
        handler.onSuccess( emptyMap() );

        List<Integer> transformedList = await( handler.listAsync( record -> record.get( 0 ).asInt() * 2 ) );

        assertEquals( asList( 2, 4, 6, 8 ), transformedList );
    }

    @Test
    void shouldReturnNotTransformedListInListAsync()
    {
        List<String> keys = asList( "key1", "key2" );
        AbstractPullAllResponseHandler handler = newHandler( keys );

        Value[] fields1 = values( "a", "b" );
        Value[] fields2 = values( "c", "d" );
        Value[] fields3 = values( "e", "f" );

        handler.onRecord( fields1 );
        handler.onRecord( fields2 );
        handler.onRecord( fields3 );
        handler.onSuccess( emptyMap() );

        List<Record> list = await( handler.listAsync( Function.identity() ) );

        List<InternalRecord> expectedRecords = asList(
                new InternalRecord( keys, fields1 ),
                new InternalRecord( keys, fields2 ),
                new InternalRecord( keys, fields3 ) );

        assertEquals( expectedRecords, list );
    }

    @Test
    void shouldConsumeAfterSuccessWithRecords()
    {
        AbstractPullAllResponseHandler handler = newHandler( singletonList( "key1" ) );
        handler.onRecord( values( 1 ) );
        handler.onRecord( values( 2 ) );
        handler.onSuccess( emptyMap() );

        assertNotNull( await( handler.consumeAsync() ) );

        assertNoRecordsCanBeFetched( handler );
    }

    @Test
    void shouldConsumeAfterSuccessWithoutRecords()
    {
        AbstractPullAllResponseHandler handler = newHandler();
        handler.onSuccess( emptyMap() );

        assertNotNull( await( handler.consumeAsync() ) );

        assertNoRecordsCanBeFetched( handler );
    }

    @Test
    void shouldConsumeAfterFailureWithRecords()
    {
        AbstractPullAllResponseHandler handler = newHandler( singletonList( "key1" ) );
        handler.onRecord( values( 1 ) );
        handler.onRecord( values( 2 ) );
        RuntimeException error = new RuntimeException( "Hi" );
        handler.onFailure( error );

        RuntimeException e = assertThrows( RuntimeException.class, () -> await( handler.consumeAsync() ) );
        assertEquals( error, e );
        assertNoRecordsCanBeFetched( handler );
    }

    @Test
    void shouldConsumeAfterFailureWithoutRecords()
    {
        AbstractPullAllResponseHandler handler = newHandler();
        RuntimeException error = new RuntimeException( "Hi" );
        handler.onFailure( error );

        RuntimeException e = assertThrows( RuntimeException.class, () -> await( handler.consumeAsync() ) );
        assertEquals( error, e );
        assertNoRecordsCanBeFetched( handler );
    }

    @Test
    void shouldConsumeAfterProcessedFailureWithRecords()
    {
        AbstractPullAllResponseHandler handler = newHandler( singletonList( "key1" ) );
        handler.onRecord( values( 1 ) );
        handler.onRecord( values( 2 ) );
        RuntimeException error = new RuntimeException( "Hi" );
        handler.onFailure( error );

        // process failure
        assertEquals( error, await( handler.failureAsync() ) );
        // consume all buffered records
        assertNotNull( await( handler.consumeAsync() ) );

        assertNoRecordsCanBeFetched( handler );
    }

    @Test
    void shouldConsumeAfterProcessedFailureWithoutRecords()
    {
        AbstractPullAllResponseHandler handler = newHandler();
        RuntimeException error = new RuntimeException( "Hi" );
        handler.onFailure( error );

        // process failure
        assertEquals( error, await( handler.failureAsync() ) );
        // consume all buffered records
        assertNotNull( await( handler.consumeAsync() ) );

        assertNoRecordsCanBeFetched( handler );
    }

    @Test
    void shouldConsumeUntilSuccess()
    {
        AbstractPullAllResponseHandler handler = newHandler( asList( "key1", "key2" ) );
        handler.onRecord( values( 1, 2 ) );
        handler.onRecord( values( 3, 4 ) );

        CompletableFuture<ResultSummary> consumeFuture = handler.consumeAsync().toCompletableFuture();
        assertFalse( consumeFuture.isDone() );

        handler.onRecord( values( 5, 6 ) );
        handler.onRecord( values( 7, 8 ) );
        assertFalse( consumeFuture.isDone() );

        handler.onSuccess( emptyMap() );

        assertTrue( consumeFuture.isDone() );
        assertNotNull( await( consumeFuture ) );

        assertNoRecordsCanBeFetched( handler );
    }

    @Test
    void shouldConsumeUntilFailure()
    {
        AbstractPullAllResponseHandler handler = newHandler( asList( "key1", "key2" ) );
        handler.onRecord( values( 1, 2 ) );
        handler.onRecord( values( 3, 4 ) );

        CompletableFuture<ResultSummary> consumeFuture = handler.consumeAsync().toCompletableFuture();
        assertFalse( consumeFuture.isDone() );

        handler.onRecord( values( 5, 6 ) );
        handler.onRecord( values( 7, 8 ) );
        assertFalse( consumeFuture.isDone() );

        RuntimeException error = new RuntimeException( "Hi" );
        handler.onFailure( error );

        assertTrue( consumeFuture.isDone() );
        assertTrue( consumeFuture.isCompletedExceptionally() );
        RuntimeException e = assertThrows( RuntimeException.class, () -> await( consumeFuture ) );
        assertEquals( error, e );
        assertNoRecordsCanBeFetched( handler );
    }

    @Test
    void shouldReturnNoRecordsWhenConsumed()
    {
        AbstractPullAllResponseHandler handler = newHandler( asList( "key1", "key2" ) );
        handler.onRecord( values( 1, 2 ) );
        handler.onRecord( values( 3, 4 ) );

        CompletableFuture<ResultSummary> consumeFuture = handler.consumeAsync().toCompletableFuture();
        assertFalse( consumeFuture.isDone() );

        CompletionStage<Record> peekStage1 = handler.peekAsync();
        CompletionStage<Record> nextStage1 = handler.nextAsync();

        handler.onRecord( values( 5, 6 ) );
        handler.onRecord( values( 7, 8 ) );

        CompletionStage<Record> peekStage2 = handler.peekAsync();
        CompletionStage<Record> nextStage2 = handler.nextAsync();
        assertFalse( consumeFuture.isDone() );

        handler.onSuccess( emptyMap() );

        assertNull( await( peekStage1 ) );
        assertNull( await( nextStage1 ) );
        assertNull( await( peekStage2 ) );
        assertNull( await( nextStage2 ) );

        assertTrue( consumeFuture.isDone() );
        assertNotNull( await( consumeFuture ) );
    }

    @Test
    void shouldReceiveSummaryAfterConsume()
    {
        Statement statement = new Statement( "RETURN 'Hello!'" );
        AbstractPullAllResponseHandler handler = newHandler( statement, singletonList( "key" ) );
        handler.onRecord( values( "Hi!" ) );
        handler.onSuccess( singletonMap( "type", value( "rw" ) ) );

        ResultSummary summary1 = await( handler.consumeAsync() );
        assertEquals( statement.text(), summary1.statement().text() );
        assertEquals( StatementType.READ_WRITE, summary1.statementType() );

        ResultSummary summary2 = await( handler.summaryAsync() );
        assertEquals( statement.text(), summary2.statement().text() );
        assertEquals( StatementType.READ_WRITE, summary2.statementType() );
    }

    private static AbstractPullAllResponseHandler newHandler()
    {
        return newHandler( new Statement( "RETURN 1" ) );
    }

    private static AbstractPullAllResponseHandler newHandler( Statement statement )
    {
        return newHandler( statement, emptyList() );
    }

    private static AbstractPullAllResponseHandler newHandler( List<String> statementKeys )
    {
        return newHandler( new Statement( "RETURN 1" ), statementKeys, connectionMock() );
    }

    private static AbstractPullAllResponseHandler newHandler( Statement statement, List<String> statementKeys )
    {
        return newHandler( statement, statementKeys, connectionMock() );
    }

    private static AbstractPullAllResponseHandler newHandler( List<String> statementKeys, Connection connection )
    {
        return newHandler( new Statement( "RETURN 1" ), statementKeys, connection );
    }

    private static AbstractPullAllResponseHandler newHandler( Statement statement, List<String> statementKeys,
            Connection connection )
    {
        RunResponseHandler runResponseHandler = new RunResponseHandler( new CompletableFuture<>(), METADATA_EXTRACTOR );
        runResponseHandler.onSuccess( singletonMap( "fields", value( statementKeys ) ) );
        return new TestPullAllResponseHandler( statement, runResponseHandler, connection );
    }

    private static Connection connectionMock()
    {
        Connection connection = mock( Connection.class );
        when( connection.serverAddress() ).thenReturn( BoltServerAddress.LOCAL_DEFAULT );
        when( connection.serverVersion() ).thenReturn( anyServerVersion() );
        return connection;
    }

    private static void assertNoRecordsCanBeFetched( AbstractPullAllResponseHandler handler )
    {
        assertNull( await( handler.peekAsync() ) );
        assertNull( await( handler.nextAsync() ) );
        assertEquals( emptyList(), await( handler.listAsync( Function.identity() ) ) );
    }

    private static class TestPullAllResponseHandler extends AbstractPullAllResponseHandler
    {
        public TestPullAllResponseHandler( Statement statement, RunResponseHandler runResponseHandler, Connection connection )
        {
            super( statement, runResponseHandler, connection, METADATA_EXTRACTOR );
        }

        @Override
        protected void afterSuccess( Map<String,Value> metadata )
        {
        }

        @Override
        protected void afterFailure( Throwable error )
        {
        }
    }
}
