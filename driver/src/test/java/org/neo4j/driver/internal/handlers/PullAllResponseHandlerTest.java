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

import org.junit.Test;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.ServerVersion;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.summary.StatementType;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.v1.Values.value;
import static org.neo4j.driver.v1.Values.values;
import static org.neo4j.driver.v1.util.TestUtil.await;

public class PullAllResponseHandlerTest
{
    @Test
    public void shouldReturnNoFailureWhenAlreadySucceeded()
    {
        PullAllResponseHandler handler = newHandler();
        handler.onSuccess( emptyMap() );

        Throwable failure = await( handler.failureAsync() );

        assertNull( failure );
    }

    @Test
    public void shouldReturnNoFailureWhenSucceededAfterFailureRequested()
    {
        PullAllResponseHandler handler = newHandler();

        CompletableFuture<Throwable> failureFuture = handler.failureAsync().toCompletableFuture();
        assertFalse( failureFuture.isDone() );

        handler.onSuccess( emptyMap() );

        assertTrue( failureFuture.isDone() );
        assertNull( await( failureFuture ) );
    }

    @Test
    public void shouldReturnFailureWhenAlreadyFailed()
    {
        PullAllResponseHandler handler = newHandler();

        RuntimeException failure = new RuntimeException( "Ops" );
        handler.onFailure( failure );

        Throwable receivedFailure = await( handler.failureAsync() );
        assertEquals( failure, receivedFailure );
    }

    @Test
    public void shouldReturnFailureWhenFailedAfterFailureRequested()
    {
        PullAllResponseHandler handler = newHandler();

        CompletableFuture<Throwable> failureFuture = handler.failureAsync().toCompletableFuture();
        assertFalse( failureFuture.isDone() );

        IOException failure = new IOException( "Broken pipe" );
        handler.onFailure( failure );

        assertTrue( failureFuture.isDone() );
        assertEquals( failure, await( failureFuture ) );
    }

    @Test
    public void shouldReturnFailureWhenRequestedMultipleTimes()
    {
        PullAllResponseHandler handler = newHandler();

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
    public void shouldReturnFailureOnlyOnceWhenFailedBeforeFailureRequested()
    {
        PullAllResponseHandler handler = newHandler();

        ServiceUnavailableException failure = new ServiceUnavailableException( "Connection terminated" );
        handler.onFailure( failure );

        assertEquals( failure, await( handler.failureAsync() ) );
        assertNull( await( handler.failureAsync() ) );
    }

    @Test
    public void shouldReturnFailureOnlyOnceWhenFailedAfterFailureRequested()
    {
        PullAllResponseHandler handler = newHandler();

        CompletionStage<Throwable> failureFuture = handler.failureAsync();

        SessionExpiredException failure = new SessionExpiredException( "Network unreachable" );
        handler.onFailure( failure );
        assertEquals( failure, await( failureFuture ) );

        assertNull( await( handler.failureAsync() ) );
    }

    @Test
    public void shouldReturnSummaryWhenAlreadySucceeded()
    {
        Statement statement = new Statement( "CREATE () RETURN 42" );
        PullAllResponseHandler handler = newHandler( statement );
        handler.onSuccess( singletonMap( "type", value( "rw" ) ) );

        ResultSummary summary = await( handler.summaryAsync() );

        assertEquals( statement, summary.statement() );
        assertEquals( StatementType.READ_WRITE, summary.statementType() );
    }

    @Test
    public void shouldReturnSummaryWhenSucceededAfterSummaryRequested()
    {
        Statement statement = new Statement( "RETURN 'Hi!" );
        PullAllResponseHandler handler = newHandler( statement );

        CompletableFuture<ResultSummary> summaryFuture = handler.summaryAsync().toCompletableFuture();
        assertFalse( summaryFuture.isDone() );

        handler.onSuccess( singletonMap( "type", value( "r" ) ) );

        assertTrue( summaryFuture.isDone() );
        ResultSummary summary = await( summaryFuture );

        assertEquals( statement, summary.statement() );
        assertEquals( StatementType.READ_ONLY, summary.statementType() );
    }

    @Test
    public void shouldReturnFailureWhenSummaryRequestedWhenAlreadyFailed()
    {
        PullAllResponseHandler handler = newHandler();

        RuntimeException failure = new RuntimeException( "Computer is burning" );
        handler.onFailure( failure );

        try
        {
            await( handler.summaryAsync() );
            fail( "Exception expected" );
        }
        catch ( RuntimeException e )
        {
            assertEquals( failure, e );
        }
    }

    @Test
    public void shouldReturnFailureWhenFailedAfterSummaryRequested()
    {
        PullAllResponseHandler handler = newHandler();

        CompletableFuture<ResultSummary> summaryFuture = handler.summaryAsync().toCompletableFuture();
        assertFalse( summaryFuture.isDone() );

        IOException failure = new IOException( "Failed to write" );
        handler.onFailure( failure );

        assertTrue( summaryFuture.isDone() );

        try
        {
            await( summaryFuture );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertEquals( failure, e );
        }
    }

    @Test
    public void shouldFailSummaryWhenRequestedMultipleTimes()
    {
        PullAllResponseHandler handler = newHandler();

        CompletableFuture<ResultSummary> summaryFuture1 = handler.summaryAsync().toCompletableFuture();
        CompletableFuture<ResultSummary> summaryFuture2 = handler.summaryAsync().toCompletableFuture();
        assertFalse( summaryFuture1.isDone() );
        assertFalse( summaryFuture2.isDone() );

        ClosedChannelException failure = new ClosedChannelException();
        handler.onFailure( failure );

        assertTrue( summaryFuture1.isDone() );
        assertTrue( summaryFuture2.isDone() );

        try
        {
            await( summaryFuture1 );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertEquals( failure, e );
        }

        try
        {
            await( summaryFuture2 );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertEquals( failure, e );
        }
    }

    @Test
    public void shouldPropagateFailureOnlyOnceFromSummary()
    {
        Statement statement = new Statement( "CREATE INDEX ON :Person(name)" );
        PullAllResponseHandler handler = newHandler( statement );

        IllegalStateException failure = new IllegalStateException( "Some state is illegal :(" );
        handler.onFailure( failure );

        try
        {
            await( handler.summaryAsync() );
            fail( "Exception expected" );
        }
        catch ( RuntimeException e )
        {
            assertEquals( failure, e );
        }

        ResultSummary summary = await( handler.summaryAsync() );
        assertNotNull( summary );
        assertEquals( statement, summary.statement() );
    }

    @Test
    public void shouldReturnSummaryWhenAlreadyFailedAndFailureConsumed()
    {
        Statement statement = new Statement( "CREATE ()" );
        PullAllResponseHandler handler = newHandler( statement );

        ServiceUnavailableException failure = new ServiceUnavailableException( "Neo4j unreachable" );
        handler.onFailure( failure );

        assertEquals( failure, await( handler.failureAsync() ) );

        ResultSummary summary = await( handler.summaryAsync() );
        assertNotNull( summary );
        assertEquals( statement, summary.statement() );
    }

    @Test
    public void shouldPeekSingleAvailableRecord()
    {
        List<String> keys = asList( "key1", "key2" );
        PullAllResponseHandler handler = newHandler( keys );
        handler.onRecord( values( "a", "b" ) );

        Record record = await( handler.peekAsync() );

        assertEquals( keys, record.keys() );
        assertEquals( "a", record.get( "key1" ).asString() );
        assertEquals( "b", record.get( "key2" ).asString() );
    }

    @Test
    public void shouldPeekFirstRecordWhenMultipleAvailable()
    {
        List<String> keys = asList( "key1", "key2", "key3" );
        PullAllResponseHandler handler = newHandler( keys );

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
    public void shouldPeekRecordThatBecomesAvailableLater()
    {
        List<String> keys = asList( "key1", "key2" );
        PullAllResponseHandler handler = newHandler( keys );

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
    public void shouldPeekAvailableNothingAfterSuccess()
    {
        List<String> keys = asList( "key1", "key2", "key3" );
        PullAllResponseHandler handler = newHandler( keys );

        handler.onRecord( values( 1, 2, 3 ) );
        handler.onSuccess( emptyMap() );

        Record record = await( handler.peekAsync() );
        assertEquals( keys, record.keys() );
        assertEquals( 1, record.get( "key1" ).asInt() );
        assertEquals( 2, record.get( "key2" ).asInt() );
        assertEquals( 3, record.get( "key3" ).asInt() );
    }

    @Test
    public void shouldPeekNothingAfterSuccess()
    {
        PullAllResponseHandler handler = newHandler();
        handler.onSuccess( emptyMap() );

        assertNull( await( handler.peekAsync() ) );
    }

    @Test
    public void shouldPeekWhenRequestedMultipleTimes()
    {
        List<String> keys = asList( "key1", "key2" );
        PullAllResponseHandler handler = newHandler( keys );

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
    public void shouldPropagateNotConsumedFailureInPeek()
    {
        PullAllResponseHandler handler = newHandler();

        RuntimeException failure = new RuntimeException( "Something is wrong" );
        handler.onFailure( failure );

        try
        {
            await( handler.peekAsync() );
            fail( "Exception expected" );
        }
        catch ( RuntimeException e )
        {
            assertEquals( failure, e );
        }
    }

    @Test
    public void shouldPropagateFailureInPeekWhenItBecomesAvailable()
    {
        PullAllResponseHandler handler = newHandler();

        CompletableFuture<Record> recordFuture = handler.peekAsync().toCompletableFuture();
        assertFalse( recordFuture.isDone() );

        RuntimeException failure = new RuntimeException( "Error" );
        handler.onFailure( failure );

        try
        {
            await( recordFuture );
            fail( "Exception expected" );
        }
        catch ( RuntimeException e )
        {
            assertEquals( failure, e );
        }
    }

    @Test
    public void shouldPropagateFailureInPeekOnlyOnce()
    {
        PullAllResponseHandler handler = newHandler();

        RuntimeException failure = new RuntimeException( "Something is wrong" );
        handler.onFailure( failure );

        try
        {
            await( handler.peekAsync() );
            fail( "Exception expected" );
        }
        catch ( RuntimeException e )
        {
            assertEquals( failure, e );
        }

        assertNull( await( handler.peekAsync() ) );
    }

    @Test
    public void shouldReturnSingleAvailableRecordInNextAsync()
    {
        List<String> keys = asList( "key1", "key2" );
        PullAllResponseHandler handler = newHandler( keys );
        handler.onRecord( values( "1", "2" ) );

        Record record = await( handler.nextAsync() );

        assertNotNull( record );
        assertEquals( keys, record.keys() );
        assertEquals( "1", record.get( "key1" ).asString() );
        assertEquals( "2", record.get( "key2" ).asString() );
    }

    @Test
    public void shouldReturnNoRecordsWhenNoneAvailableInNextAsync()
    {
        PullAllResponseHandler handler = newHandler( asList( "key1", "key2" ) );
        handler.onSuccess( emptyMap() );

        assertNull( await( handler.nextAsync() ) );
    }

    @Test
    public void shouldReturnNoRecordsWhenSuccessComesAfterNextAsync()
    {
        PullAllResponseHandler handler = newHandler( asList( "key1", "key2" ) );

        CompletableFuture<Record> recordFuture = handler.nextAsync().toCompletableFuture();
        assertFalse( recordFuture.isDone() );

        handler.onSuccess( emptyMap() );
        assertTrue( recordFuture.isDone() );

        assertNull( await( recordFuture ) );
    }

    @Test
    public void shouldPullAllAvailableRecordsWithNextAsync()
    {
        List<String> keys = asList( "key1", "key2", "key3" );
        PullAllResponseHandler handler = newHandler( keys );

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
    public void shouldReturnRecordInNextAsyncWhenItBecomesAvailableLater()
    {
        List<String> keys = asList( "key1", "key2" );
        PullAllResponseHandler handler = newHandler( keys );

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
    public void shouldReturnSameRecordOnceWhenRequestedMultipleTimesInNextAsync()
    {
        List<String> keys = asList( "key1", "key2" );
        PullAllResponseHandler handler = newHandler( keys );

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
    public void shouldPropagateExistingFailureInNextAsync()
    {
        PullAllResponseHandler handler = newHandler();
        RuntimeException error = new RuntimeException( "Failed to read" );
        handler.onFailure( error );

        try
        {
            await( handler.nextAsync() );
            fail( "Exception expected" );
        }
        catch ( RuntimeException e )
        {
            assertEquals( error, e );
        }
    }

    @Test
    public void shouldPropagateFailureInNextAsyncWhenFailureMessagesArrivesLater()
    {
        PullAllResponseHandler handler = newHandler();

        CompletableFuture<Record> recordFuture = handler.nextAsync().toCompletableFuture();
        assertFalse( recordFuture.isDone() );

        RuntimeException error = new RuntimeException( "Network failed" );
        handler.onFailure( error );

        assertTrue( recordFuture.isDone() );
        try
        {
            await( recordFuture );
            fail( "Exception expected" );
        }
        catch ( RuntimeException e )
        {
            assertEquals( error, e );
        }
    }

    @Test
    public void shouldDisableAutoReadWhenTooManyRecordsArrive()
    {
        Connection connection = connectionMock();
        PullAllResponseHandler handler = newHandler( asList( "key1", "key2" ), connection );

        for ( int i = 0; i < PullAllResponseHandler.RECORD_BUFFER_HIGH_WATERMARK + 1; i++ )
        {
            handler.onRecord( values( 100, 200 ) );
        }

        verify( connection ).disableAutoRead();
    }

    @Test
    public void shouldEnableAutoReadWhenRecordsRetrievedFromBuffer()
    {
        Connection connection = connectionMock();
        List<String> keys = asList( "key1", "key2" );
        PullAllResponseHandler handler = newHandler( keys, connection );

        int i;
        for ( i = 0; i < PullAllResponseHandler.RECORD_BUFFER_HIGH_WATERMARK + 1; i++ )
        {
            handler.onRecord( values( 100, 200 ) );
        }

        verify( connection, never() ).enableAutoRead();
        verify( connection ).disableAutoRead();

        while ( i-- > PullAllResponseHandler.RECORD_BUFFER_LOW_WATERMARK - 1 )
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
    public void shouldNotDisableAutoReadWhenSummaryRequested()
    {
        Connection connection = connectionMock();
        List<String> keys = asList( "key1", "key2" );
        PullAllResponseHandler handler = newHandler( keys, connection );

        CompletableFuture<ResultSummary> summaryFuture = handler.summaryAsync().toCompletableFuture();
        assertFalse( summaryFuture.isDone() );

        int recordCount = PullAllResponseHandler.RECORD_BUFFER_HIGH_WATERMARK + 10;
        for ( int i = 0; i < recordCount; i++ )
        {
            handler.onRecord( values( "a", "b" ) );
        }

        verify( connection, never() ).enableAutoRead();
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
    public void shouldNotDisableAutoReadWhenFailureRequested()
    {
        Connection connection = connectionMock();
        List<String> keys = asList( "key1", "key2" );
        PullAllResponseHandler handler = newHandler( keys, connection );

        CompletableFuture<Throwable> failureFuture = handler.failureAsync().toCompletableFuture();
        assertFalse( failureFuture.isDone() );

        int recordCount = PullAllResponseHandler.RECORD_BUFFER_HIGH_WATERMARK + 5;
        for ( int i = 0; i < recordCount; i++ )
        {
            handler.onRecord( values( 123, 456 ) );
        }

        verify( connection, never() ).enableAutoRead();
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

    private static PullAllResponseHandler newHandler()
    {
        return newHandler( new Statement( "RETURN 1" ) );
    }

    private static PullAllResponseHandler newHandler( Statement statement )
    {
        return newHandler( statement, emptyList(), connectionMock() );
    }

    private static PullAllResponseHandler newHandler( List<String> statementKeys )
    {
        return newHandler( new Statement( "RETURN 1" ), statementKeys, connectionMock() );
    }

    private static PullAllResponseHandler newHandler( List<String> statementKeys, Connection connection )
    {
        return newHandler( new Statement( "RETURN 1" ), statementKeys, connection );
    }

    private static PullAllResponseHandler newHandler( Statement statement, List<String> statementKeys,
            Connection connection )
    {
        RunResponseHandler runResponseHandler = new RunResponseHandler( new CompletableFuture<>() );
        runResponseHandler.onSuccess( singletonMap( "fields", value( statementKeys ) ) );
        return new TestPullAllResponseHandler( statement, runResponseHandler, connection );
    }

    private static Connection connectionMock()
    {
        Connection connection = mock( Connection.class );
        when( connection.serverAddress() ).thenReturn( BoltServerAddress.LOCAL_DEFAULT );
        when( connection.serverVersion() ).thenReturn( ServerVersion.v3_2_0 );
        return connection;
    }

    private static class TestPullAllResponseHandler extends PullAllResponseHandler
    {
        TestPullAllResponseHandler( Statement statement, RunResponseHandler runResponseHandler, Connection connection )
        {
            super( statement, runResponseHandler, connection );
        }

        @Override
        protected void afterSuccess()
        {
        }

        @Override
        protected void afterFailure( Throwable error )
        {
        }
    }
}
