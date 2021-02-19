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
package org.neo4j.driver.internal.handlers;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.value.BooleanValue;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.summary.QueryType;

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
import static org.mockito.Mockito.when;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.Values.values;
import static org.neo4j.driver.util.TestUtil.anyServerVersion;
import static org.neo4j.driver.util.TestUtil.await;

public abstract class PullAllResponseHandlerTestBase<T extends PullAllResponseHandler>
{
    @Test
    void shouldReturnNoFailureWhenAlreadySucceeded()
    {
        PullAllResponseHandler handler = newHandler();
        handler.onSuccess( emptyMap() );

        Throwable failure = await( handler.pullAllFailureAsync() );

        assertNull( failure );
    }

    @Test
    void shouldReturnNoFailureWhenSucceededAfterFailureRequested()
    {
        PullAllResponseHandler handler = newHandler();

        CompletableFuture<Throwable> failureFuture = handler.pullAllFailureAsync().toCompletableFuture();
        assertFalse( failureFuture.isDone() );

        handler.onSuccess( emptyMap() );

        assertTrue( failureFuture.isDone() );
        assertNull( await( failureFuture ) );
    }

    @Test
    void shouldReturnFailureWhenAlreadyFailed()
    {
        PullAllResponseHandler handler = newHandler();

        RuntimeException failure = new RuntimeException( "Ops" );
        handler.onFailure( failure );

        Throwable receivedFailure = await( handler.pullAllFailureAsync() );
        assertEquals( failure, receivedFailure );
    }

    @Test
    void shouldReturnFailureWhenFailedAfterFailureRequested()
    {
        PullAllResponseHandler handler = newHandler();

        CompletableFuture<Throwable> failureFuture = handler.pullAllFailureAsync().toCompletableFuture();
        assertFalse( failureFuture.isDone() );

        IOException failure = new IOException( "Broken pipe" );
        handler.onFailure( failure );

        assertTrue( failureFuture.isDone() );
        assertEquals( failure, await( failureFuture ) );
    }

    @Test
    void shouldReturnFailureWhenRequestedMultipleTimes()
    {
        PullAllResponseHandler handler = newHandler();

        CompletableFuture<Throwable> failureFuture1 = handler.pullAllFailureAsync().toCompletableFuture();
        CompletableFuture<Throwable> failureFuture2 = handler.pullAllFailureAsync().toCompletableFuture();

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
        PullAllResponseHandler handler = newHandler();

        ServiceUnavailableException failure = new ServiceUnavailableException( "Connection terminated" );
        handler.onFailure( failure );

        assertEquals( failure, await( handler.pullAllFailureAsync() ) );
        assertNull( await( handler.pullAllFailureAsync() ) );
    }

    @Test
    void shouldReturnFailureOnlyOnceWhenFailedAfterFailureRequested()
    {
        PullAllResponseHandler handler = newHandler();

        CompletionStage<Throwable> failureFuture = handler.pullAllFailureAsync();

        SessionExpiredException failure = new SessionExpiredException( "Network unreachable" );
        handler.onFailure( failure );
        assertEquals( failure, await( failureFuture ) );

        assertNull( await( handler.pullAllFailureAsync() ) );
    }

    @Test
    void shouldReturnSummaryWhenAlreadyFailedAndFailureConsumed()
    {
        Query query = new Query( "CREATE ()" );
        PullAllResponseHandler handler = newHandler(query);

        ServiceUnavailableException failure = new ServiceUnavailableException( "Neo4j unreachable" );
        handler.onFailure( failure );

        assertEquals( failure, await( handler.pullAllFailureAsync() ) );

        ResultSummary summary = await( handler.consumeAsync() );
        assertNotNull( summary );
        assertEquals(query, summary.query() );
    }

    @Test
    void shouldReturnSummaryWhenAlreadySucceeded()
    {
        Query query = new Query( "CREATE () RETURN 42" );
        PullAllResponseHandler handler = newHandler(query);
        handler.onSuccess( singletonMap( "type", value( "rw" ) ) );

        ResultSummary summary = await( handler.consumeAsync() );

        assertEquals(query, summary.query() );
        assertEquals( QueryType.READ_WRITE, summary.queryType() );
    }

    @Test
    void shouldReturnSummaryWhenSucceededAfterSummaryRequested()
    {
        Query query = new Query( "RETURN 'Hi!" );
        PullAllResponseHandler handler = newHandler(query);

        CompletableFuture<ResultSummary> summaryFuture = handler.consumeAsync().toCompletableFuture();
        assertFalse( summaryFuture.isDone() );

        handler.onSuccess( singletonMap( "type", value( "r" ) ) );

        assertTrue( summaryFuture.isDone() );
        ResultSummary summary = await( summaryFuture );

        assertEquals(query, summary.query() );
        assertEquals( QueryType.READ_ONLY, summary.queryType() );
    }

    @Test
    void shouldReturnFailureWhenSummaryRequestedWhenAlreadyFailed()
    {
        PullAllResponseHandler handler = newHandler();

        RuntimeException failure = new RuntimeException( "Computer is burning" );
        handler.onFailure( failure );

        RuntimeException e = assertThrows( RuntimeException.class, () -> await( handler.consumeAsync() ) );
        assertEquals( failure, e );
    }

    @Test
    void shouldReturnFailureWhenFailedAfterSummaryRequested()
    {
        PullAllResponseHandler handler = newHandler();

        CompletableFuture<ResultSummary> summaryFuture = handler.consumeAsync().toCompletableFuture();
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
        PullAllResponseHandler handler = newHandler();

        CompletableFuture<ResultSummary> summaryFuture1 = handler.consumeAsync().toCompletableFuture();
        CompletableFuture<ResultSummary> summaryFuture2 = handler.consumeAsync().toCompletableFuture();
        assertFalse( summaryFuture1.isDone() );
        assertFalse( summaryFuture2.isDone() );

        ClosedChannelException failure = new ClosedChannelException();
        handler.onFailure( failure );

        assertTrue( summaryFuture1.isDone() );
        assertTrue( summaryFuture2.isDone() );

        Exception e1 = assertThrows( Exception.class, () -> await( summaryFuture2 ) );
        assertEquals( failure, e1 );

        Exception e2 = assertThrows( Exception.class, () -> await( summaryFuture1 ) );
        assertEquals( failure, e2 );
    }

    @Test
    void shouldPropagateFailureOnlyOnceFromSummary()
    {
        Query query = new Query( "CREATE INDEX ON :Person(name)" );
        PullAllResponseHandler handler = newHandler(query);

        IllegalStateException failure = new IllegalStateException( "Some state is illegal :(" );
        handler.onFailure( failure );

        RuntimeException e = assertThrows( RuntimeException.class, () -> await( handler.consumeAsync() ) );
        assertEquals( failure, e );

        ResultSummary summary = await( handler.consumeAsync() );
        assertNotNull( summary );
        assertEquals(query, summary.query() );
    }

    @Test
    void shouldPeekSingleAvailableRecord()
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
    void shouldPeekFirstRecordWhenMultipleAvailable()
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
    void shouldPeekRecordThatBecomesAvailableLater()
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
    void shouldPeekAvailableNothingAfterSuccess()
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
    void shouldPeekNothingAfterSuccess()
    {
        PullAllResponseHandler handler = newHandler();
        handler.onSuccess( emptyMap() );

        assertNull( await( handler.peekAsync() ) );
    }

    @Test
    void shouldPeekWhenRequestedMultipleTimes()
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
    void shouldPropagateNotConsumedFailureInPeek()
    {
        PullAllResponseHandler handler = newHandler();

        RuntimeException failure = new RuntimeException( "Something is wrong" );
        handler.onFailure( failure );

        RuntimeException e = assertThrows( RuntimeException.class, () -> await( handler.peekAsync() ) );
        assertEquals( failure, e );
    }

    @Test
    void shouldPropagateFailureInPeekWhenItBecomesAvailable()
    {
        PullAllResponseHandler handler = newHandler();

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
        PullAllResponseHandler handler = newHandler();

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
        PullAllResponseHandler handler = newHandler( keys );
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
        PullAllResponseHandler handler = newHandler( asList( "key1", "key2" ) );
        handler.onSuccess( emptyMap() );

        assertNull( await( handler.nextAsync() ) );
    }

    @Test
    void shouldReturnNoRecordsWhenSuccessComesAfterNextAsync()
    {
        PullAllResponseHandler handler = newHandler( asList( "key1", "key2" ) );

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
    void shouldReturnRecordInNextAsyncWhenItBecomesAvailableLater()
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
    void shouldReturnSameRecordOnceWhenRequestedMultipleTimesInNextAsync()
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
    void shouldPropagateExistingFailureInNextAsync()
    {
        PullAllResponseHandler handler = newHandler();
        RuntimeException error = new RuntimeException( "FAILED to read" );
        handler.onFailure( error );

        RuntimeException e = assertThrows( RuntimeException.class, () -> await( handler.nextAsync() ) );
        assertEquals( error, e );
    }

    @Test
    void shouldPropagateFailureInNextAsyncWhenFailureMessagesArrivesLater()
    {
        PullAllResponseHandler handler = newHandler();

        CompletableFuture<Record> recordFuture = handler.nextAsync().toCompletableFuture();
        assertFalse( recordFuture.isDone() );

        RuntimeException error = new RuntimeException( "Network failed" );
        handler.onFailure( error );

        assertTrue( recordFuture.isDone() );
        RuntimeException e = assertThrows( RuntimeException.class, () -> await( recordFuture ) );
        assertEquals( error, e );
    }

    @Test
    void shouldPropagateFailureFromListAsync()
    {
        PullAllResponseHandler handler = newHandler();
        RuntimeException error = new RuntimeException( "Hi!" );
        handler.onFailure( error );

        RuntimeException e = assertThrows( RuntimeException.class, () -> await( handler.listAsync( Function.identity() ) ) );
        assertEquals( error, e );
    }

    @Test
    void shouldPropagateFailureAfterRecordFromListAsync()
    {
        PullAllResponseHandler handler = newHandler( asList( "key1", "key2" ) );

        handler.onRecord( values( "a", "b" ) );

        RuntimeException error = new RuntimeException( "Hi!" );
        handler.onFailure( error );

        RuntimeException e = assertThrows( RuntimeException.class, () -> await( handler.listAsync( Function.identity() ) ) );
        assertEquals( error, e );
    }

    @Test
    void shouldFailListAsyncWhenTransformationFunctionThrows()
    {
        PullAllResponseHandler handler = newHandler( asList( "key1", "key2" ) );
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
        PullAllResponseHandler handler = newHandler();

        handler.onSuccess( emptyMap() );

        assertEquals( emptyList(), await( handler.listAsync( Function.identity() ) ) );
    }

    @Test
    void shouldReturnTransformedListInListAsync()
    {
        PullAllResponseHandler handler = newHandler( singletonList( "key1" ) );

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
        PullAllResponseHandler handler = newHandler( keys );

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
    protected T newHandler()
    {
        return newHandler( new Query( "RETURN 1" ) );
    }

    protected T newHandler( Query query)
    {
        return newHandler(query, emptyList() );
    }

    protected T newHandler( List<String> queryKeys )
    {
        return newHandler( new Query( "RETURN 1" ), queryKeys, connectionMock() );
    }

    protected T newHandler(Query query, List<String> queryKeys )
    {
        return newHandler(query, queryKeys, connectionMock() );
    }

    protected T newHandler( List<String> queryKeys, Connection connection )
    {
        return newHandler( new Query( "RETURN 1" ), queryKeys, connection );
    }

    protected abstract T newHandler(Query query, List<String> queryKeys, Connection connection );

    protected Connection connectionMock()
    {
        Connection connection = mock( Connection.class );
        when( connection.serverAddress() ).thenReturn( BoltServerAddress.LOCAL_DEFAULT );
        when( connection.serverVersion() ).thenReturn( anyServerVersion() );
        return connection;
    }
}
