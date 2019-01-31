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
package org.neo4j.driver.react.result;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.LegacyInternalStatementResultCursorTest;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.handlers.pulln.PullResponseHandler;
import org.neo4j.driver.internal.messaging.v4.BoltProtocolV4;
import org.neo4j.driver.internal.summary.InternalResultSummary;
import org.neo4j.driver.internal.summary.InternalServerInfo;
import org.neo4j.driver.internal.summary.InternalSummaryCounters;
import org.neo4j.driver.internal.util.ServerVersion;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.exceptions.NoSuchRecordException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.summary.StatementType;
import org.neo4j.driver.v1.util.Function;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.internal.util.Futures.failedFuture;
import static org.neo4j.driver.v1.Values.value;
import static org.neo4j.driver.v1.Values.values;
import static org.neo4j.driver.v1.util.TestUtil.await;

/**
 * The tests here are very similar to the tests in {@link LegacyInternalStatementResultCursorTest},
 * as essentially they shall behaves in the same way.
 */
class AsyncStatementResultCursorTest
{
    @Test
    void shouldReturnStatementKeys() throws Throwable
    {
        // Given
        PullResponseHandler pullHandler = mock( PullResponseHandler.class );
        RunResponseHandler runHandler = newRunResponseHandler();

        // When
        List<String> keys = asList( "key1", "key2", "key3" );
        runHandler.onSuccess( Collections.singletonMap( "fields", value( keys ) ) );
        AsyncStatementResultCursor cursor = new AsyncStatementResultCursor( runHandler, pullHandler );

        // Then
        assertEquals( keys, cursor.keys() );
    }

    @Test
    void shouldReturnSummary() throws Throwable
    {
        // Given
        PullResponseHandler pullHandler = mock( PullResponseHandler.class );

        // When
        ResultSummary summary = new InternalResultSummary( new Statement( "RETURN 42" ),
                new InternalServerInfo( BoltServerAddress.LOCAL_DEFAULT, ServerVersion.v3_1_0 ),
                StatementType.SCHEMA_WRITE, new InternalSummaryCounters( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 ),
                null, null, emptyList(), 42, 42 );
        when( pullHandler.summary() ).thenReturn( completedFuture( summary ) );

        AsyncStatementResultCursor cursor = newCursor( pullHandler );

        // Then
        assertEquals( summary, await( cursor.summaryAsync() ) );
    }

    @Test
    void shouldReturnNextExistingRecord()
    {
        PullResponseHandler pullHandler = mock( PullResponseHandler.class );

        Record record = new InternalRecord( asList( "key1", "key2" ), values( 1, 2 ) );
        when( pullHandler.nextRecord() ).thenReturn( completedFuture( record ) );

        AsyncStatementResultCursor cursor = newCursor( pullHandler );

        assertEquals( record, await( cursor.nextAsync() ) );
    }

    @Test
    void shouldReturnNextNonExistingRecord()
    {
        PullResponseHandler pullHandler = mock( PullResponseHandler.class );
        when( pullHandler.nextRecord() ).thenReturn( completedWithNull() );

        AsyncStatementResultCursor cursor = newCursor( pullHandler );

        assertNull( await( cursor.nextAsync() ) );
    }

    @Test
    void shouldPeekExistingRecord()
    {
        PullResponseHandler pullHandler = mock( PullResponseHandler.class );

        Record record = new InternalRecord( asList( "key1", "key2", "key3" ), values( 3, 2, 1 ) );
        when( pullHandler.peekRecord() ).thenReturn( completedFuture( record ) );

        AsyncStatementResultCursor cursor = newCursor( pullHandler );

        assertEquals( record, await( cursor.peekAsync() ) );
    }

    @Test
    void shouldPeekNonExistingRecord()
    {
        PullResponseHandler pullHandler = mock( PullResponseHandler.class );
        when( pullHandler.peekRecord() ).thenReturn( completedWithNull() );

        AsyncStatementResultCursor cursor = newCursor( pullHandler );

        assertNull( await( cursor.peekAsync() ) );
    }

    @Test
    void shouldReturnSingleRecord()
    {
        PullResponseHandler pullHandler = mock( PullResponseHandler.class );

        Record record = new InternalRecord( asList( "key1", "key2" ), values( 42, 42 ) );
        when( pullHandler.nextRecord() ).thenReturn( completedFuture( record ) )
                .thenReturn( completedWithNull() );

        AsyncStatementResultCursor cursor = newCursor( pullHandler );

        assertEquals( record, await( cursor.singleAsync() ) );
    }

    @Test
    void shouldFailWhenAskedForSingleRecordButResultIsEmpty()
    {
        PullResponseHandler pullHandler = mock( PullResponseHandler.class );
        when( pullHandler.nextRecord() ).thenReturn( completedWithNull() );

        AsyncStatementResultCursor cursor = newCursor( pullHandler );

        NoSuchRecordException e = assertThrows( NoSuchRecordException.class, () -> await( cursor.singleAsync() ) );
        assertThat( e.getMessage(), containsString( "result is empty" ) );
    }

    @Test
    void shouldFailWhenAskedForSingleRecordButResultContainsMore()
    {
        PullResponseHandler pullHandler = mock( PullResponseHandler.class );

        Record record1 = new InternalRecord( asList( "key1", "key2" ), values( 1, 1 ) );
        Record record2 = new InternalRecord( asList( "key1", "key2" ), values( 2, 2 ) );
        when( pullHandler.nextRecord() ).thenReturn( completedFuture( record1 ) )
                .thenReturn( completedFuture( record2 ) );

        AsyncStatementResultCursor cursor = newCursor( pullHandler );

        NoSuchRecordException e = assertThrows( NoSuchRecordException.class, () -> await( cursor.singleAsync() ) );
        assertThat( e.getMessage(), containsString( "Ensure your query returns only one record" ) );
    }

    @Test
    void shouldForEachAsyncWhenResultContainsMultipleRecords()
    {
        PullResponseHandler pullHandler = mock( PullResponseHandler.class );

        Record record1 = new InternalRecord( asList( "key1", "key2", "key3" ), values( 1, 1, 1 ) );
        Record record2 = new InternalRecord( asList( "key1", "key2", "key3" ), values( 2, 2, 2 ) );
        Record record3 = new InternalRecord( asList( "key1", "key2", "key3" ), values( 3, 3, 3 ) );
        when( pullHandler.nextRecord() ).thenReturn( completedFuture( record1 ) )
                .thenReturn( completedFuture( record2 ) ).thenReturn( completedFuture( record3 ) )
                .thenReturn( completedWithNull() );

        ResultSummary summary = mock( ResultSummary.class );
        when( pullHandler.summary() ).thenReturn( completedFuture( summary ) );

        AsyncStatementResultCursor cursor = newCursor( pullHandler );

        List<Record> records = new CopyOnWriteArrayList<>();
        CompletionStage<ResultSummary> summaryStage = cursor.forEachAsync( records::add );

        assertEquals( summary, await( summaryStage ) );
        assertEquals( asList( record1, record2, record3 ), records );
    }

    @Test
    void shouldForEachAsyncWhenResultContainsOneRecords()
    {
        PullResponseHandler pullHandler = mock( PullResponseHandler.class );

        Record record = new InternalRecord( asList( "key1", "key2", "key3" ), values( 1, 1, 1 ) );
        when( pullHandler.nextRecord() ).thenReturn( completedFuture( record ) )
                .thenReturn( completedWithNull() );

        ResultSummary summary = mock( ResultSummary.class );
        when( pullHandler.summary() ).thenReturn( completedFuture( summary ) );

        AsyncStatementResultCursor cursor = newCursor( pullHandler );

        List<Record> records = new CopyOnWriteArrayList<>();
        CompletionStage<ResultSummary> summaryStage = cursor.forEachAsync( records::add );

        assertEquals( summary, await( summaryStage ) );
        assertEquals( singletonList( record ), records );
    }

    @Test
    void shouldForEachAsyncWhenResultContainsNoRecords()
    {
        PullResponseHandler pullHandler = mock( PullResponseHandler.class );
        when( pullHandler.nextRecord() ).thenReturn( completedWithNull() );

        ResultSummary summary = mock( ResultSummary.class );
        when( pullHandler.summary() ).thenReturn( completedFuture( summary ) );

        AsyncStatementResultCursor cursor = newCursor( pullHandler );

        List<Record> records = new CopyOnWriteArrayList<>();
        CompletionStage<ResultSummary> summaryStage = cursor.forEachAsync( records::add );

        assertEquals( summary, await( summaryStage ) );
        assertEquals( 0, records.size() );
    }

    @Test
    void shouldFailForEachWhenGivenActionThrows()
    {
        PullResponseHandler pullHandler = mock( PullResponseHandler.class );

        Record record1 = new InternalRecord( asList( "key1", "key2" ), values( 1, 1 ) );
        Record record2 = new InternalRecord( asList( "key1", "key2" ), values( 2, 2 ) );
        Record record3 = new InternalRecord( asList( "key1", "key2" ), values( 3, 3 ) );
        when( pullHandler.nextRecord() ).thenReturn( completedFuture( record1 ) )
                .thenReturn( completedFuture( record2 ) ).thenReturn( completedFuture( record3 ) )
                .thenReturn( completedWithNull() );

        AsyncStatementResultCursor cursor = newCursor( pullHandler );

        AtomicInteger recordsProcessed = new AtomicInteger();
        RuntimeException error = new RuntimeException( "Hello" );

        CompletionStage<ResultSummary> stage = cursor.forEachAsync( record ->
        {
            if ( record.get( "key2" ).asInt() == 2 )
            {
                throw error;
            }
            else
            {
                recordsProcessed.incrementAndGet();
            }
        } );

        RuntimeException e = assertThrows( RuntimeException.class, () -> await( stage ) );
        assertEquals( error, e );

        assertEquals( 1, recordsProcessed.get() );
        verify( pullHandler, times( 2 ) ).nextRecord();
    }

    @Test
    void shouldReturnFailureWhenExists()
    {
        PullResponseHandler pullHandler = mock( PullResponseHandler.class );

        ServiceUnavailableException error = new ServiceUnavailableException( "Hi" );
        when( pullHandler.summary() ).thenReturn( failedFuture( error ) );

        AsyncStatementResultCursor cursor = newCursor( pullHandler );

        assertEquals( error, await( cursor.failureAsync() ).getCause() );
    }

    @Test
    void shouldReturnNullFailureWhenDoesNotExist()
    {
        PullResponseHandler pullHandler = mock( PullResponseHandler.class );
        when( pullHandler.summary() ).thenReturn( completedWithNull() );

        AsyncStatementResultCursor cursor = newCursor( pullHandler );

        assertNull( await( cursor.failureAsync() ) );
    }

    @Test
    void shouldListAsyncWithoutMapFunction()
    {
        PullResponseHandler pullHandler = mock( PullResponseHandler.class );

        Record record1 = new InternalRecord( asList( "key1", "key2" ), values( 1, 1 ) );
        Record record2 = new InternalRecord( asList( "key1", "key2" ), values( 2, 2 ) );
        Queue<Record> records = new LinkedList<>();
        records.add( record1 );
        records.add( record2 );

        when( pullHandler.queue() ).thenReturn( records );
        when( pullHandler.summary() ).thenReturn( completedWithNull() );

        AsyncStatementResultCursor cursor = newCursor( pullHandler );

        List<Record> expected = new LinkedList<>();
        expected.add( record1 );
        expected.add( record2 );
        assertEquals( expected, await( cursor.listAsync() ) );
        verify( pullHandler ).request( Long.MAX_VALUE );
    }

    @Test
    void shouldListAsyncWithMapFunction()
    {
        PullResponseHandler pullHandler = mock( PullResponseHandler.class );

        Record record1 = new InternalRecord( asList( "key1", "key2" ), values( "1", 1 ) );
        Record record2 = new InternalRecord( asList( "key1", "key2" ), values( "2", 2 ) );
        Queue<Record> records = new LinkedList<>();
        records.add( record1 );
        records.add( record2 );

        Function<Record,String> mapFunction = record -> record.get( "key1" ).asString();
        List<String> values = asList( "1", "2" );

        when( pullHandler.queue() ).thenReturn( records );
        when( pullHandler.summary() ).thenReturn( completedWithNull() );

        AsyncStatementResultCursor cursor = newCursor( pullHandler );

        assertEquals( values, await( cursor.listAsync( mapFunction ) ) );
        verify( pullHandler ).summary();
        verify( pullHandler ).request( Long.MAX_VALUE );
    }

    @Test
    void shouldPropagateFailureFromListAsyncWithoutMapFunction()
    {
        PullResponseHandler pullHandler = mock( PullResponseHandler.class );
        RuntimeException error = new RuntimeException( "Hi" );
        when( pullHandler.summary() ).thenReturn( failedFuture( error ) );

        AsyncStatementResultCursor cursor = newCursor( pullHandler );

        RuntimeException e = assertThrows( RuntimeException.class, () -> await( cursor.listAsync() ) );
        assertEquals( error, e );

        verify( pullHandler ).summary();
        verify( pullHandler ).request( Long.MAX_VALUE );
    }

    @Test
    void shouldPropagateFailureFromListAsyncWithMapFunction()
    {
        Function<Record,String> mapFunction = record -> record.get( 0 ).asString();
        PullResponseHandler pullHandler = mock( PullResponseHandler.class );
        RuntimeException error = new RuntimeException( "Hi" );
        when( pullHandler.summary() ).thenReturn( failedFuture( error ) );

        AsyncStatementResultCursor cursor = newCursor( pullHandler );

        RuntimeException e = assertThrows( RuntimeException.class, () -> await( cursor.listAsync( mapFunction ) ) );
        assertEquals( error, e );

        verify( pullHandler ).summary();
        verify( pullHandler ).request( Long.MAX_VALUE );
    }

    @Test
    void shouldConsumeAsync()
    {
        PullResponseHandler pullHandler = mock( PullResponseHandler.class );
        ResultSummary summary = mock( ResultSummary.class );
        when( pullHandler.summary() ).thenReturn( completedFuture( summary ) );

        AsyncStatementResultCursor cursor = newCursor( pullHandler );

        assertEquals( summary, await( cursor.consumeAsync() ) );
    }

    @Test
    void shouldPropagateFailureInConsumeAsync()
    {
        PullResponseHandler pullHandler = mock( PullResponseHandler.class );
        RuntimeException error = new RuntimeException( "Hi" );
        when( pullHandler.summary() ).thenReturn( failedFuture( error ) );

        AsyncStatementResultCursor cursor = newCursor( pullHandler );

        RuntimeException e = assertThrows( RuntimeException.class, () -> await( cursor.consumeAsync() ) );
        assertEquals( error, e );
    }


    private static AsyncStatementResultCursor newCursor( PullResponseHandler pullHandler )
    {
        return new AsyncStatementResultCursor( newRunResponseHandler(), pullHandler );
    }

    private static AsyncStatementResultCursor newCursor( RunResponseHandler runHandler, PullResponseHandler pullHandler )
    {
        return new AsyncStatementResultCursor( runHandler, pullHandler );
    }

    private static RunResponseHandler newRunResponseHandler()
    {
        return new RunResponseHandler( new CompletableFuture<>(), BoltProtocolV4.METADATA_EXTRACTOR );
    }
}
