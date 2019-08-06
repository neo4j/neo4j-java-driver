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
package org.neo4j.driver.internal.async;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.neo4j.driver.Record;
import org.neo4j.driver.Statement;
import org.neo4j.driver.exceptions.NoSuchRecordException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.handlers.PullAllResponseHandler;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.messaging.v1.BoltProtocolV1;
import org.neo4j.driver.internal.summary.InternalResultSummary;
import org.neo4j.driver.internal.summary.InternalServerInfo;
import org.neo4j.driver.internal.summary.InternalSummaryCounters;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.summary.StatementType;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
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
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.Values.values;
import static org.neo4j.driver.internal.summary.InternalDatabaseInfo.DEFAULT_DATABASE_INFO;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.internal.util.Futures.failedFuture;
import static org.neo4j.driver.util.TestUtil.anyServerVersion;
import static org.neo4j.driver.util.TestUtil.await;

class AsyncStatementResultCursorTest
{
    @Test
    void shouldReturnStatementKeys()
    {
        RunResponseHandler runHandler = newRunResponseHandler();
        PullAllResponseHandler pullAllHandler = mock( PullAllResponseHandler.class );

        List<String> keys = asList( "key1", "key2", "key3" );
        runHandler.onSuccess( singletonMap( "fields", value( keys ) ) );

        AsyncStatementResultCursor cursor = newCursor( runHandler, pullAllHandler );

        assertEquals( keys, cursor.keys() );
    }

    @Test
    void shouldReturnSummary()
    {
        PullAllResponseHandler pullAllHandler = mock( PullAllResponseHandler.class );

        ResultSummary summary = new InternalResultSummary( new Statement( "RETURN 42" ),
                new InternalServerInfo( BoltServerAddress.LOCAL_DEFAULT, anyServerVersion() ), DEFAULT_DATABASE_INFO, StatementType.SCHEMA_WRITE,
                new InternalSummaryCounters( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 ), null, null, emptyList(), 42, 42 );
        when( pullAllHandler.summaryAsync() ).thenReturn( completedFuture( summary ) );

        AsyncStatementResultCursor cursor = newCursor( pullAllHandler );

        assertEquals( summary, await( cursor.summaryAsync() ) );
    }

    @Test
    void shouldReturnNextExistingRecord()
    {
        PullAllResponseHandler pullAllHandler = mock( PullAllResponseHandler.class );

        Record record = new InternalRecord( asList( "key1", "key2" ), values( 1, 2 ) );
        when( pullAllHandler.nextAsync() ).thenReturn( completedFuture( record ) );

        AsyncStatementResultCursor cursor = newCursor( pullAllHandler );

        assertEquals( record, await( cursor.nextAsync() ) );
    }

    @Test
    void shouldReturnNextNonExistingRecord()
    {
        PullAllResponseHandler pullAllHandler = mock( PullAllResponseHandler.class );
        when( pullAllHandler.nextAsync() ).thenReturn( completedWithNull() );

        AsyncStatementResultCursor cursor = newCursor( pullAllHandler );

        assertNull( await( cursor.nextAsync() ) );
    }

    @Test
    void shouldPeekExistingRecord()
    {
        PullAllResponseHandler pullAllHandler = mock( PullAllResponseHandler.class );

        Record record = new InternalRecord( asList( "key1", "key2", "key3" ), values( 3, 2, 1 ) );
        when( pullAllHandler.peekAsync() ).thenReturn( completedFuture( record ) );

        AsyncStatementResultCursor cursor = newCursor( pullAllHandler );

        assertEquals( record, await( cursor.peekAsync() ) );
    }

    @Test
    void shouldPeekNonExistingRecord()
    {
        PullAllResponseHandler pullAllHandler = mock( PullAllResponseHandler.class );
        when( pullAllHandler.peekAsync() ).thenReturn( completedWithNull() );

        AsyncStatementResultCursor cursor = newCursor( pullAllHandler );

        assertNull( await( cursor.peekAsync() ) );
    }

    @Test
    void shouldReturnSingleRecord()
    {
        PullAllResponseHandler pullAllHandler = mock( PullAllResponseHandler.class );

        Record record = new InternalRecord( asList( "key1", "key2" ), values( 42, 42 ) );
        when( pullAllHandler.nextAsync() ).thenReturn( completedFuture( record ) )
                .thenReturn( completedWithNull() );

        AsyncStatementResultCursor cursor = newCursor( pullAllHandler );

        assertEquals( record, await( cursor.singleAsync() ) );
    }

    @Test
    void shouldFailWhenAskedForSingleRecordButResultIsEmpty()
    {
        PullAllResponseHandler pullAllHandler = mock( PullAllResponseHandler.class );
        when( pullAllHandler.nextAsync() ).thenReturn( completedWithNull() );

        AsyncStatementResultCursor cursor = newCursor( pullAllHandler );

        NoSuchRecordException e = assertThrows( NoSuchRecordException.class, () -> await( cursor.singleAsync() ) );
        assertThat( e.getMessage(), containsString( "result is empty" ) );
    }

    @Test
    void shouldFailWhenAskedForSingleRecordButResultContainsMore()
    {
        PullAllResponseHandler pullAllHandler = mock( PullAllResponseHandler.class );

        Record record1 = new InternalRecord( asList( "key1", "key2" ), values( 1, 1 ) );
        Record record2 = new InternalRecord( asList( "key1", "key2" ), values( 2, 2 ) );
        when( pullAllHandler.nextAsync() ).thenReturn( completedFuture( record1 ) )
                .thenReturn( completedFuture( record2 ) );

        AsyncStatementResultCursor cursor = newCursor( pullAllHandler );

        NoSuchRecordException e = assertThrows( NoSuchRecordException.class, () -> await( cursor.singleAsync() ) );
        assertThat( e.getMessage(), containsString( "Ensure your query returns only one record" ) );
    }

    @Test
    void shouldForEachAsyncWhenResultContainsMultipleRecords()
    {
        PullAllResponseHandler pullAllHandler = mock( PullAllResponseHandler.class );

        Record record1 = new InternalRecord( asList( "key1", "key2", "key3" ), values( 1, 1, 1 ) );
        Record record2 = new InternalRecord( asList( "key1", "key2", "key3" ), values( 2, 2, 2 ) );
        Record record3 = new InternalRecord( asList( "key1", "key2", "key3" ), values( 3, 3, 3 ) );
        when( pullAllHandler.nextAsync() ).thenReturn( completedFuture( record1 ) )
                .thenReturn( completedFuture( record2 ) ).thenReturn( completedFuture( record3 ) )
                .thenReturn( completedWithNull() );

        ResultSummary summary = mock( ResultSummary.class );
        when( pullAllHandler.summaryAsync() ).thenReturn( completedFuture( summary ) );

        AsyncStatementResultCursor cursor = newCursor( pullAllHandler );

        List<Record> records = new CopyOnWriteArrayList<>();
        CompletionStage<ResultSummary> summaryStage = cursor.forEachAsync( records::add );

        assertEquals( summary, await( summaryStage ) );
        assertEquals( asList( record1, record2, record3 ), records );
    }

    @Test
    void shouldForEachAsyncWhenResultContainsOneRecords()
    {
        PullAllResponseHandler pullAllHandler = mock( PullAllResponseHandler.class );

        Record record = new InternalRecord( asList( "key1", "key2", "key3" ), values( 1, 1, 1 ) );
        when( pullAllHandler.nextAsync() ).thenReturn( completedFuture( record ) )
                .thenReturn( completedWithNull() );

        ResultSummary summary = mock( ResultSummary.class );
        when( pullAllHandler.summaryAsync() ).thenReturn( completedFuture( summary ) );

        AsyncStatementResultCursor cursor = newCursor( pullAllHandler );

        List<Record> records = new CopyOnWriteArrayList<>();
        CompletionStage<ResultSummary> summaryStage = cursor.forEachAsync( records::add );

        assertEquals( summary, await( summaryStage ) );
        assertEquals( singletonList( record ), records );
    }

    @Test
    void shouldForEachAsyncWhenResultContainsNoRecords()
    {
        PullAllResponseHandler pullAllHandler = mock( PullAllResponseHandler.class );
        when( pullAllHandler.nextAsync() ).thenReturn( completedWithNull() );

        ResultSummary summary = mock( ResultSummary.class );
        when( pullAllHandler.summaryAsync() ).thenReturn( completedFuture( summary ) );

        AsyncStatementResultCursor cursor = newCursor( pullAllHandler );

        List<Record> records = new CopyOnWriteArrayList<>();
        CompletionStage<ResultSummary> summaryStage = cursor.forEachAsync( records::add );

        assertEquals( summary, await( summaryStage ) );
        assertEquals( 0, records.size() );
    }

    @Test
    void shouldFailForEachWhenGivenActionThrows()
    {
        PullAllResponseHandler pullAllHandler = mock( PullAllResponseHandler.class );

        Record record1 = new InternalRecord( asList( "key1", "key2" ), values( 1, 1 ) );
        Record record2 = new InternalRecord( asList( "key1", "key2" ), values( 2, 2 ) );
        Record record3 = new InternalRecord( asList( "key1", "key2" ), values( 3, 3 ) );
        when( pullAllHandler.nextAsync() ).thenReturn( completedFuture( record1 ) )
                .thenReturn( completedFuture( record2 ) ).thenReturn( completedFuture( record3 ) )
                .thenReturn( completedWithNull() );

        AsyncStatementResultCursor cursor = newCursor( pullAllHandler );

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
        verify( pullAllHandler, times( 2 ) ).nextAsync();
    }

    @Test
    void shouldReturnFailureWhenExists()
    {
        PullAllResponseHandler pullAllHandler = mock( PullAllResponseHandler.class );

        ServiceUnavailableException error = new ServiceUnavailableException( "Hi" );
        when( pullAllHandler.failureAsync() ).thenReturn( completedFuture( error ) );

        AsyncStatementResultCursor cursor = newCursor( pullAllHandler );

        assertEquals( error, await( cursor.failureAsync() ) );
    }

    @Test
    void shouldReturnNullFailureWhenDoesNotExist()
    {
        PullAllResponseHandler pullAllHandler = mock( PullAllResponseHandler.class );
        when( pullAllHandler.failureAsync() ).thenReturn( completedWithNull() );

        AsyncStatementResultCursor cursor = newCursor( pullAllHandler );

        assertNull( await( cursor.failureAsync() ) );
    }

    @Test
    void shouldListAsyncWithoutMapFunction()
    {
        PullAllResponseHandler pullAllHandler = mock( PullAllResponseHandler.class );

        Record record1 = new InternalRecord( asList( "key1", "key2" ), values( 1, 1 ) );
        Record record2 = new InternalRecord( asList( "key1", "key2" ), values( 2, 2 ) );
        List<Record> records = asList( record1, record2 );

        when( pullAllHandler.listAsync( Function.identity() ) ).thenReturn( completedFuture( records ) );

        AsyncStatementResultCursor cursor = newCursor( pullAllHandler );

        assertEquals( records, await( cursor.listAsync() ) );
        verify( pullAllHandler ).listAsync( Function.identity() );
    }

    @Test
    void shouldListAsyncWithMapFunction()
    {
        Function<Record,String> mapFunction = record -> record.get( 0 ).asString();
        PullAllResponseHandler pullAllHandler = mock( PullAllResponseHandler.class );

        List<String> values = asList( "a", "b", "c", "d", "e" );
        when( pullAllHandler.listAsync( mapFunction ) ).thenReturn( completedFuture( values ) );

        AsyncStatementResultCursor cursor = newCursor( pullAllHandler );

        assertEquals( values, await( cursor.listAsync( mapFunction ) ) );
        verify( pullAllHandler ).listAsync( mapFunction );
    }

    @Test
    void shouldPropagateFailureFromListAsyncWithoutMapFunction()
    {
        PullAllResponseHandler pullAllHandler = mock( PullAllResponseHandler.class );
        RuntimeException error = new RuntimeException( "Hi" );
        when( pullAllHandler.listAsync( Function.identity() ) ).thenReturn( failedFuture( error ) );

        AsyncStatementResultCursor cursor = newCursor( pullAllHandler );

        RuntimeException e = assertThrows( RuntimeException.class, () -> await( cursor.listAsync() ) );
        assertEquals( error, e );
        verify( pullAllHandler ).listAsync( Function.identity() );
    }

    @Test
    void shouldPropagateFailureFromListAsyncWithMapFunction()
    {
        Function<Record,String> mapFunction = record -> record.get( 0 ).asString();
        PullAllResponseHandler pullAllHandler = mock( PullAllResponseHandler.class );
        RuntimeException error = new RuntimeException( "Hi" );
        when( pullAllHandler.listAsync( mapFunction ) ).thenReturn( failedFuture( error ) );

        AsyncStatementResultCursor cursor = newCursor( pullAllHandler );

        RuntimeException e = assertThrows( RuntimeException.class, () -> await( cursor.listAsync( mapFunction ) ) );
        assertEquals( error, e );

        verify( pullAllHandler ).listAsync( mapFunction );
    }

    @Test
    void shouldConsumeAsync()
    {
        PullAllResponseHandler pullAllHandler = mock( PullAllResponseHandler.class );
        ResultSummary summary = mock( ResultSummary.class );
        when( pullAllHandler.consumeAsync() ).thenReturn( completedFuture( summary ) );

        AsyncStatementResultCursor cursor = newCursor( pullAllHandler );

        assertEquals( summary, await( cursor.consumeAsync() ) );
    }

    @Test
    void shouldPropagateFailureInConsumeAsync()
    {
        PullAllResponseHandler pullAllHandler = mock( PullAllResponseHandler.class );
        RuntimeException error = new RuntimeException( "Hi" );
        when( pullAllHandler.consumeAsync() ).thenReturn( failedFuture( error ) );

        AsyncStatementResultCursor cursor = newCursor( pullAllHandler );

        RuntimeException e = assertThrows( RuntimeException.class, () -> await( cursor.consumeAsync() ) );
        assertEquals( error, e );
    }

    private static AsyncStatementResultCursor newCursor( PullAllResponseHandler pullAllHandler )
    {
        return new AsyncStatementResultCursor( newRunResponseHandler(), pullAllHandler );
    }

    private static AsyncStatementResultCursor newCursor( RunResponseHandler runHandler, PullAllResponseHandler pullAllHandler )
    {
        return new AsyncStatementResultCursor( runHandler, pullAllHandler );
    }

    private static RunResponseHandler newRunResponseHandler()
    {
        return new RunResponseHandler( new CompletableFuture<>(), BoltProtocolV1.METADATA_EXTRACTOR );
    }
}
