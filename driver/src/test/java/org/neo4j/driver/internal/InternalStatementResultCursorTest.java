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
package org.neo4j.driver.internal;

import org.junit.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.driver.internal.handlers.PullAllResponseHandler;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
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

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.v1.Values.value;
import static org.neo4j.driver.v1.Values.values;
import static org.neo4j.driver.v1.util.TestUtil.await;

public class InternalStatementResultCursorTest
{
    @Test
    public void shouldReturnStatementKeys()
    {
        RunResponseHandler runHandler = new RunResponseHandler( new CompletableFuture<>() );
        PullAllResponseHandler pullAllHandler = mock( PullAllResponseHandler.class );

        List<String> keys = asList( "key1", "key2", "key3" );
        runHandler.onSuccess( singletonMap( "fields", value( keys ) ) );

        InternalStatementResultCursor cursor = newCursor( runHandler, pullAllHandler );

        assertEquals( keys, cursor.keys() );
    }

    @Test
    public void shouldReturnSummary()
    {
        PullAllResponseHandler pullAllHandler = mock( PullAllResponseHandler.class );

        ResultSummary summary = new InternalResultSummary( new Statement( "RETURN 42" ),
                new InternalServerInfo( BoltServerAddress.LOCAL_DEFAULT, ServerVersion.v3_1_0 ),
                StatementType.SCHEMA_WRITE, new InternalSummaryCounters( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 ),
                null, null, emptyList(), 42, 42 );
        when( pullAllHandler.summaryAsync() ).thenReturn( completedFuture( summary ) );

        InternalStatementResultCursor cursor = newCursor( pullAllHandler );

        assertEquals( summary, await( cursor.summaryAsync() ) );
    }

    @Test
    public void shouldReturnNextExistingRecord()
    {
        PullAllResponseHandler pullAllHandler = mock( PullAllResponseHandler.class );

        Record record = new InternalRecord( asList( "key1", "key2" ), values( 1, 2 ) );
        when( pullAllHandler.nextAsync() ).thenReturn( completedFuture( record ) );

        InternalStatementResultCursor cursor = newCursor( pullAllHandler );

        assertEquals( record, await( cursor.nextAsync() ) );
    }

    @Test
    public void shouldReturnNextNonExistingRecord()
    {
        PullAllResponseHandler pullAllHandler = mock( PullAllResponseHandler.class );
        when( pullAllHandler.nextAsync() ).thenReturn( completedWithNull() );

        InternalStatementResultCursor cursor = newCursor( pullAllHandler );

        assertNull( await( cursor.nextAsync() ) );
    }

    @Test
    public void shouldPeekExistingRecord()
    {
        PullAllResponseHandler pullAllHandler = mock( PullAllResponseHandler.class );

        Record record = new InternalRecord( asList( "key1", "key2", "key3" ), values( 3, 2, 1 ) );
        when( pullAllHandler.peekAsync() ).thenReturn( completedFuture( record ) );

        InternalStatementResultCursor cursor = newCursor( pullAllHandler );

        assertEquals( record, await( cursor.peekAsync() ) );
    }

    @Test
    public void shouldPeekNonExistingRecord()
    {
        PullAllResponseHandler pullAllHandler = mock( PullAllResponseHandler.class );
        when( pullAllHandler.peekAsync() ).thenReturn( completedWithNull() );

        InternalStatementResultCursor cursor = newCursor( pullAllHandler );

        assertNull( await( cursor.peekAsync() ) );
    }

    @Test
    public void shouldReturnSingleRecord()
    {
        PullAllResponseHandler pullAllHandler = mock( PullAllResponseHandler.class );

        Record record = new InternalRecord( asList( "key1", "key2" ), values( 42, 42 ) );
        when( pullAllHandler.nextAsync() ).thenReturn( completedFuture( record ) )
                .thenReturn( completedWithNull() );

        InternalStatementResultCursor cursor = newCursor( pullAllHandler );

        assertEquals( record, await( cursor.singleAsync() ) );
    }

    @Test
    public void shouldFailWhenAskedForSingleRecordButResultIsEmpty()
    {
        PullAllResponseHandler pullAllHandler = mock( PullAllResponseHandler.class );
        when( pullAllHandler.nextAsync() ).thenReturn( completedWithNull() );

        InternalStatementResultCursor cursor = newCursor( pullAllHandler );

        try
        {
            await( cursor.singleAsync() );
            fail( "Exception expected" );
        }
        catch ( NoSuchRecordException e )
        {
            assertThat( e.getMessage(), containsString( "result is empty" ) );
        }
    }

    @Test
    public void shouldFailWhenAskedForSingleRecordButResultContainsMore()
    {
        PullAllResponseHandler pullAllHandler = mock( PullAllResponseHandler.class );

        Record record1 = new InternalRecord( asList( "key1", "key2" ), values( 1, 1 ) );
        Record record2 = new InternalRecord( asList( "key1", "key2" ), values( 2, 2 ) );
        when( pullAllHandler.nextAsync() ).thenReturn( completedFuture( record1 ) )
                .thenReturn( completedFuture( record2 ) );

        InternalStatementResultCursor cursor = newCursor( pullAllHandler );

        try
        {
            await( cursor.singleAsync() );
            fail( "Exception expected" );
        }
        catch ( NoSuchRecordException e )
        {
            assertThat( e.getMessage(), containsString( "Ensure your query returns only one record" ) );
        }
    }

    @Test
    public void shouldConsumeAsyncWhenResultContainsMultipleRecords()
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

        InternalStatementResultCursor cursor = newCursor( pullAllHandler );

        assertEquals( summary, await( cursor.consumeAsync() ) );
        verify( pullAllHandler, times( 4 ) ).nextAsync();
    }

    @Test
    public void shouldConsumeAsyncWhenResultContainsOneRecords()
    {
        PullAllResponseHandler pullAllHandler = mock( PullAllResponseHandler.class );

        Record record = new InternalRecord( asList( "key1", "key2" ), values( 1, 1 ) );
        when( pullAllHandler.nextAsync() ).thenReturn( completedFuture( record ) )
                .thenReturn( completedWithNull() );

        ResultSummary summary = mock( ResultSummary.class );
        when( pullAllHandler.summaryAsync() ).thenReturn( completedFuture( summary ) );

        InternalStatementResultCursor cursor = newCursor( pullAllHandler );

        assertEquals( summary, await( cursor.consumeAsync() ) );
        verify( pullAllHandler, times( 2 ) ).nextAsync();
    }

    @Test
    public void shouldConsumeAsyncWhenResultContainsNoRecords()
    {
        PullAllResponseHandler pullAllHandler = mock( PullAllResponseHandler.class );
        when( pullAllHandler.nextAsync() ).thenReturn( completedWithNull() );

        ResultSummary summary = mock( ResultSummary.class );
        when( pullAllHandler.summaryAsync() ).thenReturn( completedFuture( summary ) );

        InternalStatementResultCursor cursor = newCursor( pullAllHandler );

        assertEquals( summary, await( cursor.consumeAsync() ) );
        verify( pullAllHandler ).nextAsync();
    }

    @Test
    public void shouldForEachAsyncWhenResultContainsMultipleRecords()
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

        InternalStatementResultCursor cursor = newCursor( pullAllHandler );

        List<Record> records = new CopyOnWriteArrayList<>();
        CompletionStage<ResultSummary> summaryStage = cursor.forEachAsync( records::add );

        assertEquals( summary, await( summaryStage ) );
        assertEquals( asList( record1, record2, record3 ), records );
    }

    @Test
    public void shouldForEachAsyncWhenResultContainsOneRecords()
    {
        PullAllResponseHandler pullAllHandler = mock( PullAllResponseHandler.class );

        Record record = new InternalRecord( asList( "key1", "key2", "key3" ), values( 1, 1, 1 ) );
        when( pullAllHandler.nextAsync() ).thenReturn( completedFuture( record ) )
                .thenReturn( completedWithNull() );

        ResultSummary summary = mock( ResultSummary.class );
        when( pullAllHandler.summaryAsync() ).thenReturn( completedFuture( summary ) );

        InternalStatementResultCursor cursor = newCursor( pullAllHandler );

        List<Record> records = new CopyOnWriteArrayList<>();
        CompletionStage<ResultSummary> summaryStage = cursor.forEachAsync( records::add );

        assertEquals( summary, await( summaryStage ) );
        assertEquals( singletonList( record ), records );
    }

    @Test
    public void shouldForEachAsyncWhenResultContainsNoRecords()
    {
        PullAllResponseHandler pullAllHandler = mock( PullAllResponseHandler.class );
        when( pullAllHandler.nextAsync() ).thenReturn( completedWithNull() );

        ResultSummary summary = mock( ResultSummary.class );
        when( pullAllHandler.summaryAsync() ).thenReturn( completedFuture( summary ) );

        InternalStatementResultCursor cursor = newCursor( pullAllHandler );

        List<Record> records = new CopyOnWriteArrayList<>();
        CompletionStage<ResultSummary> summaryStage = cursor.forEachAsync( records::add );

        assertEquals( summary, await( summaryStage ) );
        assertEquals( 0, records.size() );
    }

    @Test
    public void shouldFailForEachWhenGivenActionThrows()
    {
        PullAllResponseHandler pullAllHandler = mock( PullAllResponseHandler.class );

        Record record1 = new InternalRecord( asList( "key1", "key2" ), values( 1, 1 ) );
        Record record2 = new InternalRecord( asList( "key1", "key2" ), values( 2, 2 ) );
        Record record3 = new InternalRecord( asList( "key1", "key2" ), values( 3, 3 ) );
        when( pullAllHandler.nextAsync() ).thenReturn( completedFuture( record1 ) )
                .thenReturn( completedFuture( record2 ) ).thenReturn( completedFuture( record3 ) )
                .thenReturn( completedWithNull() );

        InternalStatementResultCursor cursor = newCursor( pullAllHandler );

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

        try
        {
            await( stage );
            fail( "Exception expected" );
        }
        catch ( RuntimeException e )
        {
            assertEquals( error, e );
        }
        assertEquals( 1, recordsProcessed.get() );
        verify( pullAllHandler, times( 2 ) ).nextAsync();
    }

    @Test
    public void shouldListAsyncWhenResultContainsMultipleRecords()
    {
        PullAllResponseHandler pullAllHandler = mock( PullAllResponseHandler.class );

        Record record1 = new InternalRecord( asList( "key1", "key2", "key3" ), values( 1, 1, 1 ) );
        Record record2 = new InternalRecord( asList( "key1", "key2", "key3" ), values( 2, 2, 2 ) );
        Record record3 = new InternalRecord( asList( "key1", "key2", "key3" ), values( 3, 3, 3 ) );
        Record record4 = new InternalRecord( asList( "key1", "key2", "key3" ), values( 4, 4, 4 ) );
        when( pullAllHandler.nextAsync() ).thenReturn( completedFuture( record1 ) )
                .thenReturn( completedFuture( record2 ) ).thenReturn( completedFuture( record3 ) )
                .thenReturn( completedFuture( record4 ) ).thenReturn( completedWithNull() );

        InternalStatementResultCursor cursor = newCursor( pullAllHandler );

        assertEquals( asList( record1, record2, record3, record4 ), await( cursor.listAsync() ) );
    }

    @Test
    public void shouldListAsyncWhenResultContainsOneRecords()
    {
        PullAllResponseHandler pullAllHandler = mock( PullAllResponseHandler.class );

        Record record = new InternalRecord( asList( "key1", "key2", "key3" ), values( 1, 1, 1 ) );
        when( pullAllHandler.nextAsync() ).thenReturn( completedFuture( record ) )
                .thenReturn( completedWithNull() );

        InternalStatementResultCursor cursor = newCursor( pullAllHandler );

        assertEquals( singletonList( record ), await( cursor.listAsync() ) );
    }

    @Test
    public void shouldListAsyncWhenResultContainsNoRecords()
    {
        PullAllResponseHandler pullAllHandler = mock( PullAllResponseHandler.class );
        when( pullAllHandler.nextAsync() ).thenReturn( completedWithNull() );

        InternalStatementResultCursor cursor = newCursor( pullAllHandler );

        assertEquals( 0, await( cursor.listAsync() ).size() );
    }

    @Test
    public void shouldListAsyncWithFunctionWhenResultContainsMultipleRecords()
    {
        PullAllResponseHandler pullAllHandler = mock( PullAllResponseHandler.class );

        Record record1 = new InternalRecord( asList( "key1", "key2", "key3" ), values( 1, 11, 111 ) );
        Record record2 = new InternalRecord( asList( "key1", "key2", "key3" ), values( 2, 22, 222 ) );
        Record record3 = new InternalRecord( asList( "key1", "key2", "key3" ), values( 3, 33, 333 ) );
        Record record4 = new InternalRecord( asList( "key1", "key2", "key3" ), values( 4, 44, 444 ) );
        when( pullAllHandler.nextAsync() ).thenReturn( completedFuture( record1 ) )
                .thenReturn( completedFuture( record2 ) ).thenReturn( completedFuture( record3 ) )
                .thenReturn( completedFuture( record4 ) ).thenReturn( completedWithNull() );

        InternalStatementResultCursor cursor = newCursor( pullAllHandler );

        List<Integer> values = await( cursor.listAsync( record -> record.get( "key2" ).asInt() ) );
        assertEquals( asList( 11, 22, 33, 44 ), values );
    }

    @Test
    public void shouldListAsyncWithFunctionWhenResultContainsOneRecords()
    {
        PullAllResponseHandler pullAllHandler = mock( PullAllResponseHandler.class );

        Record singleRecord = new InternalRecord( asList( "key1", "key2", "key3" ), values( 1, 11, 111 ) );
        when( pullAllHandler.nextAsync() ).thenReturn( completedFuture( singleRecord ) )
                .thenReturn( completedWithNull() );

        InternalStatementResultCursor cursor = newCursor( pullAllHandler );

        List<Long> values = await( cursor.listAsync( record -> record.get( "key3" ).asLong() ) );
        assertEquals( singletonList( 111L ), values );
    }

    @Test
    public void shouldListAsyncWithFunctionWhenResultContainsNoRecords()
    {
        PullAllResponseHandler pullAllHandler = mock( PullAllResponseHandler.class );
        when( pullAllHandler.nextAsync() ).thenReturn( completedWithNull() );

        InternalStatementResultCursor cursor = newCursor( pullAllHandler );

        List<String> values = await( cursor.listAsync( record -> record.get( "key42" ).asString() ) );
        assertEquals( 0, values.size() );
    }

    @Test
    public void shouldFailListAsyncWhenGivenFunctionThrows()
    {
        PullAllResponseHandler pullAllHandler = mock( PullAllResponseHandler.class );

        Record record1 = new InternalRecord( asList( "key1", "key2" ), values( 1, 1 ) );
        Record record2 = new InternalRecord( asList( "key1", "key2" ), values( 2, 2 ) );
        Record record3 = new InternalRecord( asList( "key1", "key2" ), values( 3, 3 ) );
        when( pullAllHandler.nextAsync() ).thenReturn( completedFuture( record1 ) )
                .thenReturn( completedFuture( record2 ) ).thenReturn( completedFuture( record3 ) )
                .thenReturn( completedWithNull() );

        InternalStatementResultCursor cursor = newCursor( pullAllHandler );

        AtomicInteger recordsProcessed = new AtomicInteger();
        RuntimeException error = new RuntimeException( "Hello" );

        CompletionStage<List<Integer>> stage = cursor.listAsync( record ->
        {
            if ( record.get( "key1" ).asInt() == 2 )
            {
                throw error;
            }
            else
            {
                recordsProcessed.incrementAndGet();
                return record.get( "key1" ).asInt();
            }
        } );

        try
        {
            await( stage );
            fail( "Exception expected" );
        }
        catch ( RuntimeException e )
        {
            assertEquals( error, e );
        }
        assertEquals( 1, recordsProcessed.get() );
        verify( pullAllHandler, times( 2 ) ).nextAsync();
    }

    @Test
    public void shouldReturnFailureWhenExists()
    {
        PullAllResponseHandler pullAllHandler = mock( PullAllResponseHandler.class );

        ServiceUnavailableException error = new ServiceUnavailableException( "Hi" );
        when( pullAllHandler.failureAsync() ).thenReturn( completedFuture( error ) );

        InternalStatementResultCursor cursor = newCursor( pullAllHandler );

        assertEquals( error, await( cursor.failureAsync() ) );
    }

    @Test
    public void shouldReturnNullFailureWhenDoesNotExist()
    {
        PullAllResponseHandler pullAllHandler = mock( PullAllResponseHandler.class );
        when( pullAllHandler.failureAsync() ).thenReturn( completedWithNull() );

        InternalStatementResultCursor cursor = newCursor( pullAllHandler );

        assertNull( await( cursor.failureAsync() ) );
    }

    private static InternalStatementResultCursor newCursor( PullAllResponseHandler pullAllHandler )
    {
        return new InternalStatementResultCursor( new RunResponseHandler( new CompletableFuture<>() ), pullAllHandler );
    }

    private static InternalStatementResultCursor newCursor( RunResponseHandler runHandler,
            PullAllResponseHandler pullAllHandler )
    {
        return new InternalStatementResultCursor( runHandler, pullAllHandler );
    }
}
