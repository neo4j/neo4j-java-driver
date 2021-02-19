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
package org.neo4j.driver.internal;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.NoSuchRecordException;
import org.neo4j.driver.exceptions.ResultConsumedException;
import org.neo4j.driver.internal.cursor.AsyncResultCursor;
import org.neo4j.driver.internal.cursor.AsyncResultCursorImpl;
import org.neo4j.driver.internal.cursor.DisposableAsyncResultCursor;
import org.neo4j.driver.internal.handlers.LegacyPullAllResponseHandler;
import org.neo4j.driver.internal.handlers.PullAllResponseHandler;
import org.neo4j.driver.internal.handlers.PullResponseCompletionListener;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.messaging.v3.BoltProtocolV3;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.value.NullValue;
import org.neo4j.driver.util.Pair;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.Records.column;
import static org.neo4j.driver.Values.ofString;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.internal.BoltServerAddress.LOCAL_DEFAULT;
import static org.neo4j.driver.util.TestUtil.anyServerVersion;

class InternalResultTest
{
    @Test
    void iterationShouldWorksAsExpected()
    {
        // GIVEN
        Result result = createResult( 3 );

        // WHEN
        assertTrue( result.hasNext() );
        assertThat( values( result.next() ), equalTo( asList( value( "v1-1" ), value( "v2-1" ) ) ) );

        assertTrue( result.hasNext() );
        assertThat( values( result.next() ), equalTo( asList( value( "v1-2" ), value( "v2-2" ) ) ) );

        assertTrue( result.hasNext() ); //1 -> 2

        // THEN
        assertThat( values( result.next() ), equalTo( asList( value( "v1-3" ), value( "v2-3" ) ) ) );
        assertFalse( result.hasNext() );

        assertThrows( NoSuchRecordException.class, result::next );
    }

    @Test
    void firstOfFieldNameShouldWorkAsExpected()
    {
        // GIVEN
        Result result = createResult( 3 );

        // THEN
        assertThat( result.next().get( "k1" ), equalTo( value( "v1-1" ) ) );
        assertTrue( result.hasNext() );
    }

    @Test
    void firstOfFieldIndexShouldWorkAsExpected()
    {
        // GIVEN
        Result result = createResult( 3 );

        // THEN
        assertThat( result.next().get( 0 ), equalTo( value( "v1-1" ) ) );
        assertTrue( result.hasNext() );
    }

    @Test
    void singlePastFirstShouldFail()
    {
        // GIVEN
        Result result = createResult( 2 );
        result.next();
        result.next();

        // THEN
        assertThrows( NoSuchRecordException.class, result::single );
    }

    @Test
    void singleNoneShouldFail()
    {
        // GIVEN
        Result result = createResult( 0 );

        // THEN
        assertThrows( NoSuchRecordException.class, result::single );
    }

    @Test
    void singleWhenMoreThanOneShouldFail()
    {
        // GIVEN
        Result result = createResult( 2 );

        // THEN
        assertThrows( NoSuchRecordException.class, result::single );
    }

    @Test
    void singleOfFieldNameShouldWorkAsExpected()
    {
        // GIVEN
        Result result = createResult( 1 );

        // THEN
        assertThat( result.single().get( "k1" ), equalTo( value( "v1-1" ) ) );
        assertFalse( result.hasNext() );
    }

    @Test
    void singleOfFieldIndexShouldWorkAsExpected()
    {
        // GIVEN
        Result result = createResult( 1 );

        // THEN
        assertThat( result.single().get( 0 ), equalTo( value( "v1-1" ) ) );
        assertFalse( result.hasNext() );
    }

    @Test
    void singleShouldWorkAsExpected()
    {
        assertNotNull( createResult( 1 ).single() );
    }

    @Test
    void singleShouldThrowOnBigResult()
    {
        assertThrows( NoSuchRecordException.class, () -> createResult( 42 ).single() );
    }

    @Test
    void singleShouldThrowOnEmptyResult()
    {
        assertThrows( NoSuchRecordException.class, () -> createResult( 0 ).single() );
    }

    @Test
    void singleShouldThrowOnConsumedResult()
    {
        assertThrows( ResultConsumedException.class, () ->
        {
            Result result = createResult( 2 );
            result.consume();
            result.single();
        } );
    }

    @Test
    void shouldConsumeTwice()
    {
        // GIVEN
        Result result = createResult( 2 );
        result.consume();

        // WHEN
        result.consume();

        // THEN
        assertThrows( ResultConsumedException.class, result::hasNext );
    }

    @Test
    void shouldList()
    {
        // GIVEN
        Result result = createResult( 2 );
        List<String> records = result.list( column( "k1", ofString() ) );

        // THEN
        assertThat( records, equalTo( asList( "v1-1", "v1-2" ) ) );
    }

    @Test
    void shouldListTwice()
    {
        // GIVEN
        Result result = createResult( 2 );
        List<Record> firstList = result.list();
        assertThat( firstList.size(), equalTo( 2 ) );

        // THEN
        List<Record> secondList = result.list();
        assertThat( secondList.size(), equalTo( 0 ) );
    }

    @Test
    void singleShouldNotThrowOnPartiallyConsumedResult()
    {
        // Given
        Result result = createResult( 2 );
        result.next();

        // When + Then
        assertNotNull( result.single() );
    }

    @Test
    void singleShouldConsumeIfFailing()
    {
        Result result = createResult( 2 );

        assertThrows( NoSuchRecordException.class, result::single );
        assertFalse( result.hasNext() );
    }

    @Test
    void retainShouldWorkAsExpected()
    {
        // GIVEN
        Result result = createResult( 3 );

        // WHEN
        List<Record> records = result.list();

        // THEN
        assertFalse( result.hasNext() );
        assertThat( records, hasSize( 3 ) );
    }

    @Test
    void retainAndMapByKeyShouldWorkAsExpected()
    {
        // GIVEN
        Result result = createResult( 3 );

        // WHEN
        List<Value> records = result.list( column( "k1" ) );

        // THEN
        assertFalse( result.hasNext() );
        assertThat( records, hasSize( 3 ) );
    }

    @Test
    void retainAndMapByIndexShouldWorkAsExpected()
    {
        // GIVEN
        Result result = createResult( 3 );

        // WHEN
        List<Value> records = result.list( column( 0 ) );

        // THEN
        assertFalse( result.hasNext() );
        assertThat( records, hasSize( 3 ) );
    }

    @Test
    void accessingOutOfBoundsShouldBeNull()
    {
        // GIVEN
        Result result = createResult( 1 );

        // WHEN
        Record record = result.single();

        // THEN
        assertThat( record.get( 0 ), equalTo( value( "v1-1" ) ) );
        assertThat( record.get( 1 ), equalTo( value( "v2-1" ) ) );
        assertThat( record.get( 2 ), equalTo( NullValue.NULL ) );
        assertThat( record.get( -37 ), equalTo( NullValue.NULL ) );
    }

    @Test
    void accessingKeysWithoutCallingNextShouldNotFail()
    {
        // GIVEN
        Result result = createResult( 11 );

        // WHEN
        // not calling next or single

        // THEN
        assertThat( result.keys(), equalTo( asList( "k1", "k2" ) ) );
    }

    @Test
    void shouldPeekIntoTheFuture()
    {
        // WHEN
        Result result = createResult( 2 );

        // THEN
        assertThat( result.peek().get( "k1" ), equalTo( value( "v1-1" ) ) );

        // WHEN
        result.next();

        // THEN
        assertThat( result.peek().get( "k1" ), equalTo( value( "v1-2" ) ) );

        // WHEN
        result.next();

        // THEN
        assertThrows( NoSuchRecordException.class, result::peek );
    }

    @Test
    void shouldNotPeekIntoTheFutureWhenResultIsEmpty()
    {
        // GIVEN
        Result result = createResult( 0 );

        // THEN
        assertThrows( NoSuchRecordException.class, result::peek );
    }

    private Result createResult(int numberOfRecords )
    {
        RunResponseHandler runHandler = new RunResponseHandler( new CompletableFuture<>(), BoltProtocolV3.METADATA_EXTRACTOR );
        runHandler.onSuccess( singletonMap( "fields", value( Arrays.asList( "k1", "k2" ) ) ) );

        Query query = new Query( "<unknown>" );
        Connection connection = mock( Connection.class );
        when( connection.serverAddress() ).thenReturn( LOCAL_DEFAULT );
        when( connection.serverVersion() ).thenReturn( anyServerVersion() );
        PullAllResponseHandler pullAllHandler =
                new LegacyPullAllResponseHandler( query, runHandler, connection, BoltProtocolV3.METADATA_EXTRACTOR,
                                                  mock( PullResponseCompletionListener.class ) );

        for ( int i = 1; i <= numberOfRecords; i++ )
        {
            pullAllHandler.onRecord( new Value[]{value( "v1-" + i ), value( "v2-" + i )} );
        }
        pullAllHandler.onSuccess( emptyMap() );

        AsyncResultCursor cursor = new AsyncResultCursorImpl( runHandler, pullAllHandler );
        return new InternalResult( connection, new DisposableAsyncResultCursor( cursor ) );
    }

    private List<Value> values( Record record )
    {
        List<Value> result = new ArrayList<>( record.keys().size() );
        for ( Pair<String,Value> property : record.fields() )
        {
            result.add( property.value() );
        }
        return result;
    }
}
