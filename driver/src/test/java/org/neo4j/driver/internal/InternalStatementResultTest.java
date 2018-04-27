/*
 * Copyright (c) 2002-2018 Neo4j Sweden AB [http://neo4j.com]
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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.neo4j.driver.internal.handlers.PullAllResponseHandler;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.handlers.SessionPullAllResponseHandler;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.ServerVersion;
import org.neo4j.driver.internal.value.NullValue;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.StatementResultCursor;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.NoSuchRecordException;
import org.neo4j.driver.v1.util.Pair;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.BoltServerAddress.LOCAL_DEFAULT;
import static org.neo4j.driver.v1.Records.column;
import static org.neo4j.driver.v1.Values.ofString;
import static org.neo4j.driver.v1.Values.value;

public class InternalStatementResultTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void iterationShouldWorksAsExpected()
    {
        // GIVEN
        StatementResult result = createResult( 3 );

        // WHEN
        assertTrue( result.hasNext() );
        assertThat( values( result.next() ), equalTo( asList( value( "v1-1" ), value( "v2-1" ) ) ) );

        assertTrue( result.hasNext() );
        assertThat( values( result.next() ), equalTo( asList( value( "v1-2" ), value( "v2-2" ) ) ) );

        assertTrue( result.hasNext() ); //1 -> 2

        // THEN
        assertThat( values( result.next() ), equalTo( asList( value( "v1-3" ), value( "v2-3" ) ) ) );
        assertFalse( result.hasNext() );

        expectedException.expect( NoSuchRecordException.class );

        // WHEN
        assertNull( result.next() );
    }

    @Test
    public void firstOfFieldNameShouldWorkAsExpected()
    {
        // GIVEN
        StatementResult result = createResult( 3 );

        // THEN
        assertThat( result.next().get( "k1" ), equalTo( value( "v1-1" ) ) );
        assertTrue( result.hasNext() );
    }

    @Test
    public void firstOfFieldIndexShouldWorkAsExpected()
    {
        // GIVEN
        StatementResult result = createResult( 3 );

        // THEN
        assertThat( result.next().get( 0 ), equalTo( value( "v1-1" ) ) );
        assertTrue( result.hasNext() );
    }

    @Test
    public void singlePastFirstShouldFail()
    {
        // GIVEN
        StatementResult result = createResult( 2 );
        result.next();
        result.next();


        // THEN
        expectedException.expect( NoSuchRecordException.class );

        // THEN
        result.single();
    }

    @Test
    public void singleNoneShouldFail()
    {
        // GIVEN
        StatementResult result = createResult( 0 );


        // THEN
        expectedException.expect( NoSuchRecordException.class );

        // THEN
        result.single();
    }

    @Test
    public void singleWhenMoreThanOneShouldFail()
    {
        // GIVEN
        StatementResult result = createResult( 2 );


        // THEN
        expectedException.expect( NoSuchRecordException.class );

        // THEN
        result.single();
    }

    @Test
    public void singleOfFieldNameShouldWorkAsExpected()
    {
        // GIVEN
        StatementResult result = createResult( 1 );

        // THEN
        assertThat( result.single().get( "k1" ), equalTo( value( "v1-1" ) ) );
        assertFalse( result.hasNext() );
    }

    @Test
    public void singleOfFieldIndexShouldWorkAsExpected()
    {
        // GIVEN
        StatementResult result = createResult( 1 );

        // THEN
        assertThat( result.single().get( 0 ), equalTo( value( "v1-1" ) ) );
        assertFalse( result.hasNext() );
    }

    @Test
    public void singleShouldWorkAsExpected()
    {
        assertNotNull( createResult( 1 ).single() );
    }

    @Test
    public void singleShouldThrowOnBigResult()
    {
        // Expect
        expectedException.expect( NoSuchRecordException.class );

        // When
        createResult( 42 ).single();
    }

    @Test
    public void singleShouldThrowOnEmptyResult()
    {
        // Expect
        expectedException.expect( NoSuchRecordException.class );

        // When
        createResult( 0 ).single();
    }

    @Test
    public void singleShouldThrowOnConsumedResult()
    {
        // Expect
        expectedException.expect( NoSuchRecordException.class );

        // When
        StatementResult result = createResult( 2 );
        result.consume();
        result.single();
    }

    @Test
    public void shouldConsumeTwice()
    {
        // GIVEN
        StatementResult result = createResult( 2 );
        result.consume();

        // WHEN
        result.consume();

        // THEN
        assertFalse( result.hasNext() );
    }

    @Test
    public void shouldList()
    {
        // GIVEN
        StatementResult result = createResult( 2 );
        List<String> records = result.list( column( "k1", ofString() ) );

        // THEN
        assertThat( records, equalTo( asList( "v1-1", "v1-2" ) ) );
    }

    @Test
    public void shouldListTwice()
    {
        // GIVEN
        StatementResult result = createResult( 2 );
        List<Record> firstList = result.list();
        assertThat( firstList.size(), equalTo( 2 ) );

        // THEN
        List<Record> secondList = result.list();
        assertThat( secondList.size(), equalTo( 0 ) );
    }

    @Test
    public void singleShouldNotThrowOnPartiallyConsumedResult()
    {
        // Given
        StatementResult result = createResult( 2 );
        result.next();

        // When + Then
        assertNotNull( result.single() );
    }

    @Test
    public void singleShouldConsumeIfFailing()
    {
        // Given
        StatementResult result = createResult( 2 );

        try
        {
            result.single();
            fail( "Exception expected" );
        }
        catch ( NoSuchRecordException e )
        {
            assertFalse( result.hasNext() );
        }
    }

    @Test
    public void retainShouldWorkAsExpected()
    {
        // GIVEN
        StatementResult result = createResult( 3 );

        // WHEN
        List<Record> records = result.list();

        // THEN
        assertFalse( result.hasNext() );
        assertThat( records, hasSize( 3 ) );
    }

    @Test
    public void retainAndMapByKeyShouldWorkAsExpected()
    {
        // GIVEN
        StatementResult result = createResult( 3 );

        // WHEN
        List<Value> records = result.list( column( "k1" ) );

        // THEN
        assertFalse( result.hasNext() );
        assertThat( records, hasSize( 3 ) );
    }

    @Test
    public void retainAndMapByIndexShouldWorkAsExpected()
    {
        // GIVEN
        StatementResult result = createResult( 3 );

        // WHEN
        List<Value> records = result.list( column( 0 ) );

        // THEN
        assertFalse( result.hasNext() );
        assertThat( records, hasSize( 3 ) );
    }

    @Test
    public void accessingOutOfBoundsShouldBeNull()
    {
        // GIVEN
        StatementResult result = createResult( 1 );

        // WHEN
        Record record = result.single();

        // THEN
        assertThat( record.get( 0 ), equalTo( value( "v1-1" ) ) );
        assertThat( record.get( 1 ), equalTo( value( "v2-1" ) ) );
        assertThat( record.get( 2 ), equalTo( NullValue.NULL ) );
        assertThat( record.get( -37 ), equalTo( NullValue.NULL ) );
    }

    @Test
    public void accessingKeysWithoutCallingNextShouldNotFail()
    {
        // GIVEN
        StatementResult result = createResult( 11 );

        // WHEN
        // not calling next or single

        // THEN
        assertThat( result.keys(), equalTo( asList( "k1", "k2" ) ) );
    }

    @Test
    public void shouldPeekIntoTheFuture()
    {
        // WHEN
        StatementResult result = createResult( 2 );

        // THEN
        assertThat( result.peek().get( "k1" ), equalTo( value( "v1-1" ) ) );

        // WHEN
        result.next();

        // THEN
        assertThat( result.peek().get( "k1" ), equalTo( value( "v1-2" ) ) );

        // WHEN
        result.next();

        // THEN
        expectedException.expect( NoSuchRecordException.class );

        // WHEN
        result.peek();
    }

    @Test
    public void shouldNotPeekIntoTheFutureWhenResultIsEmpty()
    {
        // GIVEN
        StatementResult result = createResult( 0 );

        // THEN
        expectedException.expect( NoSuchRecordException.class );

        // WHEN
        Record future = result.peek();
    }

    private StatementResult createResult( int numberOfRecords )
    {
        RunResponseHandler runHandler = new RunResponseHandler( new CompletableFuture<>() );
        runHandler.onSuccess( singletonMap( "fields", value( Arrays.asList( "k1", "k2" ) ) ) );

        Statement statement = new Statement( "<unknown>" );
        Connection connection = mock( Connection.class );
        when( connection.serverAddress() ).thenReturn( LOCAL_DEFAULT );
        when( connection.serverVersion() ).thenReturn( ServerVersion.v3_2_0 );
        PullAllResponseHandler pullAllHandler = new SessionPullAllResponseHandler( statement, runHandler, connection );

        for ( int i = 1; i <= numberOfRecords; i++ )
        {
            pullAllHandler.onRecord( new Value[]{value( "v1-" + i ), value( "v2-" + i )} );
        }
        pullAllHandler.onSuccess( emptyMap() );

        StatementResultCursor cursor = new InternalStatementResultCursor( runHandler, pullAllHandler );
        return new InternalStatementResult( connection, cursor );
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
