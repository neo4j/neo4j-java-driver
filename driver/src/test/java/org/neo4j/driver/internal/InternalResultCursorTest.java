/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.driver.internal.summary.ResultBuilder;
import org.neo4j.driver.internal.value.NullValue;
import org.neo4j.driver.v1.Pair;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.RecordAccessor;
import org.neo4j.driver.v1.Records;
import org.neo4j.driver.v1.ResultCursor;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.NoRecordException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.neo4j.driver.v1.Values.value;

public class InternalResultCursorTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void iterationShouldWorksAsExpected()
    {
        // GIVEN
        ResultCursor result = createResult( 3 );

        // WHEN
        assertThat( result.position(), equalTo( -1L ) );
        assertTrue( result.next() ); //-1 -> 0
        assertTrue( result.first() );
        assertFalse( result.atEnd() );
        assertThat( values( result.record() ), equalTo(Arrays.asList(value("v1-1"), value( "v2-1" ))));

        assertThat( result.position(), equalTo( 0L ) );
        assertTrue( result.next() ); //0 -> 1
        assertFalse( result.first() );
        assertFalse( result.atEnd() );
        assertThat( values( result.record() ), equalTo(Arrays.asList(value("v1-2"), value( "v2-2" ))));

        assertThat( result.position(), equalTo( 1L ) );
        assertTrue( result.next() ); //1 -> 2

        // THEN
        assertThat( result.position(), equalTo( 2L ) );
        assertTrue( result.atEnd() );
        assertFalse( result.first() );
        assertThat( values( result.record() ), equalTo(Arrays.asList(value("v1-3"), value( "v2-3" ))));
        assertFalse( result.next() );
    }

    @Test
    public void firstFalseOnEmptyStream()
    {
        assertFalse( createResult( 0 ).first() );
    }

    @Test
    public void firstMovesCursorOnce()
    {
        // GIVEN
        ResultCursor result = createResult( 3 );

        // WHEN
        assertThat( result.position(), equalTo( -1L ) );
        assertTrue( result.first() );
        assertThat( result.position(), equalTo( 0L ) );
        assertTrue( result.first() );
        assertThat( result.position(), equalTo( 0L ) );
    }

    @Test
    public void singleShouldWorkAsExpected()
    {
        assertFalse( createResult( 42 ).single() );
        assertFalse( createResult( 0 ).single() );
        assertTrue( createResult( 1 ).single() );
    }

    @Test
    public void skipShouldWorkAsExpected()
    {
        // GIVEN
        ResultCursor result = createResult( 42 );

        // WHEN
        assertThat(result.skip( 22 ), equalTo(22L));

        // THEN
        assertThat( result.position(), equalTo( 21L ) );
        assertThat( values( result.record() ), equalTo( Arrays.asList( value( "v1-22" ), value( "v2-22" ) ) ));
    }

    @Test
    public void skipBeyondNumberOfRecords()
    {
        // GIVEN
        ResultCursor result = createResult( 10 );

        // WHEN
        assertThat(result.skip( 20 ), equalTo(10L));

        // THEN
        assertThat( result.position(), equalTo( 9L ) );
    }

    @Test
    public void skipThrowsIfNegativeNumber()
    {
        ResultCursor result = createResult( 10 );
        result.skip( 5 );

        expectedException.expect( ClientException.class );
        result.skip( -1 );
    }

    @Test
    public void limitShouldWorkAsExpected()
    {
        // GIVEN
        ResultCursor result = createResult( 42 );
        result.limit( 10 );

        // THEN
        assertThat( result.list().size(), equalTo( 10 ) );
    }

    @Test
    public void limitZeroShouldWorkAsExpected1()
    {
        // GIVEN
        ResultCursor result = createResult( 42 );
        result.limit( 0 );

        // THEN
        assertThat( result.list().size(), equalTo( 0 ) );
    }

    @Test
    public void limitZeroShouldWorkAsExpected2()
    {
        // GIVEN
        ResultCursor result = createResult( 10 );
        result.skip( 4 );
        result.limit( 0 );

        // THEN
        assertTrue( result.atEnd() );
        assertFalse( result.next() );
    }

    @Test
    public void limitOnEmptyResultShouldWorkAsExpected()
    {
        // GIVEN
        ResultCursor result = createResult( 0 );
        result.limit( 10 );

        // THEN
        assertThat( result.list().size(), equalTo( 0 ) );
    }

    @Test
    public void changingLimitShouldWorkAsExpected()
    {
        // GIVEN
        ResultCursor result = createResult( 6 );
        result.limit( 1 );
        result.limit( 60 );

        // THEN
        assertThat( result.list().size(), equalTo( 6 ) );
    }

    @Test
    public void retainShouldWorkAsExpected()
    {
        // GIVEN
        ResultCursor result = createResult( 3 );

        // WHEN
        List<Record> records = result.list();

        // THEN
        assertTrue(result.atEnd());
        assertThat(records, hasSize( 3 ) );
    }

    @Test
    public void retainAndMapByKeyShouldWorkAsExpected()
    {
        // GIVEN
        ResultCursor result = createResult( 3 );

        // WHEN
        List<Value> records = result.list( Records.columnAsIs( "k1" ) );

        // THEN
        assertTrue(result.atEnd());
        assertThat(records, hasSize( 3 ) );
    }

    @Test
    public void retainAndMapByIndexShouldWorkAsExpected()
    {
        // GIVEN
        ResultCursor result = createResult( 3 );

        // WHEN
        List<Value> records = result.list( Records.columnAsIs( 0 ) );

        // THEN
        assertTrue(result.atEnd());
        assertThat(records, hasSize( 3 ) );
    }

    @Test
    public void retainFailsIfItCannotRetainEntireResult()
    {
        ResultCursor result = createResult( 17 );
        result.skip( 5 );

        expectedException.expect( ClientException.class );
        result.list();
    }

    @Test
    public void accessingOutOfBoundsShouldBeNull()
    {
        // GIVEN
        ResultCursor result = createResult( 1 );

        // WHEN
        result.first();

        // THEN
        assertThat( result.value( 0 ), equalTo( value( "v1-1" ) ) );
        assertThat( result.value( 1 ), equalTo( value( "v2-1" ) ) );
        assertThat( result.value( 2 ), equalTo( NullValue.NULL ) );
        assertThat( result.value( -37 ), equalTo( NullValue.NULL ) );
    }

    @Test
    public void accessingRecordsWithoutCallingNextShouldFail()
    {
        // GIVEN
        ResultCursor result = createResult( 11 );

        // WHEN
        // not calling next, first, nor skip

        // THEN
        expectedException.expect( ClientException.class );
        result.record();
    }

    @Test
    public void accessingValueWithoutCallingNextShouldFail()
    {
        // GIVEN
        ResultCursor result = createResult( 11 );

        // WHEN
        // not calling next, first, nor skip

        // THEN
        expectedException.expect( ClientException.class );
        result.value( 1 );
    }

    @Test
    public void accessingFieldsWithoutCallingNextShouldFail()
    {
        // GIVEN
        ResultCursor result = createResult( 11 );

        // WHEN
        // not calling next, first, nor skip

        // THEN
        expectedException.expect( ClientException.class );
        result.fields( );
    }

    @Test
    public void accessingKeysWithoutCallingNextShouldNotFail()
    {
        // GIVEN
        ResultCursor result = createResult( 11 );

        // WHEN
        // not calling next, first, nor skip

        // THEN
        assertThat( result.keys( ), equalTo( Arrays.asList( "k1", "k2" ) ) );
    }

    @Test
    public void shouldHaveCorrectSize()
    {
        assertThat( createResult( 4 ).size(), equalTo( 2 ) );
    }

    @Test
    public void shouldPeekIntoTheFuture()
    {
        // GIVEN
        ResultCursor result = createResult( 2 );

        // WHEN
        RecordAccessor future = result.peek();

        // THEN
        assertTrue( future.hasRecord() );
        assertThat( future.value( "k1" ), equalTo( value( "v1-1" ) ) );

        // WHEN
        result.next();

        // THEN
        assertTrue( future.hasRecord() );
        assertThat( result.value( "k1" ), equalTo( value( "v1-1" ) ) );
        assertThat( future.value( "k1" ), equalTo( value( "v1-2" ) ) );

        // WHEN
        result.next();

        // THEN
        assertFalse( future.hasRecord() );
        assertThat( result.value( "k1" ), equalTo( value( "v1-2" ) ) );

        // AND THEN
        try
        {
            future.value( "k1" );
            fail( "Expected NoRecordException" );
        }
        catch ( NoRecordException e )
        {
            // yay
        }

    }

    @Test
    public void shouldNotPeekIntoTheFutureWhenResultIsEmpty()
    {
        // GIVEN
        ResultCursor result = createResult( 0 );
        RecordAccessor future = result.peek();

        // WHEN
        assertFalse( result.hasRecord() );
        assertFalse( future.hasRecord() );
    }

    private ResultCursor createResult( int numberOfRecords )
    {
        ResultBuilder builder = new ResultBuilder( "<unknown>", ParameterSupport.NO_PARAMETERS );
        builder.keys( new String[]{"k1", "k2"} );
        for ( int i = 1; i <= numberOfRecords; i++ )
        {
            builder.record( new Value[]{value( "v1-" + i ), value( "v2-" + i )} );
        }
        return builder.build();
    }

    private List<Value> values( Record record )
    {
        List<Value> result = new ArrayList<>( record.keys().size() );
        for ( Pair<String, Value> property : record.fields() )
        {
            result.add( property.value() );
        }
        return result;
    }
}
