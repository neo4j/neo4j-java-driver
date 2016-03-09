/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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


import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.value.NullValue;
import org.neo4j.driver.v1.Pair;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Records;
import org.neo4j.driver.v1.ResultStream;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.NoSuchRecordException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.neo4j.driver.v1.Values.value;

public class InternalResultStreamTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void iterationShouldWorksAsExpected()
    {
        // GIVEN
        ResultStream result = createResult( 3 );

        // WHEN
        assertTrue( result.hasNext() );
        assertThat( values( result.first() ), equalTo(Arrays.asList(value("v1-1"), value( "v2-1" ))));

        assertTrue( result.hasNext() );
        assertThat( values( result.next() ), equalTo(Arrays.asList(value("v1-2"), value( "v2-2" ))));

        assertTrue( result.hasNext() ); //1 -> 2

        // THEN
        assertThat( values( result.next() ), equalTo(Arrays.asList(value("v1-3"), value( "v2-3" ))));
        assertFalse( result.hasNext() );
        assertNull( result.next() );
    }

    @Test
    public void firstPastFirstShouldFail()
    {
        // GIVEN
        ResultStream result = createResult( 3 );
        result.next();
        result.next();


        // THEN
        expectedException.expect( NoSuchRecordException.class );

        // THEN
        result.first();
    }

    @Test
    public void firstOfFieldNameShouldWorkAsExpected()
    {
        // GIVEN
        ResultStream result = createResult( 3 );

        // THEN
        assertThat( result.first().get("k1"), equalTo( value("v1-1") ) );
        assertTrue( result.hasNext() );
    }

    @Test
    public void firstOfFieldIndexShouldWorkAsExpected()
    {
        // GIVEN
        ResultStream result = createResult( 3 );

        // THEN
        assertThat( result.first().get(0), equalTo( value("v1-1") ) );
        assertTrue( result.hasNext() );
    }

    @Test
    public void singlePastFirstShouldFail()
    {
        // GIVEN
        ResultStream result = createResult( 2 );
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
        ResultStream result = createResult( 0 );


        // THEN
        expectedException.expect( NoSuchRecordException.class );

        // THEN
        result.single();
    }

    @Test
    public void singleWhenMoreThanOneShouldFail()
    {
        // GIVEN
        ResultStream result = createResult( 2 );


        // THEN
        expectedException.expect( NoSuchRecordException.class );

        // THEN
        result.single();
    }

    @Test
    public void singleOfFieldNameShouldWorkAsExpected()
    {
        // GIVEN
        ResultStream result = createResult( 1 );

        // THEN
        assertThat( result.single().get("k1"), equalTo( value("v1-1") ) );
        assertFalse( result.hasNext() );
    }

    @Test
    public void singleOfFieldIndexShouldWorkAsExpected()
    {
        // GIVEN
        ResultStream result = createResult( 1 );

        // THEN
        assertThat( result.single().get(0), equalTo( value("v1-1") ) );
        assertFalse( result.hasNext() );
    }

    @Test
    public void firstThrowsOnEmptyStream()
    {
        // Expect
        expectedException.expect( NoSuchRecordException.class );

        // When
        createResult( 0 ).first();
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
    public void skipShouldWorkAsExpected()
    {
        // GIVEN
        ResultStream result = createResult( 42 );

        // WHEN
        assertThat( result.skip( 21 ), equalTo( 21L ) );

        // THEN
        assertThat( values( result.next() ), equalTo( Arrays.asList( value( "v1-22" ), value(
                "v2-22" ) ) ) );
    }

    @Test
    public void skipBeyondNumberOfRecords()
    {
        // GIVEN
        ResultStream result = createResult( 10 );

        // WHEN
        assertThat(result.skip( 20 ), equalTo(10L));

        // THEN
        assertFalse( result.hasNext() );
    }

    @Test
    public void skipThrowsIfNegativeNumber()
    {
        ResultStream result = createResult( 10 );
        result.skip( 5 );

        expectedException.expect( ClientException.class );
        result.skip( -1 );
    }

    @Test
    public void limitShouldWorkAsExpected()
    {
        // GIVEN
        ResultStream result = createResult( 42 );
        result.limit( 10 );

        // THEN
        assertThat( result.list().size(), equalTo( 10 ) );
    }

    @Test
    public void limitZeroShouldWorkAsExpected1()
    {
        // GIVEN
        ResultStream result = createResult( 42 );
        result.limit( 0 );

        // THEN
        assertThat( result.list().size(), equalTo( 0 ) );
    }

    @Test
    public void limitZeroShouldWorkAsExpected2()
    {
        // GIVEN
        ResultStream result = createResult( 10 );
        result.skip( 4 );
        result.limit( 0 );

        // THEN
        assertFalse( result.hasNext() );
        assertNull( result.next() );
    }

    @Test
    public void limitOnEmptyResultShouldWorkAsExpected()
    {
        // GIVEN
        ResultStream result = createResult( 0 );
        result.limit( 10 );

        // THEN
        assertThat( result.list().size(), equalTo( 0 ) );
    }

    @Test
    public void changingLimitShouldWorkAsExpected()
    {
        // GIVEN
        ResultStream result = createResult( 6 );
        result.limit( 1 );
        result.limit( 60 );

        // THEN
        assertThat( result.list().size(), equalTo( 6 ) );
    }

    @Test
    public void retainShouldWorkAsExpected()
    {
        // GIVEN
        ResultStream result = createResult( 3 );

        // WHEN
        List<Record> records = result.list();

        // THEN
        assertFalse(result.hasNext());
        assertThat(records, hasSize( 3 ) );
    }

    @Test
    public void retainAndMapByKeyShouldWorkAsExpected()
    {
        // GIVEN
        ResultStream result = createResult( 3 );

        // WHEN
        List<Value> records = result.list( Records.columnAsIs( "k1" ) );

        // THEN
        assertFalse(result.hasNext());
        assertThat(records, hasSize( 3 ) );
    }

    @Test
    public void retainAndMapByIndexShouldWorkAsExpected()
    {
        // GIVEN
        ResultStream result = createResult( 3 );

        // WHEN
        List<Value> records = result.list( Records.columnAsIs( 0 ) );

        // THEN
        assertFalse(result.hasNext());
        assertThat(records, hasSize( 3 ) );
    }

    @Test
    public void retainFailsIfItCannotRetainEntireResult()
    {
        ResultStream result = createResult( 17 );
        result.skip( 5 );

        expectedException.expect( ClientException.class );
        result.list();
    }

    @Test
    public void accessingOutOfBoundsShouldBeNull()
    {
        // GIVEN
        ResultStream result = createResult( 1 );

        // WHEN
        Record record = result.first();

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
        ResultStream result = createResult( 11 );

        // WHEN
        // not calling next, first, single, nor skip

        // THEN
        assertThat( result.keys(), equalTo( Arrays.asList( "k1", "k2" ) ) );
    }

    @Test
    public void shouldPeekIntoTheFuture()
    {
        // WHEN
        ResultStream result = createResult( 2 );

        // THEN
        assertThat( result.peek().get( "k1" ), equalTo( value( "v1-1" ) ) );

        // WHEN
        result.next();

        // THEN
        assertThat( result.peek().get( "k1" ), equalTo( value( "v1-2" ) ) );

        // WHEN
        result.next();

        // THEN
        assertNull( result.peek() );
    }

    @Test
    public void shouldNotPeekIntoTheFutureWhenResultIsEmpty()
    {
        // GIVEN
        ResultStream result = createResult( 0 );
        Record future = result.peek();

        // WHEN
        assertNull( future );
    }

    private ResultStream createResult( int numberOfRecords )
    {
        Connection connection = mock( Connection.class );
        String statement = "<unknown>";

        final InternalResultStream cursor = new InternalResultStream( connection, statement, ParameterSupport
                .NO_PARAMETERS );

        // Each time the cursor calls `recieveOne`, we'll run one of these,
        // to emulate how messages are handed over to the cursor
        final LinkedList<Runnable> inboundMessages = new LinkedList<>();

        inboundMessages.add( streamHeadMessage( cursor ) );
        for ( int i = 1; i <= numberOfRecords; i++ )
        {
            inboundMessages.add( recordMessage( cursor, i ) );
        }
        inboundMessages.add( streamTailMessage( cursor ) );

        doAnswer( new Answer()
        {
            @Override
            public Object answer( InvocationOnMock invocationOnMock ) throws Throwable
            {
                inboundMessages.poll().run();
                return null;
            }
        }).when( connection ).receiveOne();

        return cursor;
    }

    private Runnable streamTailMessage( final InternalResultStream cursor )
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                cursor.pullAllResponseCollector().done();
            }
        };
    }

    private Runnable recordMessage( final InternalResultStream cursor, final int val )
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                cursor.pullAllResponseCollector().record( new Value[]{value( "v1-" + val ), value( "v2-" + val )} );
            }
        };
    }

    private Runnable streamHeadMessage( final InternalResultStream cursor )
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                cursor.runResponseCollector().keys( new String[]{"k1", "k2"} );
                cursor.runResponseCollector().done();
            }
        };
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
