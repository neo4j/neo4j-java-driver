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
package org.neo4j.driver.v1.internal;


import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.driver.v1.ImmutableRecord;
import org.neo4j.driver.v1.Property;
import org.neo4j.driver.v1.Result;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.internal.summary.ResultBuilder;
import org.neo4j.driver.v1.internal.value.NullValue;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.driver.v1.Values.value;

public class SimpleResultTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void iterationShouldWorksAsExpected()
    {
        // GIVEN
        Result result = createResult( 3 );

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
        Result result = createResult( 3 );

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
        Result result = createResult( 42 );

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
        Result result = createResult( 10 );

        // WHEN
        assertThat(result.skip( 20 ), equalTo(10L));

        // THEN
        assertThat( result.position(), equalTo( 9L ) );
    }

    @Test
    public void skipThrowsIfNegativeNumber()
    {
        Result result = createResult( 10 );
        result.skip( 5 );

        expectedException.expect( IllegalArgumentException.class );
        result.skip( -1 );
    }

    @Test
    public void retainShouldWorkAsExpected()
    {
        // GIVEN
        Result result = createResult( 3 );

        // WHEN
        List<ImmutableRecord> records = result.retain();

        // THEN
        assertTrue(result.atEnd());
        assertThat(records, hasSize( 3 ) );
    }

    @Test
    public void retainFailsIfItCannotRetainEntireResult()
    {
        Result result = createResult( 17 );
        result.skip( 5 );

        expectedException.expect( IllegalStateException.class );
        result.retain();
    }

    @Test
    public void accessingOutOfBoundsShouldBeNull()
    {
        // GIVEN
        Result result = createResult( 1 );

        // WHEN
        result.first();

        // THEN
        assertThat( result.value( 0 ), equalTo( value( "v1-1" ) ) );
        assertThat( result.value( 1 ), equalTo( value( "v2-1" ) ) );
        assertThat( result.value( 2 ), equalTo( NullValue.NULL ) );
        assertThat( result.value( -37 ), equalTo( NullValue.NULL ) );
    }

    private Result createResult( int numberOfRecords )
    {
        ResultBuilder builder = new ResultBuilder( "<unknown>", ParameterSupport.NO_PARAMETERS );
        builder.keys( new String[]{"k1", "k2"} );
        for ( int i = 1; i <= numberOfRecords; i++ )
        {
            builder.record( new Value[]{value( "v1-" + i ), value( "v2-" + i )} );
        }
        return builder.build();
    }

    private List<Value> values( ImmutableRecord record )
    {
        List<Value> result = new ArrayList<>( record.keys().size() );
        for ( Property<Value> property : record.fields() )
        {
            result.add( property.value() );
        }
        return result;
    }
}
