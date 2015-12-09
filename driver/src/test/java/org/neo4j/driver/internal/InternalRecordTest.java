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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.junit.Test;

import org.neo4j.driver.internal.value.NullValue;
import org.neo4j.driver.v1.Function;
import org.neo4j.driver.v1.Value;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.neo4j.driver.v1.Values.value;

public class InternalRecordTest
{
    @Test
    public void accessingUnknownKeyShouldBeNull()
    {
        InternalRecord record = createRecord();

        assertThat( record.value( "k1" ), equalTo( value( 0 ) ) );
        assertThat( record.value( "k2" ), equalTo( value( 1 ) ) );
        assertThat( record.value( "k3" ), equalTo( NullValue.NULL ) );
    }

    @Test
    public void shouldHaveCorrectFieldCount()
    {
        InternalRecord record = createRecord();
        assertThat( record.fieldCount(), equalTo( 2 ) );
    }

    @Test
    public void shouldHaveCorrectFieldIndices()
    {
        InternalRecord record = createRecord();
        assertThat( record.fieldIndex( "k1" ), equalTo( 0 ) );
        assertThat( record.fieldIndex( "k2" ), equalTo( 1 ) );
    }

    @Test
    public void shouldThrowWhenAskingForIndexOfUnknownField()
    {
        InternalRecord record = createRecord();
        try
        {
            record.fieldIndex( "BATMAN" );
            fail( "Expected NoSuchElementException to be thrown" );
        }
        catch ( NoSuchElementException e )
        {
            // yay
        }
    }

    @Test
    public void accessingOutOfBoundsShouldBeNull()
    {
        InternalRecord record = createRecord();

        assertThat( record.value( 0 ), equalTo( value( 0 ) ) );
        assertThat( record.value( 1 ), equalTo( value( 1 ) ) );
        assertThat( record.value( 2 ), equalTo( NullValue.NULL ) );
        assertThat( record.value( -37 ), equalTo( NullValue.NULL ) );
    }

    @Test
    public void testContainsKey()
    {
        InternalRecord record = createRecord();

        assertTrue( record.containsKey( "k1" ) );
        assertTrue( record.containsKey( "k2" ) );
        assertFalse( record.containsKey( "k3" ) );
    }

    @Test
    public void testAsMap()
    {
        // GIVEN
        InternalRecord record = createRecord();

        // WHEN
        Map<String,Value> map = record.asMap();

        // THEN
        assertThat( map.keySet(), containsInAnyOrder( "k1", "k2" ) );
        assertThat( map.get( "k1" ), equalTo( value( 0 ) ) );
        assertThat( map.get( "k2" ), equalTo( value( 1 ) ) );
    }

    @Test
    public void testMapExtraction()
    {
        // GIVEN
        InternalRecord record = createRecord();
        Function<Value,Integer> addOne = new Function<Value,Integer>()
        {
            @Override
            public Integer apply( Value value )
            {
                return value.asInt() + 1;
            }
        };

        // WHEN
        Map<String,Integer> map = record.asMap( addOne );

        // THEN
        assertThat( map.keySet(), containsInAnyOrder( "k1", "k2" ) );
        assertThat( map.get( "k1" ), equalTo( 1 ) );
        assertThat( map.get( "k2" ), equalTo( 2 ) );
    }

    @Test
    public void testToString()
    {
        InternalRecord record = createRecord();

        assertThat( record.toString(), equalTo( "Record<{k1: 0 :: INTEGER, k2: 1 :: INTEGER}>" ) );
    }


    private InternalRecord createRecord()
    {
        List<String> keys = Arrays.asList( "k1", "k2" );
        HashMap<String,Integer> lookup = new HashMap<>();
        lookup.put( "k1", 0 );
        lookup.put( "k2", 1 );

        return new InternalRecord( keys, lookup, new Value[]{value( 0 ), value( 1 )} );
    }
}
