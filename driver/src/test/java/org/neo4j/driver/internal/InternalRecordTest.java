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

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.neo4j.driver.internal.util.Extract;
import org.neo4j.driver.internal.value.NullValue;
import org.neo4j.driver.Value;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.Values.value;

class InternalRecordTest
{
    @Test
    void accessingUnknownKeyShouldBeNull()
    {
        InternalRecord record = createRecord();

        assertThat( record.get( "k1" ), equalTo( value( 0 ) ) );
        assertThat( record.get( "k2" ), equalTo( value( 1 ) ) );
        assertThat( record.get( "k3" ), equalTo( NullValue.NULL ) );
    }

    @Test
    void shouldHaveCorrectSize()
    {
        InternalRecord record = createRecord();
        assertThat( record.size(), equalTo( 2 ) );
    }

    @Test
    void shouldHaveCorrectFieldIndices()
    {
        InternalRecord record = createRecord();
        assertThat( record.index( "k1" ), equalTo( 0 ) );
        assertThat( record.index( "k2" ), equalTo( 1 ) );
    }

    @Test
    void shouldThrowWhenAskingForIndexOfUnknownField()
    {
        InternalRecord record = createRecord();
        assertThrows( NoSuchElementException.class, () -> record.index( "BATMAN" ) );
    }

    @Test
    void accessingOutOfBoundsShouldBeNull()
    {
        InternalRecord record = createRecord();

        assertThat( record.get( 0 ), equalTo( value( 0 ) ) );
        assertThat( record.get( 1 ), equalTo( value( 1 ) ) );
        assertThat( record.get( 2 ), equalTo( NullValue.NULL ) );
        assertThat( record.get( -37 ), equalTo( NullValue.NULL ) );
    }

    @Test
    void testContainsKey()
    {
        InternalRecord record = createRecord();

        assertTrue( record.containsKey( "k1" ) );
        assertTrue( record.containsKey( "k2" ) );
        assertFalse( record.containsKey( "k3" ) );
    }

    @Test
    void testIndex()
    {
        InternalRecord record = createRecord();

        assertThat( record.index( "k1" ), equalTo( 0 ) );
        assertThat( record.index( "k2" ), equalTo( 1 ) );
    }

    @Test
    void testAsMap()
    {
        // GIVEN
        InternalRecord record = createRecord();

        // WHEN
        Map<String,Object> map = record.asMap();

        // THEN
        assertThat( map.keySet(), containsInAnyOrder( "k1", "k2" ) );
        assertThat( map.get( "k1" ), equalTo( 0L ) );
        assertThat( map.get( "k2" ), equalTo( 1L ) );
    }

    @Test
    void testMapExtraction()
    {
        // GIVEN
        InternalRecord record = createRecord();
        Function<Value,Integer> addOne = value -> value.asInt() + 1;

        // WHEN
        Map<String,Integer> map = Extract.map( record, addOne );

        // THEN
        assertThat( map.keySet(), contains( "k1", "k2" ) );
        assertThat( map.get( "k1" ), equalTo( 1 ) );
        assertThat( map.get( "k2" ), equalTo( 2 ) );
    }

    @Test
    void mapExtractionShouldPreserveIterationOrder()
    {
        // GIVEN
        List<String> keys = Arrays.asList( "k2", "k1" );
        InternalRecord record =  new InternalRecord( keys, new Value[]{value( 0 ), value( 1 )} );
        Function<Value,Integer> addOne = value -> value.asInt() + 1;

        // WHEN
        Map<String,Integer> map = Extract.map( record, addOne );

        // THEN
        assertThat( map.keySet(), contains( "k2", "k1" )  );
        Iterator<Integer> values = map.values().iterator();
        assertThat( values.next(), equalTo( 1 ) );
        assertThat( values.next(), equalTo( 2 ) );
    }

    @Test
    void testToString()
    {
        InternalRecord record = createRecord();

        assertThat( record.toString(), equalTo( "Record<{k1: 0, k2: 1}>" ) );
    }

    @Test
    void shouldHaveMethodToGetKeys()
    {
        //GIVEN
        List<String> keys = Arrays.asList( "k2", "k1" );
        InternalRecord record =  new InternalRecord( keys, new Value[]{value( 0 ), value( 1 )} );

        //WHEN
        List<String> appendedKeys = record.keys();

        //THEN
        assertThat( appendedKeys, equalTo( keys ) );
    }

    @Test
    void emptyKeysShouldGiveEmptyList()
    {
        //GIVEN
        List<String> keys = Collections.emptyList();
        InternalRecord record =  new InternalRecord( keys, new Value[]{} );

        //WHEN
        List<String> appendedKeys = record.keys();

        //THEN
        assertThat( appendedKeys, equalTo( keys ) );
    }


    @Test
    void shouldHaveMethodToGetValues()
    {
        //GIVEN
        List<String> keys = Arrays.asList( "k2", "k1" );
        Value[] values = new Value[]{value( 0 ), value( 1 )};
        InternalRecord record =  new InternalRecord( keys, values );

        //WHEN
        List<Value> appendedValues = record.values();

        //THEN
        assertThat( appendedValues, equalTo( Arrays.asList( values ) ) );
    }

    @Test
    void emptyValuesShouldGiveEmptyList()
    {
        //GIVEN
        List<String> keys = Collections.emptyList();
        Value[] values = new Value[]{};
        InternalRecord record =  new InternalRecord( keys, values );

        //WHEN
        List<Value> appendedValues = record.values();

        //THEN
        assertThat( appendedValues, equalTo( Arrays.asList( values ) ) );
    }

    private InternalRecord createRecord()
    {
        List<String> keys = Arrays.asList( "k1", "k2" );
        return new InternalRecord( keys, new Value[]{value( 0 ), value( 1 )} );
    }
}
