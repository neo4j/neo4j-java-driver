/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.internal.util.Extract;
import org.neo4j.driver.internal.util.Iterables;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.util.Function;
import org.neo4j.driver.v1.util.Pair;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.driver.v1.Values.value;

class ExtractTest
{
    @Test
    void extractEmptyArrayShouldNotBeModifiable()
    {
        List<Value> list = Extract.list( new Value[]{} );

        assertThat( list, empty() );
        assertThrows( UnsupportedOperationException.class, () -> list.add( null ) );
    }

    @Test
    void extractSingletonShouldNotBeModifiable()
    {
        List<Value> list = Extract.list( new Value[]{value( 42 )} );

        assertThat( list, equalTo( singletonList( value( 42 ) ) ) );
        assertThrows( UnsupportedOperationException.class, () -> list.add( null ) );
    }

    @Test
    void extractMultipleShouldNotBeModifiable()
    {
        List<Value> list = Extract.list( new Value[]{value( 42 ), value( 43 )} );

        assertThat( list, equalTo( asList( value( 42 ), value( 43 ) ) ) );
        assertThrows( UnsupportedOperationException.class, () -> list.add( null ) );
    }

    @Test
    void testMapOverList()
    {
        List<Integer> mapped = Extract.list( new Value[]{value( 42 ), value( 43 )}, integerExtractor() );

        assertThat( mapped, equalTo( Arrays.asList( 42, 43 ) ) );
    }

    @Test
    void testMapValues()
    {
        // GIVEN
        Map<String,Value> map = new HashMap<>();
        map.put( "k1", value( 43 ) );
        map.put( "k2", value( 42 ) );

        // WHEN
        Map<String,Integer> mappedMap = Extract.map( map, integerExtractor() );

        // THEN
        Collection<Integer> values = mappedMap.values();
        assertThat( values, containsInAnyOrder( 43, 42 ) );

    }

    @Test
    void testShouldPreserveMapOrderMapValues()
    {
        // GIVEN
        Map<String,Value> map = Iterables.newLinkedHashMapWithSize( 2 );
        map.put( "k2", value( 43 ) );
        map.put( "k1", value( 42 ) );

        // WHEN
        Map<String,Integer> mappedMap = Extract.map( map, integerExtractor() );

        // THEN
        Collection<Integer> values = mappedMap.values();
        assertThat( values, contains( 43, 42) );

    }

    @Test
    void testProperties()
    {
        // GIVEN
        Map<String,Value> props = new HashMap<>();
        props.put( "k1", value( 43 ) );
        props.put( "k2", value( 42 ) );
        InternalNode node = new InternalNode( 42L, Collections.singletonList( "L" ), props );

        // WHEN
        Iterable<Pair<String, Integer>> properties = Extract.properties( node, integerExtractor() );

        // THEN
        Iterator<Pair<String, Integer>> iterator = properties.iterator();
        assertThat( iterator.next(), equalTo( InternalPair.of( "k1", 43 ) ) );
        assertThat( iterator.next(), equalTo( InternalPair.of( "k2", 42 ) ) );
        assertFalse( iterator.hasNext() );
    }

    @Test
    void testFields()
    {
        // GIVEN
        InternalRecord record = new InternalRecord( Arrays.asList( "k1" ), new Value[]{value( 42 )} );
        // WHEN
        List<Pair<String, Integer>> fields = Extract.fields( record, integerExtractor() );


        // THEN
        assertThat( fields, equalTo( Collections.singletonList( InternalPair.of( "k1", 42 ) ) ) );
    }

    private Function<Value,Integer> integerExtractor()
    {
        return new Function<Value,Integer>()
        {

            @Override
            public Integer apply( Value value )
            {
                return value.asInt();
            }
        };
    }

}
