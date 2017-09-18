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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.ResultResourcesHandler;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.Extract;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.util.Function;
import org.neo4j.driver.v1.util.Pair;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.neo4j.driver.v1.Values.ofValue;
import static org.neo4j.driver.v1.Values.value;

public class ExtractTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void extractEmptyArrayShouldNotBeModifiable() throws Exception
    {
        List<Value> list = Extract.list( new Value[]{} );

        assertThat( list, empty() );
        exception.expect( UnsupportedOperationException.class );
        list.add( null );
    }

    @Test
    public void extractSingletonShouldNotBeModifiable() throws Exception
    {
        List<Value> list = Extract.list( new Value[]{value( 42 )} );

        assertThat( list, equalTo( singletonList( value( 42 ) ) ) );
        exception.expect( UnsupportedOperationException.class );
        list.add( null );
    }

    @Test
    public void extractMultipleShouldNotBeModifiable() throws Exception
    {
        List<Value> list = Extract.list( new Value[]{value( 42 ), value( 43 )} );

        assertThat( list, equalTo( asList( value( 42 ), value( 43 ) ) ) );
        exception.expect( UnsupportedOperationException.class );
        list.add( null );
    }

    @Test
    public void testMapOverList() throws Exception
    {
        List<Integer> mapped = Extract.list( new Value[]{value( 42 ), value( 43 )}, integerExtractor() );

        assertThat( mapped, equalTo( Arrays.asList( 42, 43 ) ) );
    }

    @Test
    public void testMapShouldNotBeModifiable() throws Exception
    {
        // GIVEN
        Map<String,Value> map = new HashMap<>();
        map.put( "k1", value( "foo" ) );
        map.put( "k2", value( 42 ) );

        // WHEN
        Map<String,Value> valueMap = Extract.map( map );

        // THEN
        exception.expect( UnsupportedOperationException.class );
        valueMap.put( "foo", value( "bar" ) );
    }

    @Test
    public void testMapValues() throws Exception
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
    public void testShouldPreserveMapOrderMapValues() throws Exception
    {
        // GIVEN
        Map<String,Value> map = new LinkedHashMap<>();
        map.put( "k2", value( 43 ) );
        map.put( "k1", value( 42 ) );

        // WHEN
        Map<String,Integer> mappedMap = Extract.map( map, integerExtractor() );

        // THEN
        Collection<Integer> values = mappedMap.values();
        assertThat( values, contains( 43, 42) );

    }

    @Test
    public void testProperties() throws Exception
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
    public void testFields() throws Exception
    {
        // GIVEN
        Connection connection = mock( Connection.class );
        String statement = "<unknown>";

        Statement stmt = new Statement( statement );
        InternalStatementResult result = new InternalStatementResult( stmt, connection, ResultResourcesHandler.NO_OP );
        result.runResponseHandler().onSuccess( singletonMap( "fields", value( singletonList( "k1" ) ) ) );
        result.pullAllResponseHandler().onRecord( new Value[]{value( 42 )} );
        result.pullAllResponseHandler().onSuccess( Collections.<String,Value>emptyMap() );

        connection.run( statement, Values.EmptyMap.asMap( ofValue() ), result.runResponseHandler() );
        connection.pullAll( result.pullAllResponseHandler() );
        connection.flush();

        // WHEN
        List<Pair<String, Integer>> fields = Extract.fields( result.single(), integerExtractor() );


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
