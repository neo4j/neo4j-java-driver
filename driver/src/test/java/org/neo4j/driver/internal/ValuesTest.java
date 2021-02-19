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

import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZonedDateTime;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.driver.internal.value.DateTimeValue;
import org.neo4j.driver.internal.value.DateValue;
import org.neo4j.driver.internal.value.DurationValue;
import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.driver.internal.value.LocalDateTimeValue;
import org.neo4j.driver.internal.value.LocalTimeValue;
import org.neo4j.driver.internal.value.MapValue;
import org.neo4j.driver.internal.value.NodeValue;
import org.neo4j.driver.internal.value.PathValue;
import org.neo4j.driver.internal.value.RelationshipValue;
import org.neo4j.driver.internal.value.TimeValue;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.types.IsoDuration;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Path;
import org.neo4j.driver.types.Point;
import org.neo4j.driver.types.Relationship;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.driver.internal.util.ValueFactory.emptyNodeValue;
import static org.neo4j.driver.internal.util.ValueFactory.emptyRelationshipValue;
import static org.neo4j.driver.internal.util.ValueFactory.filledPathValue;
import static org.neo4j.driver.Values.isoDuration;
import static org.neo4j.driver.Values.ofDouble;
import static org.neo4j.driver.Values.ofFloat;
import static org.neo4j.driver.Values.ofInteger;
import static org.neo4j.driver.Values.ofList;
import static org.neo4j.driver.Values.ofLong;
import static org.neo4j.driver.Values.ofMap;
import static org.neo4j.driver.Values.ofNumber;
import static org.neo4j.driver.Values.ofObject;
import static org.neo4j.driver.Values.ofString;
import static org.neo4j.driver.Values.ofToString;
import static org.neo4j.driver.Values.point;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.Values.values;

class ValuesTest
{
    @Test
    void shouldConvertPrimitiveArrays()
    {
        assertThat( value( new int[]{1, 2, 3} ),
                equalTo( new ListValue( values( 1, 2, 3 ) ) ) );

        assertThat( value( new long[]{1, 2, 3} ),
                equalTo( new ListValue( values( 1, 2, 3 ) ) ) );

        assertThat( value( new float[]{1.1f, 2.2f, 3.3f} ),
                equalTo( new ListValue( values( 1.1f, 2.2f, 3.3f ) ) ) );

        assertThat( value( new double[]{1.1, 2.2, 3.3} ),
                equalTo( new ListValue( values( 1.1, 2.2, 3.3 ) ) ) );

        assertThat( value( new boolean[]{true, false, true} ),
                equalTo( new ListValue( values( true, false, true ) ) ) );

        assertThat( value( new char[]{'a', 'b', 'c'} ),
                equalTo( new ListValue( values( 'a', 'b', 'c' ) ) ) );

        assertThat( value( new String[]{"a", "b", "c"} ),
                equalTo( new ListValue( values( "a", "b", "c" ) ) ) );
    }

    @Test
    void shouldComplainAboutStrangeTypes()
    {
        ClientException e = assertThrows( ClientException.class, () -> value( new Object() ) );
        assertEquals( "Unable to convert java.lang.Object to Neo4j Value.", e.getMessage() );
    }

    @Test
    void equalityRules()
    {
        assertEquals( value( 1 ), value( 1 ) );
        assertEquals( value( Long.MAX_VALUE ), value( Long.MAX_VALUE ) );
        assertEquals( value( Long.MIN_VALUE ), value( Long.MIN_VALUE ) );
        assertNotEquals( value( 1 ), value( 2 ) );

        assertEquals( value( 1.1337 ), value( 1.1337 ) );
        assertEquals( value( Double.MAX_VALUE ), value( Double.MAX_VALUE ) );
        assertEquals( value( Double.MIN_VALUE ), value( Double.MIN_VALUE ) );

        assertEquals( value( true ), value( true ) );
        assertEquals( value( false ), value( false ) );
        assertNotEquals( value( true ), value( false ) );

        assertEquals( value( "Hello" ), value( "Hello" ) );
        assertEquals( value( "This åäö string ?? contains strange Ü" ),
                value( "This åäö string ?? contains strange Ü" ) );
        assertEquals( value( "" ), value( "" ) );
        assertNotEquals( value( "Hello" ), value( "hello" ) );
        assertNotEquals( value( "This åäö string ?? contains strange " ),
                value( "This åäö string ?? contains strange Ü" ) );

        assertEquals( value( 'A' ), value( 'A' ) );
        assertEquals( value( 'A' ), value( "A" ) );
    }

    @Test
    void shouldMapDriverComplexTypesToListOfJavaPrimitiveTypes()
    {
        // Given
        Map<String,Value> map = new HashMap<>();
        map.put( "Cat", new ListValue( values( "meow", "miaow" ) ) );
        map.put( "Dog", new ListValue( values( "wow" ) ) );
        map.put( "Wrong", new ListValue( values( -1 ) ) );
        MapValue mapValue = new MapValue( map );

        // When
        Iterable<List<String>> list = mapValue.values( ofList( ofToString() ) );

        // Then
        assertEquals( 3, mapValue.size() );
        Iterator<List<String>> listIterator = list.iterator();
        Set<String> setA = new HashSet<>( 3 );
        Set<String> setB = new HashSet<>( 3 );
        for ( Value value : mapValue.values() )
        {
            String a = value.get( 0 ).toString();
            String b = listIterator.next().get( 0 );
            setA.add( a );
            setB.add( b );
        }
        assertThat( setA, equalTo( setB ) );
    }

    @Test
    void shouldMapDriverMapsToJavaMaps()
    {
        // Given
        Map<String,Value> map = new HashMap<>();
        map.put( "Cat", value( 1 ) );
        map.put( "Dog", value( 2 ) );
        MapValue values = new MapValue( map );

        // When
        Map<String,String> result = values.asMap( Values.ofToString() );

        // Then
        assertThat( result.size(), equalTo( 2 ) );
        assertThat( result.get( "Dog" ), equalTo( "2" ) );
        assertThat( result.get( "Cat" ), equalTo( "1" ) );
    }

    @Test
    void shouldNotBeAbleToGetKeysFromNonKeyedValue()
    {
        assertThrows( ClientException.class, () -> value( "asd" ).get( 1 ) );
    }

    @Test
    void shouldNotBeAbleToDoCrazyCoercions()
    {
        assertThrows( ClientException.class, () -> value( 1 ).asPath() );
    }

    @Test
    void shouldNotBeAbleToGetSizeOnNonSizedValues()
    {
        assertThrows( ClientException.class, () -> value( 1 ).size() );
    }

    @Test
    void shouldMapInteger()
    {
        // Given
        Value val = value( 1, 2, 3 );

        // When/Then
        assertThat( val.asList( ofInteger() ), contains( 1, 2, 3 ) );
        assertThat( val.asList( ofLong() ), contains( 1L, 2L, 3L ) );
        assertThat( val.asList( ofNumber() ), contains( 1L, 2L, 3L ) );
        assertThat( val.asList( ofObject() ), contains( 1L, 2L, 3L ) );
    }

    @Test
    void shouldMapFloat()
    {
        // Given
        Value val = value( 1.0, 1.2, 3.2 );

        // When/Then
        assertThat( val.asList( ofDouble() ), contains( 1.0, 1.2, 3.2 ) );
        assertThat( val.asList( ofNumber() ), contains( 1.0, 1.2, 3.2 ) );
        assertThat( val.asList( ofObject() ), contains( 1.0, 1.2, 3.2 ) );
    }

    @Test
    void shouldMapFloatToJavaFloat()
    {
        // Given all double -> float conversions other than integers
        //       loose precision, as far as java is concerned, so we
        //       can only convert integer numbers to float.
        Value val = value( 1.0, 2.0, 3.0 );

        // When/Then
        assertThat( val.asList( ofFloat() ), contains( 1.0F, 2.0F, 3.0F ) );
    }

    @Test
    void shouldMapString()
    {
        // Given
        Value val = value( "hello", "world" );

        // When/Then
        assertThat( val.asList( ofString() ), contains( "hello", "world" ) );
        assertThat( val.asList( ofObject() ), contains( "hello", "world" ) );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    void shouldMapMapOfString()
    {
        // Given
        Map<String,Object> map = new HashMap<>();
        map.put( "hello", "world" );
        Value val = value( asList( map, map ) );

        // When/Then
        assertThat( val.asList( ofMap() ), contains( map, map ) );
        assertThat( val.asList( ofObject() ), contains( map, map ) );
    }

    @Test
    void shouldHandleCollection()
    {
        // Given
        Collection<String> collection = new ArrayDeque<>();
        collection.add( "hello" );
        collection.add( "world" );
        Value val = value( collection );

        // When/Then
        assertThat( val.asList(), Matchers.<Object>containsInAnyOrder( "hello", "world" ) );
    }

    @Test
    void shouldHandleIterator()
    {
        // Given
        Iterator<String> iterator = asList( "hello", "world" ).iterator();
        Value val = value( iterator );

        // When/Then
        assertThat( val.asList(), Matchers.<Object>containsInAnyOrder( "hello", "world" ) );
    }

    @Test
    void shouldCreateDateValueFromLocalDate()
    {
        LocalDate localDate = LocalDate.now();
        Value value = value( localDate );

        assertThat( value, instanceOf( DateValue.class ) );
        assertEquals( localDate, value.asLocalDate() );
    }

    @Test
    void shouldCreateDateValue()
    {
        Object localDate = LocalDate.now();
        Value value = value( localDate );

        assertThat( value, instanceOf( DateValue.class ) );
        assertEquals( localDate, value.asObject() );
    }

    @Test
    void shouldCreateTimeValueFromOffsetTime()
    {
        OffsetTime offsetTime = OffsetTime.now();
        Value value = value( offsetTime );

        assertThat( value, instanceOf( TimeValue.class ) );
        assertEquals( offsetTime, value.asOffsetTime() );
    }

    @Test
    void shouldCreateTimeValue()
    {
        OffsetTime offsetTime = OffsetTime.now();
        Value value = value( offsetTime );

        assertThat( value, instanceOf( TimeValue.class ) );
        assertEquals( offsetTime, value.asObject() );
    }

    @Test
    void shouldCreateLocalTimeValueFromLocalTime()
    {
        LocalTime localTime = LocalTime.now();
        Value value = value( localTime );

        assertThat( value, instanceOf( LocalTimeValue.class ) );
        assertEquals( localTime, value.asLocalTime() );
    }

    @Test
    void shouldCreateLocalTimeValue()
    {
        LocalTime localTime = LocalTime.now();
        Value value = value( localTime );

        assertThat( value, instanceOf( LocalTimeValue.class ) );
        assertEquals( localTime, value.asObject() );
    }

    @Test
    void shouldCreateLocalDateTimeValueFromLocalDateTime()
    {
        LocalDateTime localDateTime = LocalDateTime.now();
        Value value = value( localDateTime );

        assertThat( value, instanceOf( LocalDateTimeValue.class ) );
        assertEquals( localDateTime, value.asLocalDateTime() );
    }

    @Test
    void shouldCreateLocalDateTimeValue()
    {
        LocalDateTime localDateTime = LocalDateTime.now();
        Value value = value( localDateTime );

        assertThat( value, instanceOf( LocalDateTimeValue.class ) );
        assertEquals( localDateTime, value.asObject() );
    }

    @Test
    void shouldCreateDateTimeValueFromOffsetDateTime()
    {
        OffsetDateTime offsetDateTime = OffsetDateTime.now();
        Value value = value( offsetDateTime );

        assertThat( value, instanceOf( DateTimeValue.class ) );
        assertEquals( offsetDateTime, value.asOffsetDateTime() );
        assertEquals( offsetDateTime.toZonedDateTime(), value.asZonedDateTime() );
        assertEquals( offsetDateTime.toZonedDateTime(), value.asObject() );
    }

    @Test
    void shouldCreateDateTimeValueFromZonedDateTime()
    {
        ZonedDateTime zonedDateTime = ZonedDateTime.now();
        Value value = value( zonedDateTime );

        assertThat( value, instanceOf( DateTimeValue.class ) );
        assertEquals( zonedDateTime, value.asZonedDateTime() );
    }

    @Test
    void shouldCreateDateTimeValue()
    {
        ZonedDateTime zonedDateTime = ZonedDateTime.now();
        Value value = value( zonedDateTime );

        assertThat( value, instanceOf( DateTimeValue.class ) );
        assertEquals( zonedDateTime, value.asObject() );
    }

    @Test
    void shouldCreateIsoDurationValue()
    {
        Value value = isoDuration( 42_1, 42_2, 42_3, 42_4 );

        assertThat( value, instanceOf( DurationValue.class ) );
        IsoDuration duration = value.asIsoDuration();

        assertEquals( 42_1, duration.months() );
        assertEquals( 42_2, duration.days() );
        assertEquals( 42_3, duration.seconds() );
        assertEquals( 42_4, duration.nanoseconds() );
    }

    @Test
    void shouldCreateValueFromIsoDuration()
    {
        Value durationValue1 = isoDuration( 1, 2, 3, 4 );
        IsoDuration duration = durationValue1.asIsoDuration();
        Value durationValue2 = value( duration );

        assertEquals( duration, durationValue1.asIsoDuration() );
        assertEquals( duration, durationValue2.asIsoDuration() );
        assertEquals( durationValue1, durationValue2 );
    }

    @Test
    void shouldCreateValueFromPeriod()
    {
        Period period = Period.of( 5, 11, 190 );

        Value value = value( period );
        IsoDuration isoDuration = value.asIsoDuration();

        assertEquals( period.toTotalMonths(), isoDuration.months() );
        assertEquals( period.getDays(), isoDuration.days() );
        assertEquals( 0, isoDuration.seconds() );
        assertEquals( 0, isoDuration.nanoseconds() );
    }

    @Test
    void shouldCreateValueFromDuration()
    {
        Duration duration = Duration.ofSeconds( 183951, 4384718937L );

        Value value = value( duration );
        IsoDuration isoDuration = value.asIsoDuration();

        assertEquals( 0, isoDuration.months() );
        assertEquals( 0, isoDuration.days() );
        assertEquals( duration.getSeconds(), isoDuration.seconds() );
        assertEquals( duration.getNano(), isoDuration.nanoseconds() );
    }

    @Test
    void shouldCreateValueFromPoint2D()
    {
        Value point2DValue1 = point( 1, 2, 3 );
        Point point2D = point2DValue1.asPoint();
        Value point2DValue2 = value( point2D );

        assertEquals( point2D, point2DValue1.asPoint() );
        assertEquals( point2D, point2DValue2.asPoint() );
        assertEquals( point2DValue1, point2DValue2 );
    }

    @Test
    void shouldCreateValueFromPoint3D()
    {
        Value point3DValue1 = point( 1, 2, 3, 4 );
        Point point3D = point3DValue1.asPoint();
        Value point3DValue2 = value( point3D );

        assertEquals( point3D, point3DValue1.asPoint() );
        assertEquals( point3D, point3DValue2.asPoint() );
        assertEquals( point3DValue1, point3DValue2 );
    }

    @Test
    void shouldCreateValueFromNodeValue()
    {
        NodeValue node = emptyNodeValue();
        Value value = value( node );
        assertEquals( node, value );
    }

    @Test
    void shouldCreateValueFromNode()
    {
        Node node = emptyNodeValue().asNode();
        Value value = value( node );
        assertEquals( node, value.asNode() );
    }

    @Test
    void shouldCreateValueFromRelationshipValue()
    {
        RelationshipValue rel = emptyRelationshipValue();
        Value value = value( rel );
        assertEquals( rel, value );
    }

    @Test
    void shouldCreateValueFromRelationship()
    {
        Relationship rel = emptyRelationshipValue().asRelationship();
        Value value = value( rel );
        assertEquals( rel, value.asRelationship() );
    }

    @Test
    void shouldCreateValueFromPathValue()
    {
        PathValue path = filledPathValue();
        Value value = value( path );
        assertEquals( path, value );
    }

    @Test
    void shouldCreateValueFromPath()
    {
        Path path = filledPathValue().asPath();
        Value value = value( path );
        assertEquals( path, value.asPath() );
    }

    @Test
    void shouldCreateValueFromStream()
    {
        Stream<String> stream = Stream.of( "foo", "bar", "baz", "qux" );
        Value value = value( stream );
        assertEquals( asList( "foo", "bar", "baz", "qux" ), value.asObject() );
    }

    @Test
    void shouldFailToConvertStreamOfUnsupportedTypeToValue()
    {
        Stream<Object> stream = Stream.of( new Object(), new Object() );
        ClientException e = assertThrows( ClientException.class, () -> value( stream ) );
        assertEquals( "Unable to convert java.lang.Object to Neo4j Value.", e.getMessage() );
    }

    @Test
    void shouldCreateValueFromStreamOfStreams()
    {
        Stream<Stream<String>> stream = Stream.of( Stream.of( "foo", "bar" ), Stream.of( "baz", "qux" ) );
        Value value = value( stream );
        assertEquals( asList( asList( "foo", "bar" ), asList( "baz", "qux" ) ), value.asObject() );
    }

    @Test
    void shouldCreateValueFromStreamOfNulls()
    {
        Stream<Object> stream = Stream.of( null, null, null );
        Value value = value( stream );
        assertEquals( asList( null, null, null ), value.asObject() );
    }
}
