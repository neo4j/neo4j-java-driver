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
package org.neo4j.driver.internal.value;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;

import org.neo4j.driver.internal.types.TypeConstructor;
import org.neo4j.driver.Value;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.Values.isoDuration;
import static org.neo4j.driver.Values.ofValue;
import static org.neo4j.driver.Values.point;

class NullValueTest
{
    @Test
    void shouldEqualItself()
    {
        assertThat( NullValue.NULL, equalTo( NullValue.NULL ) );
    }

    @Test
    void shouldBeNull()
    {
        assertTrue( NullValue.NULL.isNull() );
    }

    @Test
    void shouldTypeAsNull()
    {
        assertThat( ((InternalValue) NullValue.NULL).typeConstructor(), equalTo( TypeConstructor.NULL ) );
    }

    @Test
    void shouldReturnNativeTypesAsDefaultValue()
    {
        Value value = NullValue.NULL;
        // string
        assertThat( value.asString( "string value" ), equalTo( "string value" ) );

        // primitives
        assertThat( value.asBoolean( false ), equalTo( false ) );
        assertThat( value.asBoolean( true ), equalTo( true ) );
        assertThat( value.asInt( 10 ), equalTo( 10 ) );
        assertThat( value.asLong( 100L ), equalTo( 100L ) );
        assertThat( value.asFloat( 10.4f ), equalTo( 10.4f ) );
        assertThat( value.asDouble( 10.10 ), equalTo( 10.10 ) );

        //array, list, map
        assertThat( value.asByteArray( new byte[]{1, 2} ), equalTo( new byte[]{1, 2} ) );
        assertThat( value.asList( emptyList() ), equalTo( emptyList() ) );
        assertThat( value.asList( ofValue(), emptyList() ), equalTo( emptyList() ) );
        assertThat( value.asMap( emptyMap() ), equalTo( emptyMap() ) );
        assertThat( value.asMap( ofValue(), emptyMap() ), equalTo( emptyMap() ) );

        // spatial, temporal
        assertAsWithDefaultValueReturnDefault( value::asPoint, point( 1234, 1, 2 ).asPoint() );

        assertAsWithDefaultValueReturnDefault( value::asLocalDate, LocalDate.now() );
        assertAsWithDefaultValueReturnDefault( value::asOffsetTime, OffsetTime.now() );
        assertAsWithDefaultValueReturnDefault( value::asLocalTime, LocalTime.now() );
        assertAsWithDefaultValueReturnDefault( value::asLocalDateTime, LocalDateTime.now() );
        assertAsWithDefaultValueReturnDefault( value::asOffsetDateTime, OffsetDateTime.now() );
        assertAsWithDefaultValueReturnDefault( value::asZonedDateTime, ZonedDateTime.now() );
        assertAsWithDefaultValueReturnDefault( value::asIsoDuration,
                isoDuration( 1, 2, 3, 4 ).asIsoDuration() );
    }

    @Test
    void shouldReturnAsNull()
    {
        assertComputeOrDefaultReturnNull( Value::asObject );
        assertComputeOrDefaultReturnNull( Value::asNumber );

        assertComputeOrDefaultReturnNull( Value::asEntity );
        assertComputeOrDefaultReturnNull( Value::asNode );
        assertComputeOrDefaultReturnNull( Value::asRelationship );
        assertComputeOrDefaultReturnNull( Value::asPath );

        assertComputeOrDefaultReturnNull( Value::asString );
        assertComputeOrDefaultReturnNull( Value::asByteArray );
        assertComputeOrDefaultReturnNull( Value::asList );
        assertComputeOrDefaultReturnNull( v -> v.asList( ofValue() ) );
        assertComputeOrDefaultReturnNull( Value::asMap );
        assertComputeOrDefaultReturnNull( v -> v.asMap( ofValue() ) );

        assertComputeOrDefaultReturnNull( Value::asPoint );

        assertComputeOrDefaultReturnNull( Value::asLocalDate );
        assertComputeOrDefaultReturnNull( Value::asOffsetTime );
        assertComputeOrDefaultReturnNull( Value::asLocalTime );
        assertComputeOrDefaultReturnNull( Value::asLocalDateTime );
        assertComputeOrDefaultReturnNull( Value::asOffsetTime );
        assertComputeOrDefaultReturnNull( Value::asZonedDateTime );
        assertComputeOrDefaultReturnNull( Value::asIsoDuration );
    }

    @Test
    void shouldReturnAsDefaultValue()
    {
        assertComputeOrDefaultReturnDefault( Value::asObject, "null string" );
        assertComputeOrDefaultReturnDefault( Value::asNumber, 10 );
    }

    private static <T> void assertComputeOrDefaultReturnDefault( Function<Value,T> f, T defaultAndExpectedValue )
    {
        Value value = NullValue.NULL;
        assertThat( value.computeOrDefault( f, defaultAndExpectedValue ), equalTo( defaultAndExpectedValue ) );
    }

    private static <T> void assertComputeOrDefaultReturnNull( Function<Value,T> f )
    {
        assertComputeOrDefaultReturnDefault( f, null );
    }

    private static <T> void assertAsWithDefaultValueReturnDefault( Function<T,T> map, T defaultValue )
    {
        assertThat( map.apply( defaultValue ), equalTo( defaultValue ) );
    }
}
