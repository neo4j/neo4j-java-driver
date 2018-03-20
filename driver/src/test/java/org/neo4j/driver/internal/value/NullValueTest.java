/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.driver.internal.value;

import org.junit.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.Collections;

import org.neo4j.driver.internal.types.TypeConstructor;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.util.Function;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertTrue;
import static org.neo4j.driver.v1.Values.point;

public class NullValueTest
{
    @Test
    public void shouldEqualItself()
    {
        assertThat( NullValue.NULL, equalTo( NullValue.NULL ) );
    }

    @Test
    public void shouldBeNull()
    {
        assertTrue( NullValue.NULL.isNull() );
    }

    @Test
    public void shouldTypeAsNull()
    {
        assertThat( ((InternalValue) NullValue.NULL).typeConstructor(), equalTo( TypeConstructor.NULL ) );
    }

    @Test
    public void shouldReturnNativeTypesAsDefaultValue() throws Throwable
    {
        Value value = NullValue.NULL;
        assertThat( value.asBoolean( false ), equalTo( false ) );
        assertThat( value.asBoolean( true ), equalTo( true ) );
        assertThat( value.asString( "string value" ), equalTo( "string value" ) );
        assertThat( value.asInt( 10 ), equalTo( 10 ) );
        assertThat( value.asLong( 100L ), equalTo( 100L ) );
        assertThat( value.asFloat( 10.4f ), equalTo( 10.4f ) );
        assertThat( value.asDouble( 10.10 ), equalTo( 10.10 ) );
    }

    @Test
    public void shouldReturnAsNull() throws Throwable
    {
        Value value = NullValue.NULL;

        assertThat( value.computeOrNull( Value::asObject ), nullValue() );
        assertThat( value.computeOrNull( Value::asNumber ), nullValue() );
        assertThat( value.computeOrNull( Value::asByteArray ), nullValue() );

        assertThat( value.computeOrNull( Value::asEntity ), nullValue() );
        assertThat( value.computeOrNull( Value::asNode ), nullValue() );
        assertThat( value.computeOrNull( Value::asRelationship ), nullValue() );
        assertThat( value.computeOrNull( Value::asPath ), nullValue() );

        assertThat( value.computeOrNull( Value::asPoint ), nullValue() );

        assertThat( value.computeOrNull( Value::asLocalDate ), nullValue() );
        assertThat( value.computeOrNull( Value::asOffsetTime ), nullValue() );
        assertThat( value.computeOrNull( Value::asLocalTime ), nullValue() );
        assertThat( value.computeOrNull( Value::asLocalDateTime ), nullValue() );
        assertThat( value.computeOrNull( Value::asZonedDateTime ), nullValue() );
        assertThat( value.computeOrNull( Value::asIsoDuration ), nullValue() );

        assertThat( value.computeOrNull( Value::asList ), nullValue() );
        assertThat( value.computeOrNull( Value::asMap ), nullValue() );
    }

    @Test
    public void shouldReturnAsDefaultValue() throws Throwable
    {
        Value value = NullValue.NULL;

        assertThat( value.computeOrDefault( Value::asObject, "null" ), equalTo( "null" ) );
        assertThat( value.computeOrDefault( Value::asNumber, 10 ), equalTo( 10 ) );
        assertThat( value.computeOrDefault( Value::asByteArray, new byte[0] ), equalTo( new byte[0] ) );

        assertThat( value.computeOrDefault( Value::asEntity, null ), nullValue() );
        assertThat( value.computeOrDefault( Value::asNode, null ), nullValue() );
        assertThat( value.computeOrDefault( Value::asRelationship, null ), nullValue() );
        assertThat( value.computeOrDefault( Value::asPath, null ), nullValue() );

        assertComputeOrDefaultReturnsDefaultValue( Value::asPoint, point( 1234, 1, 2 ) );

        assertComputeOrDefaultReturnsDefaultValue( Value::asLocalDate, LocalDate.now() );
        assertComputeOrDefaultReturnsDefaultValue( Value::asOffsetTime, OffsetTime.now() );
        assertComputeOrDefaultReturnsDefaultValue( Value::asLocalTime, LocalTime.now() );
        assertComputeOrDefaultReturnsDefaultValue( Value::asLocalDateTime, LocalDateTime.now() );
        assertComputeOrDefaultReturnsDefaultValue( Value::asZonedDateTime, ZonedDateTime.now() );
        assertComputeOrDefaultReturnsDefaultValue( Value::asIsoDuration, Values.isoDuration( 1, 2, 3, 4 ) );

        assertThat( value.computeOrDefault( Value::asList, Collections.emptyList() ), equalTo( Collections.emptyList() ) );
        assertThat( value.computeOrDefault( Value::asMap, Collections.emptyMap() ), equalTo( Collections.emptyMap() ) );
    }

    private <T> void assertComputeOrDefaultReturnsDefaultValue( Function<Value, T> f, T defaultAndExpectedValue )
    {
        Value value = NullValue.NULL;
        T returned = value.computeOrDefault( f, defaultAndExpectedValue );
        assertThat( returned, equalTo( defaultAndExpectedValue ) );
    }
}
