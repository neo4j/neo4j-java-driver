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

import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.internal.types.TypeConstructor;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.value.LossyCoercion;
import org.neo4j.driver.types.TypeSystem;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class IntegerValueTest
{
    private TypeSystem typeSystem = InternalTypeSystem.TYPE_SYSTEM;

    @Test
    void testZeroIntegerValue()
    {
        // Given
        IntegerValue value = new IntegerValue( 0 );

        // Then
        assertThat( value.asLong(), equalTo( 0L ) );
        assertThat( value.asInt(), equalTo( 0 ) );
        assertThat( value.asDouble(), equalTo( 0.0 ) );
        assertThat( value.asFloat(), equalTo( (float) 0.0 ) );
        assertThat( value.asNumber(), equalTo( (Number) 0L ) );
    }

    @Test
    void testNonZeroIntegerValue()
    {
        // Given
        IntegerValue value = new IntegerValue( 1 );

        // Then
        assertThat( value.asLong(), equalTo( 1L ) );
        assertThat( value.asInt(), equalTo( 1 ) );
        assertThat( value.asDouble(), equalTo( 1.0 ) );
        assertThat( value.asFloat(), equalTo( (float) 1.0 ) );
        assertThat( value.asNumber(), equalTo( (Number) 1L ) );
    }

    @Test
    void testIsInteger()
    {
        // Given
        IntegerValue value = new IntegerValue( 1L );

        // Then
        assertThat( typeSystem.INTEGER().isTypeOf( value ), equalTo( true ) );
    }

    @Test
    void testEquals()
    {
        // Given
        IntegerValue firstValue = new IntegerValue( 1 );
        IntegerValue secondValue = new IntegerValue( 1 );

        // Then
        assertThat( firstValue, equalTo( secondValue ) );
    }

    @Test
    void testHashCode()
    {
        // Given
        IntegerValue value = new IntegerValue( 1L );

        // Then
        assertThat( value.hashCode(), notNullValue() );
    }

    @Test
    void shouldNotBeNull()
    {
        Value value = new IntegerValue( 1L );
        assertFalse( value.isNull() );
    }

    @Test
    void shouldTypeAsInteger()
    {
        InternalValue value = new IntegerValue( 1L );
        assertThat( value.typeConstructor(), equalTo( TypeConstructor.INTEGER ) );
    }

    @Test
    void shouldThrowIfLargerThanIntegerMax()
    {
        IntegerValue value1 = new IntegerValue( Integer.MAX_VALUE );
        IntegerValue value2 = new IntegerValue( Integer.MAX_VALUE + 1L);

        assertThat(value1.asInt(), equalTo(Integer.MAX_VALUE));
        assertThrows( LossyCoercion.class, value2::asInt );
    }

    @Test
    void shouldThrowIfSmallerThanIntegerMin()
    {
        IntegerValue value1 = new IntegerValue( Integer.MIN_VALUE );
        IntegerValue value2 = new IntegerValue( Integer.MIN_VALUE - 1L );

        assertThat(value1.asInt(), equalTo(Integer.MIN_VALUE));
        assertThrows( LossyCoercion.class, value2::asInt );
    }

    @Test
    void shouldThrowIfLargerThan()
    {
        IntegerValue value1 = new IntegerValue( 9007199254740992L);
        IntegerValue value2 = new IntegerValue(9007199254740993L );

        assertThat(value1.asDouble(), equalTo(9007199254740992D));
        assertThrows( LossyCoercion.class, value2::asDouble );
    }
}
