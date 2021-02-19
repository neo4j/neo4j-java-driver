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

class FloatValueTest
{
    private TypeSystem typeSystem = InternalTypeSystem.TYPE_SYSTEM;

    @Test
    void testZeroFloatValue()
    {
        // Given
        FloatValue value = new FloatValue( 0 );

        // Then
        assertThat( value.asInt(), equalTo( 0 ) );
        assertThat( value.asLong(), equalTo( 0L ) );
        assertThat( value.asFloat(), equalTo( (float) 0.0 ) );
        assertThat( value.asDouble(), equalTo( 0.0 ) );
    }

    @Test
    void testNonZeroFloatValue()
    {
        // Given
        FloatValue value = new FloatValue( 6.28 );

        // Then
        assertThat( value.asDouble(), equalTo( 6.28 ) );
    }

    @Test
    void testIsFloat()
    {
        // Given
        FloatValue value = new FloatValue( 6.28 );

        // Then
        assertThat( typeSystem.FLOAT().isTypeOf( value ), equalTo( true ) );
    }

    @Test
    void testEquals()
    {
        // Given
        FloatValue firstValue = new FloatValue( 6.28 );
        FloatValue secondValue = new FloatValue( 6.28 );

        // Then
        assertThat( firstValue, equalTo( secondValue ) );
    }

    @Test
    void testHashCode()
    {
        // Given
        FloatValue value = new FloatValue( 6.28 );

        // Then
        assertThat( value.hashCode(), notNullValue() );
    }

    @Test
    void shouldNotBeNull()
    {
        Value value = new FloatValue( 6.28 );
        assertFalse( value.isNull() );
    }

    @Test
    void shouldTypeAsFloat()
    {
        InternalValue value = new FloatValue( 6.28 );
        assertThat( value.typeConstructor(), equalTo( TypeConstructor.FLOAT ) );
    }

    @Test
    void shouldThrowIfFloatContainsDecimalWhenConverting()
    {
        FloatValue value = new FloatValue( 1.1 );

        assertThrows( LossyCoercion.class, value::asInt );
    }

    @Test
    void shouldThrowIfLargerThanIntegerMax()
    {
        FloatValue value1 = new FloatValue( Integer.MAX_VALUE );
        FloatValue value2 = new FloatValue( Integer.MAX_VALUE + 1L);

        assertThat(value1.asInt(), equalTo(Integer.MAX_VALUE));
        assertThrows( LossyCoercion.class, value2::asInt );
    }

    @Test
    void shouldThrowIfSmallerThanIntegerMin()
    {
        FloatValue value1 = new FloatValue( Integer.MIN_VALUE );
        FloatValue value2 = new FloatValue( Integer.MIN_VALUE - 1L );

        assertThat(value1.asInt(), equalTo(Integer.MIN_VALUE));
        assertThrows( LossyCoercion.class, value2::asInt );
    }
}
