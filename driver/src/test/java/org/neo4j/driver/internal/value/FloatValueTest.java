/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.internal.types.TypeConstructor;
import org.neo4j.driver.v1.types.TypeSystem;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.value.LossyCoercion;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

public class FloatValueTest
{
    TypeSystem typeSystem = InternalTypeSystem.TYPE_SYSTEM;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void testZeroFloatValue() throws Exception
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
    public void testNonZeroFloatValue() throws Exception
    {
        // Given
        FloatValue value = new FloatValue( 6.28 );

        // Then
        assertThat( value.asDouble(), equalTo( 6.28 ) );
    }

    @Test
    public void testIsFloat() throws Exception
    {
        // Given
        FloatValue value = new FloatValue( 6.28 );

        // Then
        assertThat( typeSystem.FLOAT().isTypeOf( value ), equalTo( true ) );
    }

    @Test
    public void testEquals() throws Exception
    {
        // Given
        FloatValue firstValue = new FloatValue( 6.28 );
        FloatValue secondValue = new FloatValue( 6.28 );

        // Then
        assertThat( firstValue, equalTo( secondValue ) );
    }

    @Test
    public void testHashCode() throws Exception
    {
        // Given
        FloatValue value = new FloatValue( 6.28 );

        // Then
        assertThat( value.hashCode(), notNullValue() );
    }

    @Test
    public void shouldNotBeNull()
    {
        Value value = new FloatValue( 6.28 );
        assertFalse( value.isNull() );
    }

    @Test
    public void shouldTypeAsFloat()
    {
        InternalValue value = new FloatValue( 6.28 );
        assertThat( value.typeConstructor(), equalTo( TypeConstructor.FLOAT_TyCon ) );
    }

    @Test
    public void shouldThrowIfFloatContainsDecimalWhenConverting()
    {
        FloatValue value = new FloatValue( 1.1 );

        exception.expect( LossyCoercion.class );
        value.asInt();
    }

    @Test
    public void shouldThrowIfLargerThanIntegerMax()
    {
        FloatValue value1 = new FloatValue( Integer.MAX_VALUE );
        FloatValue value2 = new FloatValue( Integer.MAX_VALUE + 1L);

        assertThat(value1.asInt(), equalTo(Integer.MAX_VALUE));
        exception.expect( LossyCoercion.class );
        value2.asInt();
    }

    @Test
    public void shouldThrowIfSmallerThanIntegerMin()
    {
        FloatValue value1 = new FloatValue( Integer.MIN_VALUE );
        FloatValue value2 = new FloatValue( Integer.MIN_VALUE - 1L );

        assertThat(value1.asInt(), equalTo(Integer.MIN_VALUE));
        exception.expect( LossyCoercion.class );
        value2.asInt();
    }
}
