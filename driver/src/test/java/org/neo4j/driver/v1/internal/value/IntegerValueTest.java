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
package org.neo4j.driver.v1.internal.value;

import org.junit.Test;

import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.internal.types.TypeConstructor;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

public class IntegerValueTest
{

    @Test
    public void testZeroIntegerValue() throws Exception
    {
        // Given
        IntegerValue value = new IntegerValue( 0 );

        // Then
        assertThat( value.asBoolean(), equalTo( false ) );
        assertThat( value.asInteger(), equalTo( 0 ) );
        assertThat( value.asLong(), equalTo( 0L ) );
        assertThat( value.asFloat(), equalTo( (float) 0.0 ) );
        assertThat( value.asDouble(), equalTo( 0.0 ) );
    }

    @Test
    public void testNonZeroIntegerValue() throws Exception
    {
        // Given
        IntegerValue value = new IntegerValue( 1 );

        // Then
        assertThat( value.asBoolean(), equalTo( true ) );
        assertThat( value.asInteger(), equalTo( 1 ) );
        assertThat( value.asLong(), equalTo( 1L ) );
        assertThat( value.asFloat(), equalTo( (float) 1.0 ) );
        assertThat( value.asDouble(), equalTo( 1.0 ) );
    }

    @Test
    public void testIsInteger() throws Exception
    {
        // Given
        IntegerValue value = new IntegerValue( 1L );

        // Then
        assertThat( value.isInteger(), equalTo( true ) );
    }

    @Test
    public void testEquals() throws Exception
    {
        // Given
        IntegerValue firstValue = new IntegerValue( 1 );
        IntegerValue secondValue = new IntegerValue( 1 );

        // Then
        assertThat( firstValue, equalTo( secondValue ) );
    }

    @Test
    public void testHashCode() throws Exception
    {
        // Given
        IntegerValue value = new IntegerValue( 1L );

        // Then
        assertThat( value.hashCode(), notNullValue() );
    }

    @Test
    public void shouldNotBeNull()
    {
        Value value = new IntegerValue( 1L );
        assertFalse( value.isNull() );
    }

    @Test
    public void shouldTypeAsInteger()
    {
        InternalValue value = new IntegerValue( 1L );
        assertThat( value.typeConstructor(), equalTo( TypeConstructor.INTEGER_TyCon ) );
    }
}
