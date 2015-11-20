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
package org.neo4j.driver.internal.value;

import org.junit.Test;

import org.neo4j.driver.v1.values.IntegerValue;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

public class IntegerValueTest
{

    @Test
    public void testZeroIntegerValue() throws Exception
    {
        // Given
        IntegerValue value = new IntegerValue( 0 );

        // Then
        assertThat( value.javaBoolean(), equalTo( false ) );
        assertThat( value.javaInteger(), equalTo( 0 ) );
        assertThat( value.javaLong(), equalTo( 0L ) );
        assertThat( value.javaFloat(), equalTo( (float) 0.0 ) );
        assertThat( value.javaDouble(), equalTo( 0.0 ) );
    }

    @Test
    public void testNonZeroIntegerValue() throws Exception
    {
        // Given
        IntegerValue value = new IntegerValue( 1 );

        // Then
        assertThat( value.javaBoolean(), equalTo( true ) );
        assertThat( value.javaInteger(), equalTo( 1 ) );
        assertThat( value.javaLong(), equalTo( 1L ) );
        assertThat( value.javaFloat(), equalTo( (float) 1.0 ) );
        assertThat( value.javaDouble(), equalTo( 1.0 ) );
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
}