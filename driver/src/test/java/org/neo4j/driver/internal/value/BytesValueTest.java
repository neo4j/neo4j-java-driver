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

import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.internal.types.TypeConstructor;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.types.TypeSystem;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

public class BytesValueTest
{
    public static final byte[] TEST_BYTES = "0123".getBytes();

    TypeSystem typeSystem = InternalTypeSystem.TYPE_SYSTEM;

    @Test
    public void testBytesValue() throws Exception
    {
        // Given
        BytesValue value = new BytesValue( TEST_BYTES );

        // Then
        assertThat( value.asObject(), equalTo( TEST_BYTES ) );
    }

    @Test
    public void testIsBytes() throws Exception
    {
        // Given
        BytesValue value = new BytesValue( TEST_BYTES );

        // Then
        assertThat( typeSystem.BYTES().isTypeOf( value ), equalTo( true ) );
    }

    @Test
    public void testEquals() throws Exception
    {
        // Given
        BytesValue firstValue = new BytesValue( TEST_BYTES );
        BytesValue secondValue = new BytesValue( TEST_BYTES );

        // Then
        assertThat( firstValue, equalTo( secondValue ) );
    }

    @Test
    public void testHashCode() throws Exception
    {
        // Given
        BytesValue value = new BytesValue( TEST_BYTES );

        // Then
        assertThat( value.hashCode(), notNullValue() );
    }

    @Test
    public void shouldNotBeNull()
    {
        Value value = new BytesValue( TEST_BYTES );
        assertFalse( value.isNull() );
    }

    @Test
    public void shouldTypeAsString()
    {
        InternalValue value = new BytesValue( TEST_BYTES );
        assertThat( value.typeConstructor(), equalTo( TypeConstructor.BYTES ) );
    }

    @Test
    public void shouldHaveBytesType()
    {
        InternalValue value = new BytesValue( TEST_BYTES );
        assertThat( value.type(), equalTo( InternalTypeSystem.TYPE_SYSTEM.BYTES() ) );
    }
}
