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
import org.neo4j.driver.types.TypeSystem;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;

class BytesValueTest
{
    private static final byte[] TEST_BYTES = "0123".getBytes();

    private TypeSystem typeSystem = InternalTypeSystem.TYPE_SYSTEM;

    @Test
    void testBytesValue()
    {
        // Given
        BytesValue value = new BytesValue( TEST_BYTES );

        // Then
        assertThat( value.asObject(), equalTo( TEST_BYTES ) );
    }

    @Test
    void testIsBytes()
    {
        // Given
        BytesValue value = new BytesValue( TEST_BYTES );

        // Then
        assertThat( typeSystem.BYTES().isTypeOf( value ), equalTo( true ) );
    }

    @Test
    void testEquals()
    {
        // Given
        BytesValue firstValue = new BytesValue( TEST_BYTES );
        BytesValue secondValue = new BytesValue( TEST_BYTES );

        // Then
        assertThat( firstValue, equalTo( secondValue ) );
    }

    @Test
    void testHashCode()
    {
        // Given
        BytesValue value = new BytesValue( TEST_BYTES );

        // Then
        assertThat( value.hashCode(), notNullValue() );
    }

    @Test
    void shouldNotBeNull()
    {
        Value value = new BytesValue( TEST_BYTES );
        assertFalse( value.isNull() );
    }

    @Test
    void shouldTypeAsString()
    {
        InternalValue value = new BytesValue( TEST_BYTES );
        assertThat( value.typeConstructor(), equalTo( TypeConstructor.BYTES ) );
    }

    @Test
    void shouldHaveBytesType()
    {
        InternalValue value = new BytesValue( TEST_BYTES );
        assertThat( value.type(), equalTo( InternalTypeSystem.TYPE_SYSTEM.BYTES() ) );
    }
}
