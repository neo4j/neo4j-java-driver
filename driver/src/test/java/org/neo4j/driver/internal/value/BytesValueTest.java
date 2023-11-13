/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.internal.types.TypeConstructor;
import org.neo4j.driver.types.TypeSystem;

class BytesValueTest {
    private static final byte[] TEST_BYTES = "0123".getBytes();

    private final TypeSystem typeSystem = InternalTypeSystem.TYPE_SYSTEM;

    @Test
    void testBytesValue() {
        // Given
        var value = new BytesValue(TEST_BYTES);

        // Then
        assertThat(value.asObject(), equalTo(TEST_BYTES));
    }

    @Test
    void testIsBytes() {
        // Given
        var value = new BytesValue(TEST_BYTES);

        // Then
        assertThat(typeSystem.BYTES().isTypeOf(value), equalTo(true));
    }

    @Test
    void testEquals() {
        // Given
        var firstValue = new BytesValue(TEST_BYTES);
        var secondValue = new BytesValue(TEST_BYTES);

        // Then
        assertThat(firstValue, equalTo(secondValue));
    }

    @Test
    void testHashCode() {
        // Given
        var value = new BytesValue(TEST_BYTES);

        // Then
        assertThat(value.hashCode(), notNullValue());
    }

    @Test
    void shouldNotBeNull() {
        Value value = new BytesValue(TEST_BYTES);
        assertFalse(value.isNull());
    }

    @Test
    void shouldTypeAsString() {
        InternalValue value = new BytesValue(TEST_BYTES);
        assertThat(value.typeConstructor(), equalTo(TypeConstructor.BYTES));
    }

    @Test
    void shouldHaveBytesType() {
        InternalValue value = new BytesValue(TEST_BYTES);
        assertThat(value.type(), equalTo(InternalTypeSystem.TYPE_SYSTEM.BYTES()));
    }

    @Test
    void shouldMapToType() {
        var bytes = new byte[] {0};
        var values = Values.value(bytes);
        assertEquals(bytes, values.as(byte[].class));
        assertNotNull(values.as(Object.class));
    }
}
