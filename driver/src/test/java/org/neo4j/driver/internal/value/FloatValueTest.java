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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.Serializable;
import java.lang.constant.Constable;
import java.lang.constant.ConstantDesc;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.exceptions.value.LossyCoercion;
import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.internal.types.TypeConstructor;
import org.neo4j.driver.types.TypeSystem;

class FloatValueTest {
    private final TypeSystem typeSystem = InternalTypeSystem.TYPE_SYSTEM;

    @Test
    void testZeroFloatValue() {
        // Given
        var value = new FloatValue(0);

        // Then
        assertThat(value.asInt(), equalTo(0));
        assertThat(value.asLong(), equalTo(0L));
        assertThat(value.asFloat(), equalTo((float) 0.0));
        assertThat(value.asDouble(), equalTo(0.0));
    }

    @Test
    void testNonZeroFloatValue() {
        // Given
        var value = new FloatValue(6.28);

        // Then
        assertThat(value.asDouble(), equalTo(6.28));
    }

    @Test
    void testIsFloat() {
        // Given
        var value = new FloatValue(6.28);

        // Then
        assertThat(typeSystem.FLOAT().isTypeOf(value), equalTo(true));
    }

    @Test
    void testEquals() {
        // Given
        var firstValue = new FloatValue(6.28);
        var secondValue = new FloatValue(6.28);

        // Then
        assertThat(firstValue, equalTo(secondValue));
    }

    @Test
    void testHashCode() {
        // Given
        var value = new FloatValue(6.28);

        // Then
        assertThat(value.hashCode(), notNullValue());
    }

    @Test
    void shouldNotBeNull() {
        Value value = new FloatValue(6.28);
        assertFalse(value.isNull());
    }

    @Test
    void shouldTypeAsFloat() {
        InternalValue value = new FloatValue(6.28);
        assertThat(value.typeConstructor(), equalTo(TypeConstructor.FLOAT));
    }

    @Test
    void shouldThrowIfFloatContainsDecimalWhenConverting() {
        var value = new FloatValue(1.1);

        assertThrows(LossyCoercion.class, value::asInt);
    }

    @Test
    void shouldThrowIfLargerThanIntegerMax() {
        var value1 = new FloatValue(Integer.MAX_VALUE);
        var value2 = new FloatValue(Integer.MAX_VALUE + 1L);

        assertThat(value1.asInt(), equalTo(Integer.MAX_VALUE));
        assertThrows(LossyCoercion.class, value2::asInt);
    }

    @Test
    void shouldThrowIfSmallerThanIntegerMin() {
        var value1 = new FloatValue(Integer.MIN_VALUE);
        var value2 = new FloatValue(Integer.MIN_VALUE - 1L);

        assertThat(value1.asInt(), equalTo(Integer.MIN_VALUE));
        assertThrows(LossyCoercion.class, value2::asInt);
    }

    @SuppressWarnings("ConstantValue")
    @Test
    void shouldMapToType() {
        var expected = 0.0;
        var value = Values.value(expected);
        assertEquals(expected, value.as(double.class));
        assertEquals(expected, value.as(Double.class));
        assertEquals(expected, value.as(Number.class));
        assertEquals(expected, value.as(Serializable.class));
        assertEquals(expected, value.as(Comparable.class));
        assertEquals(expected, value.as(Constable.class));
        assertEquals(expected, value.as(ConstantDesc.class));
        assertEquals(expected, value.as(Object.class));
        assertEquals((long) expected, value.as(long.class));
        assertEquals((long) expected, value.as(Long.class));
        assertEquals((int) expected, value.as(int.class));
        assertEquals((int) expected, value.as(Integer.class));
        assertEquals((float) expected, value.as(float.class));
        assertEquals((float) expected, value.as(Float.class));
    }
}
