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

import java.io.Serializable;
import java.lang.constant.Constable;
import java.lang.constant.ConstantDesc;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.internal.types.TypeConstructor;
import org.neo4j.driver.types.TypeSystem;

class StringValueTest {
    private final TypeSystem typeSystem = InternalTypeSystem.TYPE_SYSTEM;

    @Test
    void testStringValue() {
        // Given
        var value = new StringValue("Spongebob");

        // Then
        assertThat(value.asString(), equalTo("Spongebob"));
    }

    @Test
    void testIsString() {
        // Given
        var value = new StringValue("Spongebob");

        // Then
        assertThat(typeSystem.STRING().isTypeOf(value), equalTo(true));
    }

    @Test
    void testEquals() {
        // Given
        var firstValue = new StringValue("Spongebob");
        var secondValue = new StringValue("Spongebob");

        // Then
        assertThat(firstValue, equalTo(secondValue));
    }

    @Test
    void testHashCode() {
        // Given
        var value = new StringValue("Spongebob");

        // Then
        assertThat(value.hashCode(), notNullValue());
    }

    @Test
    void shouldNotBeNull() {
        Value value = new StringValue("Spongebob");
        assertFalse(value.isNull());
    }

    @Test
    void shouldTypeAsString() {
        InternalValue value = new StringValue("Spongebob");
        assertThat(value.typeConstructor(), equalTo(TypeConstructor.STRING));
    }

    @Test
    void shouldHaveStringType() {
        InternalValue value = new StringValue("Spongebob");
        assertThat(value.type(), equalTo(InternalTypeSystem.TYPE_SYSTEM.STRING()));
    }

    @Test
    void shouldMapToType() {
        var string = "0";
        var values = Values.value(string);
        assertEquals(string, values.as(String.class));
        assertEquals(string.charAt(0), values.as(char.class));
        assertEquals(string.charAt(0), values.as(Character.class));
        assertEquals(string, values.as(Serializable.class));
        assertEquals(string, values.as(Comparable.class));
        assertEquals(string, values.as(CharSequence.class));
        assertEquals(string, values.as(Constable.class));
        assertEquals(string, values.as(ConstantDesc.class));
        assertEquals(string, values.as(Object.class));
    }
}
