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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.internal.value.BooleanValue.FALSE;
import static org.neo4j.driver.internal.value.BooleanValue.TRUE;

import java.io.Serializable;
import java.lang.constant.Constable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.internal.types.TypeConstructor;
import org.neo4j.driver.types.TypeSystem;

class BooleanValueTest {
    private final TypeSystem typeSystem = InternalTypeSystem.TYPE_SYSTEM;

    @Test
    void testBooleanTrue() {
        // Given
        var value = TRUE;

        // Then
        assertThat(value.asBoolean(), equalTo(true));
        assertThat(value.isTrue(), equalTo(true));
        assertThat(value.isFalse(), equalTo(false));
    }

    @Test
    void testBooleanFalse() {
        // Given
        var value = FALSE;

        // Then
        assertThat(value.asBoolean(), equalTo(false));
        assertThat(value.isTrue(), equalTo(false));
        assertThat(value.isFalse(), equalTo(true));
    }

    @Test
    void testIsBoolean() {
        assertThat(typeSystem.BOOLEAN().isTypeOf(TRUE), equalTo(true));
    }

    @Test
    void testEquals() {
        assertThat(TRUE, equalTo(TRUE));
    }

    @Test
    void testHashCode() {
        assertThat(TRUE.hashCode(), notNullValue());
    }

    @Test
    void shouldNotBeNull() {
        assertFalse(TRUE.isNull());
        assertFalse(BooleanValue.FALSE.isNull());
    }

    @Test
    void shouldTypeAsBoolean() {
        assertThat(TRUE.typeConstructor(), equalTo(TypeConstructor.BOOLEAN));
        assertThat(BooleanValue.FALSE.typeConstructor(), equalTo(TypeConstructor.BOOLEAN));
    }

    @Test
    void shouldConvertToBooleanAndObject() {
        assertTrue(TRUE.asBoolean());
        assertFalse(BooleanValue.FALSE.asBoolean());
        assertThat(TRUE.asObject(), equalTo((Object) Boolean.TRUE));
        assertThat(FALSE.asObject(), equalTo((Object) Boolean.FALSE));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldMapToType(boolean expected) {
        var value = Values.value(expected);
        assertEquals(expected, value.as(boolean.class));
        assertEquals(expected, value.as(Boolean.class));
        assertEquals(expected, value.as(Serializable.class));
        assertEquals(expected, value.as(Constable.class));
        assertEquals(expected, value.as(Object.class));
    }
}
