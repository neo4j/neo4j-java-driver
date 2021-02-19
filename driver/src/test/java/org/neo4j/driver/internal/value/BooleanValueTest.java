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
import org.neo4j.driver.types.TypeSystem;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.internal.value.BooleanValue.FALSE;
import static org.neo4j.driver.internal.value.BooleanValue.TRUE;

class BooleanValueTest
{
    private TypeSystem typeSystem = InternalTypeSystem.TYPE_SYSTEM;

    @Test
    void testBooleanTrue()
    {
        // Given
        BooleanValue value = TRUE;

        // Then
        assertThat( value.asBoolean(), equalTo( true ) );
        assertThat( value.isTrue(), equalTo( true ) );
        assertThat( value.isFalse(), equalTo( false ) );
    }

    @Test
    void testBooleanFalse()
    {
        // Given
        BooleanValue value = FALSE;

        // Then
        assertThat( value.asBoolean(), equalTo( false ) );
        assertThat( value.isTrue(), equalTo( false ) );
        assertThat( value.isFalse(), equalTo( true ) );
    }

    @Test
    void testIsBoolean()
    {
        // Given
        BooleanValue value = TRUE;

        // Then
        assertThat( typeSystem.BOOLEAN().isTypeOf( value ), equalTo( true ) );
    }

    @Test
    void testEquals()
    {
        // Given
        BooleanValue firstValue = TRUE;
        BooleanValue secondValue = TRUE;

        // Then
        assertThat( firstValue, equalTo( secondValue ) );
    }

    @Test
    void testHashCode()
    {
        // Given
        BooleanValue value = TRUE;

        // Then
        assertThat( value.hashCode(), notNullValue() );
    }

    @Test
    void shouldNotBeNull()
    {
        assertFalse( TRUE.isNull() );
        assertFalse( BooleanValue.FALSE.isNull() );
    }

    @Test
    void shouldTypeAsBoolean()
    {
        assertThat( TRUE.typeConstructor(), equalTo( TypeConstructor.BOOLEAN ) );
        assertThat( BooleanValue.FALSE.typeConstructor(), equalTo( TypeConstructor.BOOLEAN ) );
    }

    @Test
    void shouldConvertToBooleanAndObject()
    {
        assertTrue( TRUE.asBoolean());
        assertFalse( BooleanValue.FALSE.asBoolean());
        assertThat( TRUE.asObject(), equalTo( (Object) Boolean.TRUE ));
        assertThat( FALSE.asObject(), equalTo( (Object) Boolean.FALSE ));
    }
}
