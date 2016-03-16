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

import org.junit.Test;

import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.internal.types.TypeConstructor;
import org.neo4j.driver.v1.types.TypeSystem;
import org.neo4j.driver.v1.Value;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

public class StringValueTest
{
    TypeSystem typeSystem = InternalTypeSystem.TYPE_SYSTEM;

    @Test
    public void testStringValue() throws Exception
    {
        // Given
        StringValue value = new StringValue( "Spongebob" );

        // Then
        assertThat( value.asString(), equalTo( "Spongebob" ) );
    }

    @Test
    public void testIsString() throws Exception
    {
        // Given
        StringValue value = new StringValue( "Spongebob" );

        // Then
        assertThat( typeSystem.STRING().isTypeOf( value ), equalTo( true ) );
    }

    @Test
    public void testEquals() throws Exception
    {
        // Given
        StringValue firstValue = new StringValue( "Spongebob" );
        StringValue secondValue = new StringValue( "Spongebob" );

        // Then
        assertThat( firstValue, equalTo( secondValue ) );
    }

    @Test
    public void testHashCode() throws Exception
    {
        // Given
        StringValue value = new StringValue( "Spongebob" );

        // Then
        assertThat( value.hashCode(), notNullValue() );
    }

    @Test
    public void shouldNotBeNull()
    {
        Value value = new StringValue( "Spongebob" );
        assertFalse( value.isNull() );
    }

    @Test
    public void shouldTypeAsString()
    {
        InternalValue value = new StringValue( "Spongebob" );
        assertThat( value.typeConstructor(), equalTo( TypeConstructor.STRING_TyCon ) );
    }

    @Test
    public void shouldHaveStringType()
    {
        InternalValue value = new StringValue( "Spongebob" );
        assertThat( value.type(), equalTo( InternalTypeSystem.TYPE_SYSTEM.STRING() ) );
    }
}
