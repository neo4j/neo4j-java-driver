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

import org.neo4j.driver.v1.Identity;
import org.neo4j.driver.v1.TypeSystem;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.internal.Identities;
import org.neo4j.driver.v1.internal.types.StandardTypeSystem;
import org.neo4j.driver.v1.internal.types.TypeConstructor;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

public class IdentityValueTest
{
    TypeSystem typeSystem = StandardTypeSystem.TYPE_SYSTEM;

    @Test
    public void testValueAsIdentity() throws Exception
    {
        // Given
        Identity id = Identities.identity( 1 );
        IdentityValue value = new IdentityValue( id );

        // Then
        assertThat( value.asIdentity(), equalTo( id ) );
    }

    @Test
    public void testIsIdentity() throws Exception
    {
        // Given
        Identity id = Identities.identity( 1 );
        IdentityValue value = new IdentityValue( id );

        // Then
        assertThat( typeSystem.IDENTITY().isTypeOf( value ), equalTo( true ) );
    }

    @Test
    public void testEquals() throws Exception
    {
        // Given
        Identity id = Identities.identity( 1 );
        IdentityValue firstValue = new IdentityValue( id );
        IdentityValue secondValue = new IdentityValue( id );

        // Then
        assertThat( firstValue, equalTo( secondValue ) );
    }

    @Test
    public void testHashCode() throws Exception
    {
        // Given
        Identity id = Identities.identity( 1 );
        IdentityValue value = new IdentityValue( id );

        // Then
        assertThat( value.hashCode(), notNullValue() );
    }

    @Test
    public void shouldNotBeNull()
    {
        Value value = new IdentityValue( Identities.identity( 1 ) );
        assertFalse( value.isNull() );
    }

    @Test
    public void shouldTypeAsIdentity()
    {
        InternalValue value = new IdentityValue( Identities.identity( 1 ) );
        assertThat( value.typeConstructor(), equalTo( TypeConstructor.IDENTITY_TyCon ) );
    }
}
