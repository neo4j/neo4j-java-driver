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
package org.neo4j.driver.exceptions;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.value.Uncoercible;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import static org.neo4j.driver.internal.Identities.identity;
import static org.neo4j.driver.Values.value;

public class UncoercibleTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldNotBeAbleToCoerceIdentityToInteger() throws Throwable
    {
        // Given
        Value id = value( identity( "node/1" ) );

        // Expect
        exception.expect( Uncoercible.class );
        exception.expectMessage( "Cannot coerce identity to Java int" );

        // Then
        assertThat( id.isIdentity(), equalTo( true ) );
        id.javaInteger();

    }

    @Test
    public void shouldNotBeAbleToCoerceIdentityToLong() throws Throwable
    {
        // Given
        Value id = value( identity( "node/1" ) );

        // Expect
        exception.expect( Uncoercible.class );
        exception.expectMessage( "Cannot coerce identity to Java long" );

        // Then
        assertThat( id.isIdentity(), equalTo( true ) );
        id.javaLong();

    }

    @Test
    public void shouldNotBeAbleToCoerceIdentityToFloat() throws Throwable
    {
        // Given
        Value id = value( identity( "node/1" ) );

        // Expect
        exception.expect( Uncoercible.class );
        exception.expectMessage( "Cannot coerce identity to Java float" );

        // Then
        assertThat( id.isIdentity(), equalTo( true ) );
        id.javaFloat();

    }

    @Test
    public void shouldNotBeAbleToCoerceIdentityToDouble() throws Throwable
    {
        // Given
        Value id = value( identity( "node/1" ) );

        // Expect
        exception.expect( Uncoercible.class );
        exception.expectMessage( "Cannot coerce identity to Java double" );

        // Then
        assertThat( id.isIdentity(), equalTo( true ) );
        id.javaDouble();

    }

    @Test
    public void shouldNotBeAbleToCoerceIdentityToNode() throws Throwable
    {
        // Given
        Value id = value( identity( "node/1" ) );

        // Expect
        exception.expect( Uncoercible.class );
        exception.expectMessage( "Cannot coerce identity to Node" );

        // Then
        assertThat( id.isIdentity(), equalTo( true ) );
        id.asNode();

    }

    @Test
    public void shouldNotBeAbleToCoerceIdentityToRelationship() throws Throwable
    {
        // Given
        Value id = value( identity( "node/1" ) );

        // Expect
        exception.expect( Uncoercible.class );
        exception.expectMessage( "Cannot coerce identity to Relationship" );

        // Then
        assertThat( id.isIdentity(), equalTo( true ) );
        id.asRelationship();

    }

    @Test
    public void shouldNotBeAbleToCoerceIdentityToPath() throws Throwable
    {
        // Given
        Value id = value( identity( "node/1" ) );

        // Expect
        exception.expect( Uncoercible.class );
        exception.expectMessage( "Cannot coerce identity to Path" );

        // Then
        assertThat( id.isIdentity(), equalTo( true ) );
        id.asPath();

    }

}