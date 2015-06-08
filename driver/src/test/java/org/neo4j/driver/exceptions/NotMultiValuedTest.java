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
import org.neo4j.driver.exceptions.value.NotMultiValued;

import static org.neo4j.driver.internal.Identities.identity;
import static org.neo4j.driver.Values.value;

public class NotMultiValuedTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void identityValueIsNotIndexedCollection() throws Throwable
    {
        // Given
        Value id = value( identity( "node/1" ) );

        // Expect
        exception.expect( NotMultiValued.class );
        exception.expectMessage( "identity is not an indexed collection" );

        // When
        id.get( 0 );
    }

    @Test
    public void identityValueIsNotKeyedCollection() throws Throwable
    {
        // Given
        Value id = value( identity( "node/1" ) );

        // Expect
        exception.expect( NotMultiValued.class );
        exception.expectMessage( "identity is not a keyed collection" );

        // When
        id.get( "" );
    }

    @Test
    public void identityValueIsNotIterable() throws Throwable
    {
        // Given
        Value id = value( identity( "node/1" ) );

        // Expect
        exception.expect( NotMultiValued.class );
        exception.expectMessage( "identity is not iterable" );

        // When
        id.iterator();
    }

}