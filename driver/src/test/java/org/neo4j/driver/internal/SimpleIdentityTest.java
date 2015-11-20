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
package org.neo4j.driver.internal;

import org.junit.Test;

import org.neo4j.driver.v1.Identity;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

public class SimpleIdentityTest
{

    @Test
    public void shouldBeAbleToCompareIdentities() throws Throwable
    {
        // Given
        Identity firstIdentity = new SimpleIdentity( 1 );
        Identity secondIdentity = new SimpleIdentity( 1 );

        // Then
        assertThat( firstIdentity, equalTo( secondIdentity ) );

    }

    @Test
    public void hashCodeShouldNotBeNull() throws Throwable
    {
        // Given
        Identity identity = new SimpleIdentity( 1 );

        // Then
        assertThat( identity.hashCode(), notNullValue() );

    }

    @Test
    public void shouldBeAbleToCastIdentityToString() throws Throwable
    {
        // Given
        Identity identity = new SimpleIdentity( 1 );

        // Then
        assertThat( identity.toString(), equalTo( "#1" ) );

    }

}