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
package org.neo4j.driver.integration;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.driver.v1.Result;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.util.TestNeo4jSession;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.neo4j.driver.v1.Values.parameters;

public class ResultStreamIT
{
    @Rule
    public TestNeo4jSession session = new TestNeo4jSession();

    @Test
    public void shouldAllowIteratingOverResultStream() throws Throwable
    {
        // When
        Result res = session.run( "UNWIND [1,2,3,4] AS a RETURN a" );

        // Then I should be able to iterate over the result
        int idx = 1;
        while ( res.next() )
        {
            assertEquals( idx++, res.get( "a" ).javaLong() );
        }
    }

    @Test
    public void shouldHaveFieldNamesInResult()
    {
        // When
        Result res = session.run( "CREATE (n:TestNode {name:'test'}) RETURN n" );

        // Then
        assertEquals( "[n]", res.fieldNames().toString() );
        assertEquals( "[n]", res.single().fieldNames().toString() );
    }

    @Test
    public void shouldGiveHelpfulFailureMessageWhenCurrentRecordHasNotBeenSet() throws Throwable
    {
        // Given
        Result rs = session.run( "CREATE (n:Person {name:{name}}) RETURN n", parameters( "name", "Tom Hanks" ) );

        // When & Then
        try
        {
            rs.get( "n" );
            fail( "The test should fail with a proper message to indicate `next` method should be called first" );
        }
        catch( ClientException e )
        {
            assertEquals(
                    "In order to access fields of a record in a result, " +
                    "you must first call next() to point the result to the next record in the result stream.",
                    e.getMessage() );

        }
    }

    @Test
    public void shouldGiveHelpfulFailureMessageWhenAccessNonExistingField() throws Throwable
    {
        // Given
        Result rs = session.run( "CREATE (n:Person {name:{name}}) RETURN n", parameters( "name", "Tom Hanks" ) );

        // When
        Value m = rs.single().get( "m" );

        // Then
        assertNull( m );
    }

    @Test
    public void shouldGiveHelpfulFailureMessageWhenAccessNonExistingPropertyOnNode() throws Throwable
    {
        // Given
        Result rs = session.run( "CREATE (n:Person {name:{name}}) RETURN n", parameters( "name", "Tom Hanks" ) );

        // When
        Value n = rs.single().get( "n" );
        Value age = n.get( "age" );

        // Then
        assertNull( age );
    }
}
