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
package org.neo4j.driver.v1.integration;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.util.TestNeo4jSession;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.neo4j.driver.v1.Values.parameters;

public class ResultStreamIT
{
    @Rule
    public TestNeo4jSession session = new TestNeo4jSession();

    @Test
    public void shouldAllowIteratingOverResultStream() throws Throwable
    {
        // When
        StatementResult res = session.run( "UNWIND [1,2,3,4] AS a RETURN a" );

        // Then I should be able to iterate over the result
        int idx = 1;
        while ( res.hasNext() )
        {
            assertEquals( idx++, res.next().get( "a" ).asLong() );
        }
    }

    @Test
    public void shouldHaveFieldNamesInResult()
    {
        // When
        StatementResult res = session.run( "CREATE (n:TestNode {name:'test'}) RETURN n" );

        // Then
        assertEquals( "[n]", res.keys().toString() );
        assertNotNull( res.single() );
        assertEquals( "[n]", res.keys().toString() );
    }

    @Test
    public void shouldGiveHelpfulFailureMessageWhenAccessNonExistingField() throws Throwable
    {
        // Given
        StatementResult rs =
                session.run( "CREATE (n:Person {name:{name}}) RETURN n", parameters( "name", "Tom Hanks" ) );

        // When
        Record single = rs.single();

        // Then
        assertTrue( single.get( "m" ).isNull() );
    }

    @Test
    public void shouldGiveHelpfulFailureMessageWhenAccessNonExistingPropertyOnNode() throws Throwable
    {
        // Given
        StatementResult rs =
                session.run( "CREATE (n:Person {name:{name}}) RETURN n", parameters( "name", "Tom Hanks" ) );

        // When
        Record record = rs.single();

        // Then
        assertTrue( record.get( "n" ).get( "age" ).isNull() );
    }

    @Test
    public void shouldNotReturnNullKeysOnEmptyResult()
    {
        // Given
        StatementResult rs = session.run( "CREATE (n:Person {name:{name}})", parameters( "name", "Tom Hanks" ) );

        // THEN
        assertNotNull( rs.keys() );
    }

    @Test
    public void shouldBeAbleToReuseSessionAfterFailure() throws Throwable
    {
        // Given
        StatementResult res1 = session.run( "INVALID" );
        try
        {
            res1.consume();
        }
        catch ( Exception e )
        {
            //ignore
        }

        // When
        StatementResult res2 = session.run( "RETURN 1" );

        // Then
        assertTrue( res2.hasNext() );
        assertEquals( res2.next().get("1").asLong(), 1L );
    }
}
