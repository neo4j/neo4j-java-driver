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

import org.neo4j.driver.Result;
import org.neo4j.driver.util.TestSession;

import static org.junit.Assert.assertEquals;

public class ResultStreamIT
{
    @Rule
    public TestSession session = new TestSession();

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
}
