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
package org.neo4j.driver.v1;

import javadoctest.DocSnippet;
import javadoctest.DocTestRunner;
import org.junit.Rule;
import org.junit.runner.RunWith;

import org.neo4j.driver.v1.util.TestNeo4jSession;

import static org.junit.Assert.assertEquals;

@RunWith( DocTestRunner.class )
public class TransactionDocIT
{
    @Rule
    public TestNeo4jSession session = new TestNeo4jSession();

    /** @see Transaction */
    public void classDoc( DocSnippet snippet )
    {
        // Given
        session.run( "MATCH (n) DETACH DELETE n" );
        snippet.set( "session", session );

        // When I run the snippet
        snippet.run();

        // Then a node should've been created
        StatementResult cursor = session.run( "MATCH (n) RETURN count(n)" );
        assertEquals( 1,  cursor.single().get( "count(n)" ).asInt() );
    }

    /** @see Transaction#failure()  */
    public void failure( DocSnippet snippet )
    {
        // Given
        snippet.set( "session", session );

        // When I run the snippet
        snippet.run();

        // Then a node should've been created
        StatementResult cursor = session.run( "MATCH (n) RETURN count(n)" );
        assertEquals( 0, cursor.single().get( "count(n)" ).asInt() );
    }
}
