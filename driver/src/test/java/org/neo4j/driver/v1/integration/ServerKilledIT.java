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

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.util.Neo4jRunner;
import org.neo4j.driver.v1.util.TestNeo4j;

import static org.junit.Assert.fail;

/**
 * Mainly concerned about the connection pool - we want to make sure that bad connections are evacuated from the
 * pool properly if the server dies, or all connections are lost for some other reason.
 */
public class ServerKilledIT
{
    @Rule
    public TestNeo4j neo4j = new TestNeo4j();

    @Test
    public void shouldRecoverFromServerRestart() throws Throwable
    {
        // Given
        try ( Driver driver = GraphDatabase.driver( Neo4jRunner.DEFAULT_URI ) )
        {
            Session s1 = driver.session();
            Session s2 = driver.session();
            Session s3 = driver.session();
            Session s4 = driver.session();

            // And given they are all returned to the connection pool
            s1.close();
            s2.close();
            s3.close();
            s4.close();

            // When
            neo4j.forceRestart();

            // Then we should be able to start using sessions again, at most O(numSessions) session calls later
            // TODO: These should value evicted immediately, not show up as application-loggingLevel errors first
            int toleratedFailures = 4;
            for ( int i = 0; i < 10; i++ )
            {
                try ( Session s = driver.session() )
                {
                    s.run( "RETURN 'Hello, world!'" );
                }
                catch ( ClientException e )
                {
                    if ( toleratedFailures-- == 0 )
                    {
                        fail( "Expected (for now) at most four failures, one for each old connection, but now I've " +
                              "gotten " +
                              "five: " + e.getMessage() );
                    }
                }
            }

            if (toleratedFailures > 0)
            {
                fail("This query should have failed " + toleratedFailures + " times");
            }
        }
    }
}
