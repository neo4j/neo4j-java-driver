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

import org.neo4j.Neo4j;
import org.neo4j.driver.Session;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.util.TestNeo4j;

import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

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
        assumeTrue( neo4j.canControlServer() );

        // And given we've spun up a few running sessions
        Session s1 = Neo4j.session( neo4j.address() );
        Session s2 = Neo4j.session( neo4j.address() );
        Session s3 = Neo4j.session( neo4j.address() );
        Session s4 = Neo4j.session( neo4j.address() );

        // And given they are all returned to the connection pool
        s1.close();
        s2.close();
        s3.close();
        s4.close();

        // When
        neo4j.restartDatabase();

        // Then we should be able to start using sessions again, at most O(numSessions) session calls later
        // TODO: These should get evicted immediately, not show up as application-level errors first
        int toleratedFailures = 4;
        for ( int i = 0; i < 10; i++ )
        {
            try(Session s = Neo4j.session( neo4j.address() ))
            {
                s.run( "RETURN 'Hello, world!'" );
            }
            catch(ClientException e)
            {
                if(toleratedFailures-- == 0)
                {
                    fail("Expected (for now) at most four failures, one for each old connection, but now I've gotten " +
                         "five: " + e.getMessage());
                }
            }
        }
    }
}
