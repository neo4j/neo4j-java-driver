/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
