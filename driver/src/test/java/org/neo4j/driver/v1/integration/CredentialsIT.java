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
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import org.neo4j.driver.internal.auth.InternalAuthToken;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.util.Neo4jSettings;
import org.neo4j.driver.v1.util.TestNeo4j;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import static org.neo4j.driver.v1.AuthTokens.basic;
import static org.neo4j.driver.v1.Values.parameters;
import static org.neo4j.driver.v1.Values.valueAsIs;

public class CredentialsIT
{
    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    @Rule
    public TestNeo4j neo4j = new TestNeo4j();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void basicCredentialsShouldWork() throws Throwable
    {
        // Given
        String password = "secret";
        enableAuth( password );

        Driver driver = GraphDatabase.driver( neo4j.address(),
                basic("neo4j", password ) );

        // When
        Session session = driver.session();
        Value single = session.run( "RETURN 1" ).single().get(0);

        // Then
        assertThat( single.asLong(), equalTo( 1L ) );
    }

    @Test
    public void shouldGetHelpfulErrorOnInvalidCredentials() throws Throwable
    {
        // Given
        String password = "secret";
        enableAuth( password );

        Driver driver = GraphDatabase.driver( neo4j.address(), basic("thisisnotthepassword", password ) );
        Session session = driver.session();

        // Expect
        exception.expect( ClientException.class );
        exception.expectMessage( "The client is unauthorized due to authentication failure." );

        // When
        session.run( "RETURN 1" ).single().get(0);
    }

    private void enableAuth( String password ) throws Exception
    {
        neo4j.restartServerOnEmptyDatabase( Neo4jSettings.DEFAULT
                .updateWith( Neo4jSettings.AUTH_ENABLED, "true" )
                .updateWith( Neo4jSettings.AUTH_FILE, tempDir.newFile( "auth" ).getAbsolutePath() ));

        Driver setPassword = GraphDatabase.driver( neo4j.address(), new InternalAuthToken(
                parameters(
                        "scheme", "basic",
                        "principal", "neo4j",
                        "credentials", "neo4j",
                        "new_credentials", password ).asMap(valueAsIs()) ) );
        Session sess = setPassword.session();
        sess.run( "RETURN 1" ).discard();
        sess.close();
        setPassword.close();
    }
}
