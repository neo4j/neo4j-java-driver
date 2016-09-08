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

import java.util.HashMap;

import org.neo4j.driver.internal.security.InternalAuthToken;
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
import static org.neo4j.driver.v1.AuthTokens.custom;
import static org.neo4j.driver.v1.Values.ofValue;
import static org.neo4j.driver.v1.Values.parameters;

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

        // When & Then
        try( Driver driver = GraphDatabase.driver( neo4j.uri(),
                basic("neo4j", password ) );
             Session session = driver.session() )
        {
            Value single = session.run( "RETURN 1" ).single().get( 0 );
            assertThat( single.asLong(), equalTo( 1L ) );
        }
    }

    @SuppressWarnings( "EmptyTryBlock" )
    @Test
    public void shouldGetHelpfulErrorOnInvalidCredentials() throws Throwable
    {
        // Given
        String password = "secret";
        enableAuth( password );

        // Expect
        exception.expect( ClientException.class );
        exception.expectMessage( "The client is unauthorized due to authentication failure." );

        // When
        try( Driver driver = GraphDatabase.driver( neo4j.uri(), basic("thisisnotthepassword", password ) );
             Session ignored = driver.session() ) {
            //empty
        }
    }

    @Test
    public void shouldBeAbleToProvideRealmWithBasicAuth() throws Throwable
    {
        // Given
        String password = "secret";
        enableAuth( password );

        // When & Then
        try( Driver driver = GraphDatabase.driver( neo4j.uri(),
                basic("neo4j", password, "native") );
             Session session = driver.session() )
        {
            Value single = session.run( "CREATE () RETURN 1" ).single().get( 0 );
            assertThat( single.asLong(), equalTo( 1L ) );
        }
    }

    @Test
    public void shouldBeAbleToConnectWithCustomToken() throws Throwable
    {
        // Given
        String password = "secret";
        enableAuth( password );

        // When & Then
        try( Driver driver = GraphDatabase.driver( neo4j.uri(),
                custom("neo4j", password, "native", "basic" ) );
             Session session = driver.session() )
        {
            Value single = session.run( "CREATE () RETURN 1" ).single().get( 0 );
            assertThat( single.asLong(), equalTo( 1L ) );
        }
    }

    @Test
    public void shouldBeAbleToConnectWithCustomTokenWithAdditionalParameters() throws Throwable
    {
        // Given
        String password = "secret";
        enableAuth( password );
        HashMap<String,Object> parameters = new HashMap<>();
        parameters.put( "secret", 16 );

        // When & Then
        try( Driver driver = GraphDatabase.driver( neo4j.uri(),
                custom("neo4j", password, "native", "basic", parameters ) );
             Session session = driver.session() )
        {
            Value single = session.run( "CREATE () RETURN 1" ).single().get( 0 );
            assertThat( single.asLong(), equalTo( 1L ) );
        }
    }

    private void enableAuth( String password ) throws Exception
    {
        neo4j.restart( Neo4jSettings.TEST_SETTINGS
                .updateWith( Neo4jSettings.AUTH_ENABLED, "true" )
                .updateWith( Neo4jSettings.DATA_DIR, tempDir.getRoot().getAbsolutePath().replace("\\", "/") ));

        try ( Driver setPassword =
                      GraphDatabase.driver( neo4j.uri(), new InternalAuthToken(
                                      parameters(
                        "scheme", "basic",
                        "principal", "neo4j",
                        "credentials", "neo4j",
                        "new_credentials", password ).asMap( ofValue()) ) );
              Session sess = setPassword.session() )
        {
            sess.run( "RETURN 1" ).consume();
        }
    }
}
