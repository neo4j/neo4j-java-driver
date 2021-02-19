/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import org.neo4j.driver.internal.security.InternalAuthToken;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.AuthenticationException;
import org.neo4j.driver.exceptions.SecurityException;
import org.neo4j.driver.internal.util.DisabledOnNeo4jWith;
import org.neo4j.driver.internal.util.Neo4jFeature;
import org.neo4j.driver.util.DatabaseExtension;
import org.neo4j.driver.util.Neo4jSettings;
import org.neo4j.driver.util.ParallelizableIT;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.AuthTokens.basic;
import static org.neo4j.driver.AuthTokens.custom;
import static org.neo4j.driver.Values.ofValue;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.util.Neo4jRunner.PASSWORD;

@ParallelizableIT
class CredentialsIT
{
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    @Test
    @DisabledOnNeo4jWith( Neo4jFeature.BOLT_V4 )
    // This feature is removed in 4.0
    void shouldBePossibleToChangePassword() throws Exception
    {
        String newPassword = "secret";
        String tmpDataDir = Files.createTempDirectory( Paths.get( "target" ), "tmp" ).toAbsolutePath().toString().replace( "\\", "/" );

        neo4j.restartDb( Neo4jSettings.TEST_SETTINGS
                .updateWith( Neo4jSettings.DATA_DIR, tmpDataDir ) );

        AuthToken authToken = new InternalAuthToken( parameters(
                "scheme", "basic",
                "principal", "neo4j",
                "credentials", "neo4j",
                "new_credentials", newPassword ).asMap( ofValue() ) );

        // change the password
        try ( Driver driver = GraphDatabase.driver( neo4j.uri(), authToken );
              Session session = driver.session() )
        {
            session.run( "RETURN 1" ).consume();
        }

        // verify old password does not work
        final Driver badDriver = GraphDatabase.driver( CredentialsIT.neo4j.uri(), basic( "neo4j", PASSWORD ) );
        assertThrows( AuthenticationException.class, badDriver::verifyConnectivity );

        // verify new password works
        try ( Driver driver = GraphDatabase.driver( CredentialsIT.neo4j.uri(), AuthTokens.basic( "neo4j", newPassword ) );
              Session session = driver.session() )
        {
            session.run( "RETURN 2" ).consume();
        }
    }

    @Test
    void basicCredentialsShouldWork()
    {
        // When & Then
        try( Driver driver = GraphDatabase.driver( neo4j.uri(),
                basic( "neo4j", PASSWORD ) );
             Session session = driver.session() )
        {
            Value single = session.run( "RETURN 1" ).single().get( 0 );
            assertThat( single.asLong(), equalTo( 1L ) );
        }
    }

    @Test
    void shouldGetHelpfulErrorOnInvalidCredentials()
    {
        SecurityException e = assertThrows( SecurityException.class, () ->
        {
            try ( Driver driver = GraphDatabase.driver( neo4j.uri(), basic( "thisisnotthepassword", PASSWORD ) );
                  Session session = driver.session() )
            {
                session.run( "RETURN 1" );
            }
        } );
        assertThat( e.getMessage(), containsString( "The client is unauthorized due to authentication failure." ) );
    }

    @Test
    void shouldBeAbleToProvideRealmWithBasicAuth()
    {
        // When & Then
        try( Driver driver = GraphDatabase.driver( neo4j.uri(),
                basic( "neo4j", PASSWORD, "native" ) );
             Session session = driver.session() )
        {
            Value single = session.run( "CREATE () RETURN 1" ).single().get( 0 );
            assertThat( single.asLong(), equalTo( 1L ) );
        }
    }

    @Test
    void shouldBeAbleToConnectWithCustomToken()
    {
        // When & Then
        try( Driver driver = GraphDatabase.driver( neo4j.uri(),
                custom( "neo4j", PASSWORD, "native", "basic" ) );
             Session session = driver.session() )
        {
            Value single = session.run( "CREATE () RETURN 1" ).single().get( 0 );
            assertThat( single.asLong(), equalTo( 1L ) );
        }
    }

    @Test
    void shouldBeAbleToConnectWithCustomTokenWithAdditionalParameters()
    {
        Map<String,Object> params = singletonMap( "secret", 16 );

        // When & Then
        try( Driver driver = GraphDatabase.driver( neo4j.uri(),
                custom( "neo4j", PASSWORD, "native", "basic", params ) );
             Session session = driver.session() )
        {
            Value single = session.run( "CREATE () RETURN 1" ).single().get( 0 );
            assertThat( single.asLong(), equalTo( 1L ) );
        }
    }

    @Test
    void directDriverShouldFailEarlyOnWrongCredentials()
    {
        testDriverFailureOnWrongCredentials( "bolt://localhost:" + neo4j.boltPort() );
    }

    @Test
    void routingDriverShouldFailEarlyOnWrongCredentials()
    {
        testDriverFailureOnWrongCredentials( "neo4j://localhost:" + neo4j.boltPort() );
    }

    private void testDriverFailureOnWrongCredentials( String uri )
    {
        Config config = Config.builder().withLogging( DEV_NULL_LOGGING ).build();
        AuthToken authToken = AuthTokens.basic( "neo4j", "wrongSecret" );

        final Driver driver = GraphDatabase.driver( uri, authToken, config );
        assertThrows( AuthenticationException.class, driver::verifyConnectivity );
    }
}
