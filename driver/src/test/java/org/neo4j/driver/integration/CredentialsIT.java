/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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

import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.driver.AuthTokens.basic;
import static org.neo4j.driver.AuthTokens.custom;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.exceptions.AuthenticationException;
import org.neo4j.driver.exceptions.SecurityException;
import org.neo4j.driver.testutil.DatabaseExtension;
import org.neo4j.driver.testutil.ParallelizableIT;

@ParallelizableIT
class CredentialsIT {
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    @Test
    void basicCredentialsShouldWork() {
        // When & Then
        try (var driver = GraphDatabase.driver(neo4j.uri(), basic("neo4j", neo4j.adminPassword()));
                var session = driver.session()) {
            var single = session.run("RETURN 1").single().get(0);
            assertThat(single.asLong(), equalTo(1L));
        }
    }

    @Test
    void shouldGetHelpfulErrorOnInvalidCredentials() {
        var e = assertThrows(SecurityException.class, () -> {
            try (var driver = GraphDatabase.driver(neo4j.uri(), basic("thisisnotthepassword", neo4j.adminPassword()));
                    var session = driver.session()) {
                session.run("RETURN 1");
            }
        });
        assertThat(e.getMessage(), containsString("The client is unauthorized due to authentication failure."));
    }

    @Test
    void shouldBeAbleToProvideRealmWithBasicAuth() {
        // When & Then
        try (var driver = GraphDatabase.driver(neo4j.uri(), basic("neo4j", neo4j.adminPassword(), "native"));
                var session = driver.session()) {
            var single = session.run("CREATE () RETURN 1").single().get(0);
            assertThat(single.asLong(), equalTo(1L));
        }
    }

    @Test
    void shouldBeAbleToConnectWithCustomToken() {
        // When & Then
        try (var driver = GraphDatabase.driver(neo4j.uri(), custom("neo4j", neo4j.adminPassword(), "native", "basic"));
                var session = driver.session()) {
            var single = session.run("CREATE () RETURN 1").single().get(0);
            assertThat(single.asLong(), equalTo(1L));
        }
    }

    @Test
    void shouldBeAbleToConnectWithCustomTokenWithAdditionalParameters() {
        Map<String, Object> params = singletonMap("secret", 16);

        // When & Then
        try (var driver = GraphDatabase.driver(
                        neo4j.uri(), custom("neo4j", neo4j.adminPassword(), "native", "basic", params));
                var session = driver.session()) {
            var single = session.run("CREATE () RETURN 1").single().get(0);
            assertThat(single.asLong(), equalTo(1L));
        }
    }

    @Test
    void directDriverShouldFailEarlyOnWrongCredentials() {
        testDriverFailureOnWrongCredentials(neo4j.uri().toString());
    }

    @Test
    void routingDriverShouldFailEarlyOnWrongCredentials() {
        testDriverFailureOnWrongCredentials(neo4j.uri().toString());
    }

    private void testDriverFailureOnWrongCredentials(String uri) {
        @SuppressWarnings("deprecation")
        var config = Config.builder().withLogging(DEV_NULL_LOGGING).build();
        var authToken = AuthTokens.basic("neo4j", "wrongSecret");

        @SuppressWarnings("resource")
        final var driver = GraphDatabase.driver(uri, authToken, config);
        assertThrows(AuthenticationException.class, driver::verifyConnectivity);
    }
}
