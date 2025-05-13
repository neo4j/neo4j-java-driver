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
package org.neo4j.driver;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.internal.security.StaticAuthTokenManager;
import org.neo4j.driver.testutil.TestUtil;

class GraphDatabaseTest {
    @SuppressWarnings("deprecation")
    private static final Config INSECURE_CONFIG =
            Config.builder().withoutEncryption().withLogging(Logging.none()).build();

    @Test
    @SuppressWarnings("resource")
    void throwsWhenBoltSchemeUsedWithRoutingParams() {
        assertThrows(
                IllegalArgumentException.class, () -> GraphDatabase.driver("bolt://localhost:7687/?policy=my_policy"));
    }

    @Test
    @SuppressWarnings("ResultOfMethodCallIgnored")
    void shouldRespondToInterruptsWhenConnectingToUnresponsiveServer() throws Exception {
        try (var serverSocket = new ServerSocket(0)) {
            // setup other thread to interrupt current thread when it blocks
            TestUtil.interruptWhenInWaitingState(Thread.currentThread());

            @SuppressWarnings("resource")
            final var driver = GraphDatabase.driver(
                    "bolt://localhost:" + serverSocket.getLocalPort(),
                    Config.builder().withConnectionTimeout(1, SECONDS).build());
            try {
                assertThrows(ServiceUnavailableException.class, driver::verifyConnectivity);
            } finally {
                // clear interrupted flag
                Thread.interrupted();
            }
        }
    }

    @Test
    void shouldPrintNiceErrorWhenConnectingToUnresponsiveServer() throws Exception {
        var localPort = -1;
        try (var serverSocket = new ServerSocket(0)) {
            localPort = serverSocket.getLocalPort();
        }
        @SuppressWarnings("resource")
        final var driver = GraphDatabase.driver("bolt://localhost:" + localPort, INSECURE_CONFIG);
        final var error = assertThrows(ServiceUnavailableException.class, driver::verifyConnectivity);
        assertThat(error.getMessage(), containsString("Unable to connect to"));
    }

    @Test
    void shouldPrintNiceRoutingErrorWhenConnectingToUnresponsiveServer() throws Exception {
        var localPort = -1;
        try (var serverSocket = new ServerSocket(0)) {
            localPort = serverSocket.getLocalPort();
        }
        @SuppressWarnings("resource")
        final var driver = GraphDatabase.driver("neo4j://localhost:" + localPort, INSECURE_CONFIG);
        final var error = assertThrows(ServiceUnavailableException.class, driver::verifyConnectivity);
        assertThat(error.getMessage(), containsString("Unable to connect to"));
    }

    @Test
    void shouldFailToCreateUnencryptedDriverWhenServerDoesNotRespond() throws IOException {
        testFailureWhenServerDoesNotRespond(false);
    }

    @Test
    @Disabled("TLS actually fails, the test setup is not valid")
    void shouldFailToCreateEncryptedDriverWhenServerDoesNotRespond() throws IOException {
        testFailureWhenServerDoesNotRespond(true);
    }

    @Test
    @SuppressWarnings("resource")
    void shouldAcceptNullTokenOnFactoryWithString() {
        GraphDatabase.driver("neo4j://host", (AuthToken) null);
    }

    @Test
    @SuppressWarnings("resource")
    void shouldAcceptNullTokenOnFactoryWithUri() {
        GraphDatabase.driver(URI.create("neo4j://host"), (AuthToken) null);
    }

    @Test
    @SuppressWarnings("resource")
    void shouldAcceptNullTokenOnFactoryWithStringAndConfig() {
        GraphDatabase.driver("neo4j://host", (AuthToken) null, Config.defaultConfig());
    }

    @Test
    @SuppressWarnings("resource")
    void shouldAcceptNullTokenOnFactoryWithUriAndConfig() {
        GraphDatabase.driver(URI.create("neo4j://host"), (AuthToken) null, Config.defaultConfig());
    }

    @Test
    @SuppressWarnings("resource")
    void shouldRejectNullAuthTokenManagerOnFactoryWithString() {
        assertThrows(NullPointerException.class, () -> GraphDatabase.driver("neo4j://host", (AuthTokenManager) null));
    }

    @Test
    @SuppressWarnings("resource")
    void shouldRejectNullAuthTokenManagerOnFactoryWithUri() {
        assertThrows(
                NullPointerException.class,
                () -> GraphDatabase.driver(URI.create("neo4j://host"), (AuthTokenManager) null));
    }

    @Test
    @SuppressWarnings("resource")
    void shouldRejectNullAuthTokenManagerOnFactoryWithStringAndConfig() {
        assertThrows(
                NullPointerException.class,
                () -> GraphDatabase.driver("neo4j://host", (AuthTokenManager) null, Config.defaultConfig()));
    }

    @Test
    @SuppressWarnings("resource")
    void shouldRejectNullAuthTokenManagerOnFactoryWithUriAndConfig() {
        assertThrows(
                NullPointerException.class,
                () -> GraphDatabase.driver(
                        URI.create("neo4j://host"), (AuthTokenManager) null, Config.defaultConfig()));
    }

    @Test
    void shouldCreateDriverWithManager() {
        assertNotNull(GraphDatabase.driver("neo4j://host", new StaticAuthTokenManager(AuthTokens.bearer("token"))));
    }

    @Test
    void shouldCreateDriverWithManagerAndConfig() {
        assertNotNull(GraphDatabase.driver(
                "neo4j://host", new StaticAuthTokenManager(AuthTokens.bearer("token")), Config.defaultConfig()));
    }

    private static void testFailureWhenServerDoesNotRespond(boolean encrypted) throws IOException {
        try (var server = new ServerSocket(0)) // server that accepts connections but does not reply
        {
            var connectionTimeoutMillis = 1_000;
            var config = createConfig(encrypted, connectionTimeoutMillis);
            @SuppressWarnings("resource")
            final var driver = GraphDatabase.driver(URI.create("bolt://localhost:" + server.getLocalPort()), config);

            var e = assertThrows(ServiceUnavailableException.class, driver::verifyConnectivity);
            assertEquals(e.getMessage(), "Unable to establish connection in " + connectionTimeoutMillis + "ms");
        }
    }

    private static Config createConfig(boolean encrypted, int timeoutMillis) {
        @SuppressWarnings("deprecation")
        var configBuilder = Config.builder()
                .withConnectionTimeout(timeoutMillis, MILLISECONDS)
                .withLogging(DEV_NULL_LOGGING);

        if (encrypted) {
            configBuilder.withEncryption();
        } else {
            configBuilder.withoutEncryption();
        }

        return configBuilder.build();
    }
}
