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

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.net.URI;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.driver.Config;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.testutil.ParallelizableIT;
import org.neo4j.driver.testutil.SessionExtension;

@ParallelizableIT
class ErrorIT {
    @RegisterExtension
    static final SessionExtension session = new SessionExtension();

    @Test
    void shouldThrowHelpfulSyntaxError() {
        var e = assertThrows(ClientException.class, () -> {
            var result = session.run("invalid query");
            result.consume();
        });

        assertThat(e.getMessage(), startsWith("Invalid input"));
    }

    @Test
    void shouldNotAllowMoreTxAfterClientException() {
        // Given
        var tx = session.beginTransaction();

        // And Given an error has occurred
        try {
            tx.run("invalid").consume();
        } catch (ClientException e) {
            /*empty*/
        }

        // Expect
        var e = assertThrows(ClientException.class, () -> {
            var cursor = tx.run("RETURN 1");
            cursor.single().get("1").asInt();
        });
        assertThat(e.getMessage(), startsWith("Cannot run more queries in this transaction"));
    }

    @Test
    void shouldAllowNewQueryAfterRecoverableError() {
        // Given an error has occurred
        try {
            session.run("invalid").consume();
        } catch (ClientException e) {
            /*empty*/
        }

        // When
        var cursor = session.run("RETURN 1");
        var val = cursor.single().get("1").asInt();

        // Then
        assertThat(val, equalTo(1));
    }

    @Test
    void shouldAllowNewTransactionAfterRecoverableError() {
        // Given an error has occurred in a prior transaction
        try (var tx = session.beginTransaction()) {
            tx.run("invalid").consume();
        } catch (ClientException e) {
            /*empty*/
        }

        // When
        try (var tx = session.beginTransaction()) {
            var cursor = tx.run("RETURN 1");
            var val = cursor.single().get("1").asInt();

            // Then
            assertThat(val, equalTo(1));
        }
    }

    @Test
    void shouldExplainConnectionError() {
        @SuppressWarnings("resource")
        final var driver = GraphDatabase.driver("bolt://localhost:7777");
        var e = assertThrows(ServiceUnavailableException.class, driver::verifyConnectivity);

        assertEquals(
                "Unable to connect to localhost:7777, ensure the database is running "
                        + "and that there is a working network connection to it.",
                e.getMessage());
    }

    @Test
    void shouldHandleFailureAtRunTime() {
        var label = UUID.randomUUID().toString(); // avoid clashes with other tests
        String query;

        if (session.isNeo4j43OrEarlier()) {
            query = "CREATE CONSTRAINT ON (a:`" + label + "`) ASSERT a.name IS UNIQUE";
        } else {
            query = "CREATE CONSTRAINT FOR (a:`" + label + "`) REQUIRE a.name IS UNIQUE";
        }

        // given
        var tx = session.beginTransaction();
        tx.run(query);
        tx.commit();

        // and
        var anotherTx = session.beginTransaction();

        // then expect
        var e = assertThrows(ClientException.class, () -> anotherTx.run(query));
        anotherTx.rollback();
        assertThat(e.getMessage(), containsString(label));
        assertThat(e.getMessage(), containsString("name"));
    }

    @Test
    void shouldGetHelpfulErrorWhenTryingToConnectToHttpPort() {
        var config = Config.builder().withoutEncryption().build();

        var boltUri = session.uri();
        var uri = URI.create(String.format("%s://%s:%d", boltUri.getScheme(), boltUri.getHost(), session.httpPort()));
        @SuppressWarnings("resource")
        final var driver = GraphDatabase.driver(uri, config);
        var e = assertThrows(ClientException.class, driver::verifyConnectivity);
        assertEquals(
                "Server responded HTTP. Make sure you are not trying to connect to the http endpoint "
                        + "(HTTP defaults to port 7474 whereas BOLT defaults to port 7687)",
                e.getMessage());
    }

    @Test
    void shouldThrowErrorWithNiceStackTrace(TestInfo testInfo) {
        var error = assertThrows(
                ClientException.class, () -> session.run("RETURN 10 / 0").consume());

        // thrown error should have this class & method in the stacktrace
        var stackTrace = error.getStackTrace();
        assertTrue(
                Stream.of(stackTrace).anyMatch(element -> testClassAndMethodMatch(testInfo, element)),
                () -> "Expected stacktrace element is absent:\n" + Arrays.toString(stackTrace));

        // thrown error should have a suppressed error with an async stacktrace
        assertThat(asList(error.getSuppressed()), hasSize(greaterThanOrEqualTo(1)));
    }

    private static boolean testClassAndMethodMatch(TestInfo testInfo, StackTraceElement element) {
        return testClassMatches(testInfo, element) && testMethodMatches(testInfo, element);
    }

    private static boolean testClassMatches(TestInfo testInfo, StackTraceElement element) {
        var expectedName = testInfo.getTestClass().map(Class::getName).orElse("");
        var actualName = element.getClassName();
        return Objects.equals(expectedName, actualName);
    }

    private static boolean testMethodMatches(TestInfo testInfo, StackTraceElement element) {
        var expectedName = testInfo.getTestMethod().map(Method::getName).orElse("");
        var actualName = element.getMethodName();
        return Objects.equals(expectedName, actualName);
    }
}
