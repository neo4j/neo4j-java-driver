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
package org.neo4j.docs.driver;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.util.EnabledOnNeo4jWith;
import org.neo4j.driver.summary.QueryType;
import org.neo4j.driver.testutil.DatabaseExtension;
import org.neo4j.driver.testutil.ParallelizableIT;
import org.neo4j.driver.testutil.StdIOCapture;
import org.neo4j.driver.testutil.TestUtil;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.internal.util.Neo4jEdition.ENTERPRISE;
import static org.neo4j.driver.internal.util.Neo4jFeature.BOLT_V4;
import static org.neo4j.driver.testutil.TestUtil.await;
import static org.neo4j.driver.testutil.TestUtil.createDatabase;
import static org.neo4j.driver.testutil.TestUtil.dropDatabase;

@ParallelizableIT
class ExamplesIT {
    static final String USER = "neo4j";

    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    private String uri;

    @SuppressWarnings("resource")
    private int readInt(String database, final String query, final Value parameters) {
        SessionConfig sessionConfig;
        if (database == null) {
            sessionConfig = SessionConfig.defaultConfig();
        } else {
            sessionConfig = SessionConfig.forDatabase(database);
        }
        try (var session = neo4j.driver().session(sessionConfig)) {
            return session.executeRead(
                    tx -> tx.run(query, parameters).single().get(0).asInt());
        }
    }

    private int readInt(final String query, final Value parameters) {
        return readInt(null, query, parameters);
    }

    private int readInt(final String query) {
        return readInt(query, parameters());
    }

    @SuppressWarnings("resource")
    private void write(final String query, final Value parameters) {
        try (var session = neo4j.driver().session()) {
            session.executeWriteWithoutResult(tx -> tx.run(query, parameters).consume());
        }
    }

    private void write(String query) {
        write(query, parameters());
    }

    private int personCount(String name) {
        return readInt("MATCH (a:Person {name: $name}) RETURN count(a)", parameters("name", name));
    }

    private int companyCount(String name) {
        return readInt("MATCH (a:Company {name: $name}) RETURN count(a)", parameters("name", name));
    }

    @BeforeEach
    void setUp() {
        uri = neo4j.uri().toString();
        TestUtil.cleanDb(neo4j.driver());
    }

    @Test
    void testShouldRunAutocommitTransactionExample() {
        // Given
        try (var example =
                new AutocommitTransactionExample(uri, USER, neo4j.adminPassword())) {
            // When
            example.addPerson("Alice");

            // Then
            assertTrue(personCount("Alice") > 0);
        }
    }

    @Test
    @SuppressWarnings("resource")
    void testShouldRunAsyncAutocommitTransactionExample() {
        try (var example =
                new AsyncAutocommitTransactionExample(uri, USER, neo4j.adminPassword())) {
            // create some 'Product' nodes
            try (var session = neo4j.driver().session()) {
                session.run("UNWIND ['Tesseract', 'Orb', 'Eye of Agamotto'] AS item "
                        + "CREATE (:Product {id: 0, title: item})");
            }

            // read all 'Product' nodes
            var titles = await(example.readProductTitles());
            assertEquals(new HashSet<>(asList("Tesseract", "Orb", "Eye of Agamotto")), new HashSet<>(titles));
        }
    }

    @Test
    void testShouldAsyncRunResultConsumeExample() {
        // Given
        write("CREATE (a:Person {name: 'Alice'})");
        write("CREATE (a:Person {name: 'Bob'})");
        try (var example = new AsyncResultConsumeExample(uri, USER, neo4j.adminPassword())) {
            // When
            var names = await(example.getPeople());

            // Then
            assertEquals(asList("Alice", "Bob"), names);
        }
    }

    @Test
    void testShouldAsyncRunMultipleTransactionExample() {
        // Given
        write("CREATE (a:Person {name: 'Alice'})");
        write("CREATE (a:Person {name: 'Bob'})");
        try (var example =
                new AsyncRunMultipleTransactionExample(uri, USER, neo4j.adminPassword())) {
            // When
            var nodesCreated = await(example.addEmployees("Acme"));

            // Then
            var employeeCount =
                    readInt("MATCH (emp:Person)-[WORKS_FOR]->(com:Company) WHERE com.name = 'Acme' RETURN count(emp)");
            assertEquals(2, employeeCount);
            assertEquals(1, nodesCreated);
        }
    }

    @Test
    void testShouldRunConfigConnectionPoolExample() {
        // Given
        try (var example = new ConfigConnectionPoolExample(uri, USER, neo4j.adminPassword())) {
            // Then
            assertTrue(example.canConnect());
        }
    }

    @Test
    void testShouldRunBasicAuthExample() {
        // Given
        try (var example = new BasicAuthExample(uri, USER, neo4j.adminPassword())) {
            // Then
            assertTrue(example.canConnect());
        }
    }

    @Test
    void testShouldRunCustomAuthExample() {
        // Given
        try (var example = new CustomAuthExample(uri, USER, neo4j.adminPassword(), null, "basic", Map.of())) {
            // Then
            assertTrue(example.canConnect());
        }
    }

    @Test
    void testShouldRunConfigConnectionTimeoutExample() {
        // Given
        try (var example =
                new ConfigConnectionTimeoutExample(uri, USER, neo4j.adminPassword())) {
            // Then
            assertNotNull(example);
        }
    }

    @Test
    void testShouldRunConfigMaxRetryTimeExample() {
        // Given
        try (var example = new ConfigMaxRetryTimeExample(uri, USER, neo4j.adminPassword())) {
            // Then
            assertNotNull(example);
        }
    }

    @Test
    void testShouldRunConfigTrustExample() {
        // Given
        try (var example = new ConfigTrustExample(uri, USER, neo4j.adminPassword())) {
            // Then
            assertNotNull(example);
        }
    }

    @Test
    void testShouldRunConfigUnencryptedExample() {
        // Given
        try (var example = new ConfigUnencryptedExample(uri, USER, neo4j.adminPassword())) {
            // Then
            assertNotNull(example);
        }
    }

    @Test
    void testShouldRunDriverLifecycleExample() {
        // Given
        try (var example = new DriverLifecycleExample(uri, USER, neo4j.adminPassword())) {
            // Then
            assertNotNull(example);
        }
    }

    @Test
    void testShouldRunHelloWorld() {
        // Given
        try (var greeter = new HelloWorldExample(uri, USER, neo4j.adminPassword())) {
            // When
            var stdIO = StdIOCapture.capture();

            try (stdIO) {
                greeter.printGreeting("hello, world");
            }

            // Then
            assertEquals(1, stdIO.stdout().size());
            assertTrue(stdIO.stdout().get(0).contains("hello, world"));
        }
    }

    @Test
    void testShouldRunReadWriteTransactionExample() {
        // Given
        try (var example = new ReadWriteTransactionExample(uri, USER, neo4j.adminPassword())) {
            // When
            var nodeID = example.addPerson("Alice");

            // Then
            assertTrue(nodeID >= 0L);
        }
    }

    @Test
    void testShouldRunResultConsumeExample() {
        // Given
        write("CREATE (a:Person {name: 'Alice'})");
        write("CREATE (a:Person {name: 'Bob'})");
        try (var example = new ResultConsumeExample(uri, USER, neo4j.adminPassword())) {
            // When
            var names = example.getPeople();

            // Then
            assertEquals(List.of("Alice", "Bob"), names);
        }
    }

    @Test
    void testShouldRunResultRetainExample() {
        // Given
        write("CREATE (a:Person {name: 'Alice'})");
        write("CREATE (a:Person {name: 'Bob'})");
        try (var example = new ResultRetainExample(uri, USER, neo4j.adminPassword())) {
            // When
            example.addEmployees("Acme");

            // Then
            var employeeCount =
                    readInt("MATCH (emp:Person)-[WORKS_FOR]->(com:Company) WHERE com.name = 'Acme' RETURN count(emp)");
            assertEquals(2, employeeCount);
        }
    }

    @Test
    void testShouldRunTransactionFunctionExample() {
        // Given
        try (var example = new TransactionFunctionExample(uri, USER, neo4j.adminPassword())) {
            // When
            example.addPerson("Alice");

            // Then
            assertTrue(personCount("Alice") > 0);
        }
    }

    @Test
    void testShouldConfigureTransactionTimeoutExample() {
        // Given
        try (var example =
                new TransactionTimeoutConfigExample(uri, USER, neo4j.adminPassword())) {
            // When
            example.addPerson("Alice");

            // Then
            assertTrue(personCount("Alice") > 0);
        }
    }

    @Test
    void testShouldConfigureTransactionMetadataExample() {
        // Given
        try (var example =
                new TransactionMetadataConfigExample(uri, USER, neo4j.adminPassword())) {
            // When
            example.addPerson("Alice");

            // Then
            assertTrue(personCount("Alice") > 0);
        }
    }

    @Test
    @SuppressWarnings("resource")
    void testShouldRunAsyncTransactionFunctionExample() {
        try (var example =
                new AsyncTransactionFunctionExample(uri, USER, neo4j.adminPassword())) {
            // create some 'Product' nodes
            try (var session = neo4j.driver().session()) {
                session.run(
                        "UNWIND ['Infinity Gauntlet', 'Mjölnir'] AS item " + "CREATE (:Product {id: 0, title: item})");
            }

            var stdIOCapture = StdIOCapture.capture();

            // print all 'Product' nodes to fake stdout
            try (stdIOCapture) {
                var summary = await(example.printAllProducts());
                assertEquals(QueryType.READ_ONLY, summary.queryType());
            }

            Set<String> capturedOutput = new HashSet<>(stdIOCapture.stdout());
            assertEquals(new HashSet<>(asList("Infinity Gauntlet", "Mjölnir")), capturedOutput);
        }
    }

    @Test
    void testPassBookmarksExample() {
        try (var example = new PassBookmarkExample(uri, USER, neo4j.adminPassword())) {
            // When
            example.addEmployAndMakeFriends();

            // Then
            assertEquals(1, companyCount("Wayne Enterprises"));
            assertEquals(1, companyCount("LexCorp"));
            assertEquals(1, personCount("Alice"));
            assertEquals(1, personCount("Bob"));

            var employeeCountOfWayne = readInt(
                    "MATCH (emp:Person)-[WORKS_FOR]->(com:Company) WHERE com.name = 'Wayne Enterprises' RETURN count(emp)");
            assertEquals(1, employeeCountOfWayne);

            var employeeCountOfLexCorp = readInt(
                    "MATCH (emp:Person)-[WORKS_FOR]->(com:Company) WHERE com.name = 'LexCorp' RETURN count(emp)");
            assertEquals(1, employeeCountOfLexCorp);

            var friendCount =
                    readInt("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) RETURN count(a)");
            assertEquals(1, friendCount);
        }
    }

    @Test
    @EnabledOnNeo4jWith(BOLT_V4)
    @SuppressWarnings("resource")
    void testShouldRunRxAutocommitTransactionExample() {
        try (var example =
                new RxAutocommitTransactionExample(uri, USER, neo4j.adminPassword())) {
            // create some 'Product' nodes
            try (var session = neo4j.driver().session()) {
                session.run("UNWIND ['Tesseract', 'Orb', 'Eye of Agamotto'] AS item "
                        + "CREATE (:Product {id: 0, title: item})");
            }

            // read all 'Product' nodes
            var titles = await(example.readProductTitles());
            assertEquals(new HashSet<>(asList("Tesseract", "Orb", "Eye of Agamotto")), new HashSet<>(titles));
        }
    }

    @Test
    @EnabledOnNeo4jWith(BOLT_V4)
    @SuppressWarnings("resource")
    void testShouldRunRxTransactionFunctionExampleReactor() {
        try (var example =
                new RxTransactionFunctionExample(uri, USER, neo4j.adminPassword())) {
            // create some 'Product' nodes
            try (var session = neo4j.driver().session()) {
                session.run(
                        "UNWIND ['Infinity Gauntlet', 'Mjölnir'] AS item " + "CREATE (:Product {id: 0, title: item})");
            }

            var stdIOCapture = StdIOCapture.capture();

            // print all 'Product' nodes to fake stdout
            try (stdIOCapture) {
                final var summaryList = await(example.printAllProducts());
                assertEquals(1, summaryList.size());
                var summary = summaryList.get(0);
                assertEquals(QueryType.READ_ONLY, summary.queryType());
            }

            Set<String> capturedOutput = new HashSet<>(stdIOCapture.stdout());
            assertEquals(new HashSet<>(asList("Infinity Gauntlet", "Mjölnir")), capturedOutput);
        }
    }

    @Test
    @EnabledOnNeo4jWith(BOLT_V4)
    void testShouldRunRxResultConsumeExampleReactor() {
        // Given
        write("CREATE (a:Person {name: 'Alice'})");
        write("CREATE (a:Person {name: 'Bob'})");
        try (var example = new RxResultConsumeExample(uri, USER, neo4j.adminPassword())) {
            // When
            var names = await(example.getPeople());

            // Then
            assertEquals(List.of("Alice", "Bob"), names);
        }
    }

    @Test
    @EnabledOnNeo4jWith(value = BOLT_V4, edition = ENTERPRISE)
    void testUseAnotherDatabaseExample() {
        var driver = neo4j.driver();
        dropDatabase(driver, "examples");
        createDatabase(driver, "examples");

        try (var example = new DatabaseSelectionExample(uri, USER, neo4j.adminPassword())) {
            // When
            example.useAnotherDatabaseExample();

            // Then
            var greetingCount = readInt("examples", "MATCH (a:Greeting) RETURN count(a)", Values.parameters());
            assertEquals(1, greetingCount);
        }
    }
}
