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

import static java.util.Collections.emptyList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.driver.SessionConfig.builder;
import static org.neo4j.driver.SessionConfig.forDatabase;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.util.Matchers.arithmeticError;
import static org.neo4j.driver.internal.util.Matchers.connectionAcquisitionTimeoutError;
import static org.neo4j.driver.internal.util.Neo4jFeature.BOLT_V4;
import static org.neo4j.driver.testutil.DaemonThreadFactory.daemon;

import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SimpleQueryRunner;
import org.neo4j.driver.TransactionCallback;
import org.neo4j.driver.TransactionContext;
import org.neo4j.driver.exceptions.AuthenticationException;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.ResultConsumedException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.driver.internal.DriverFactory;
import org.neo4j.driver.internal.security.BoltSecurityPlanManager;
import org.neo4j.driver.internal.util.DisabledOnNeo4jWith;
import org.neo4j.driver.internal.util.DriverFactoryWithFixedRetryLogic;
import org.neo4j.driver.internal.util.EnabledOnNeo4jWith;
import org.neo4j.driver.reactivestreams.ReactiveResult;
import org.neo4j.driver.reactivestreams.ReactiveSession;
import org.neo4j.driver.summary.QueryType;
import org.neo4j.driver.testutil.DatabaseExtension;
import org.neo4j.driver.testutil.ParallelizableIT;
import org.neo4j.driver.testutil.TestUtil;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ParallelizableIT
class SessionIT {
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    private Driver driver;
    private ExecutorService executor;

    @AfterEach
    void tearDown() {
        if (driver != null) {
            driver.close();
        }
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Test
    @SuppressWarnings("resource")
    void shouldKnowSessionIsClosed() {
        // Given
        var session = neo4j.driver().session();

        // When
        session.close();

        // Then
        assertFalse(session.isOpen());
    }

    @Test
    void shouldHandleNullConfig() {
        // Given
        driver = GraphDatabase.driver(neo4j.uri(), neo4j.authTokenManager(), null, null);
        var session = driver.session();

        // When
        session.close();

        // Then
        assertFalse(session.isOpen());
    }

    @Test
    @SuppressWarnings({"resource"})
    void shouldHandleNullAuthToken() {
        // null auth token should be interpreted as AuthTokens.none() and fail driver creation
        // because server expects basic auth
        assertThrows(AuthenticationException.class, () -> GraphDatabase.driver(neo4j.uri(), (AuthToken) null)
                .verifyConnectivity());
    }

    @Test
    void executeReadTxInReadSession() {
        testExecuteReadTx(AccessMode.READ);
    }

    @Test
    void executeReadTxInWriteSession() {
        testExecuteReadTx(AccessMode.WRITE);
    }

    @Test
    void executeWriteTxInReadSession() {
        testExecuteWriteTx(AccessMode.READ);
    }

    @Test
    void executeWriteTxInWriteSession() {
        testExecuteWriteTx(AccessMode.WRITE);
    }

    @Test
    void rollsBackWriteTxInReadSessionWhenFunctionThrows() {
        testTxRollbackWhenFunctionThrows(AccessMode.READ);
    }

    @Test
    void rollsBackWriteTxInWriteSessionWhenFunctionThrows() {
        testTxRollbackWhenFunctionThrows(AccessMode.WRITE);
    }

    @Test
    void readTxRetriedUntilSuccess() {
        var failures = 6;
        var retries = failures + 1;
        try (var driver = newDriverWithFixedRetries(retries)) {
            try (var session = driver.session()) {
                session.run("CREATE (:Person {name: 'Bruce Banner'})");
            }

            var work = newThrowingWorkSpy("MATCH (n) RETURN n.name", failures);
            try (var session = driver.session()) {
                var record = session.executeRead(work);
                assertEquals("Bruce Banner", record.get(0).asString());
            }

            verify(work, times(retries)).execute(any(TransactionContext.class));
        }
    }

    @Test
    void writeTxRetriedUntilSuccess() {
        var failures = 4;
        var retries = failures + 1;
        try (var driver = newDriverWithFixedRetries(retries)) {
            var work = newThrowingWorkSpy("CREATE (p:Person {name: 'Hulk'}) RETURN p", failures);
            try (var session = driver.session()) {
                var record = session.executeWrite(work);
                assertEquals("Hulk", record.get(0).asNode().get("name").asString());
            }

            try (var session = driver.session()) {
                var record = session.run("MATCH (p: Person {name: 'Hulk'}) RETURN count(p)")
                        .single();
                assertEquals(1, record.get(0).asInt());
            }

            verify(work, times(retries)).execute(any(TransactionContext.class));
        }
    }

    @Test
    void readTxRetriedUntilFailure() {
        var failures = 3;
        var retries = failures - 1;
        try (var driver = newDriverWithFixedRetries(retries)) {
            var work = newThrowingWorkSpy("MATCH (n) RETURN n.name", failures);
            try (var session = driver.session()) {
                assertThrows(ServiceUnavailableException.class, () -> session.executeRead(work));
            }

            verify(work, times(failures)).execute(any(TransactionContext.class));
        }
    }

    @Test
    void writeTxRetriedUntilFailure() {
        var failures = 8;
        var retries = failures - 1;
        try (var driver = newDriverWithFixedRetries(retries)) {
            var work = newThrowingWorkSpy("CREATE (:Person {name: 'Ronan'})", failures);
            try (var session = driver.session()) {
                assertThrows(ServiceUnavailableException.class, () -> session.executeWrite(work));
            }

            try (var session = driver.session()) {
                var result = session.run("MATCH (p:Person {name: 'Ronan'}) RETURN count(p)");
                assertEquals(0, result.single().get(0).asInt());
            }

            verify(work, times(failures)).execute(any(TransactionContext.class));
        }
    }

    @Test
    void writeTxRetryErrorsAreCollected() {
        try (var driver = newDriverWithLimitedRetries(5)) {
            var work = newThrowingWorkSpy("CREATE (:Person {name: 'Ronan'})", Integer.MAX_VALUE);
            int suppressedErrors;
            try (var session = driver.session()) {
                var e = assertThrows(ServiceUnavailableException.class, () -> session.executeWrite(work));
                assertThat(e.getSuppressed(), not(emptyArray()));
                suppressedErrors = e.getSuppressed().length;
            }

            try (var session = driver.session()) {
                var result = session.run("MATCH (p:Person {name: 'Ronan'}) RETURN count(p)");
                assertEquals(0, result.single().get(0).asInt());
            }

            verify(work, times(suppressedErrors + 1)).execute(any(TransactionContext.class));
        }
    }

    @Test
    void readTxRetryErrorsAreCollected() {
        try (var driver = newDriverWithLimitedRetries(4)) {
            var work = newThrowingWorkSpy("MATCH (n) RETURN n.name", Integer.MAX_VALUE);
            int suppressedErrors;
            try (var session = driver.session()) {
                var e = assertThrows(ServiceUnavailableException.class, () -> session.executeRead(work));
                assertThat(e.getSuppressed(), not(emptyArray()));
                suppressedErrors = e.getSuppressed().length;
            }

            verify(work, times(suppressedErrors + 1)).execute(any(TransactionContext.class));
        }
    }

    @Test
    void readTxCommittedWithoutTxSuccess() {
        try (var driver = newDriverWithoutRetries();
                var session = driver.session()) {
            assertTrue(session.lastBookmarks().isEmpty());

            long answer = session.executeRead(
                    tx -> tx.run("RETURN 42").single().get(0).asLong());
            assertEquals(42, answer);

            // bookmark should be not-null after commit
            assertEquals(1, session.lastBookmarks().size());
        }
    }

    @Test
    void writeTxCommittedWithoutTxSuccess() {
        try (var driver = newDriverWithoutRetries()) {
            try (var session = driver.session()) {
                long answer = session.executeWrite(tx -> tx.run("CREATE (:Person {name: 'Thor Odinson'}) RETURN 42")
                        .single()
                        .get(0)
                        .asLong());
                assertEquals(42, answer);
            }

            try (var session = driver.session()) {
                var result = session.run("MATCH (p:Person {name: 'Thor Odinson'}) RETURN count(p)");
                assertEquals(1, result.single().get(0).asInt());
            }
        }
    }

    @Test
    void readTxRolledBackWithTxFailure() {
        try (var driver = newDriverWithoutRetries();
                var session = driver.session()) {
            assertTrue(session.lastBookmarks().isEmpty());

            assertThrows(
                    IllegalStateException.class,
                    () -> session.executeRead(tx -> {
                        var result = tx.run("RETURN 42");
                        var single = result.single().get(0).asLong();
                        throw new IllegalStateException();
                    }));

            // bookmark should remain null after rollback
            assertTrue(session.lastBookmarks().isEmpty());
        }
    }

    @Test
    void writeTxRolledBackWithTxFailure() {
        try (var driver = newDriverWithoutRetries()) {
            try (var session = driver.session()) {
                assertThrows(
                        IllegalStateException.class,
                        () -> session.executeWrite(tx -> {
                            tx.run("CREATE (:Person {name: 'Natasha Romanoff'})");
                            throw new IllegalStateException();
                        }));
            }

            try (var session = driver.session()) {
                var result = session.run("MATCH (p:Person {name: 'Natasha Romanoff'}) RETURN count(p)");
                assertEquals(0, result.single().get(0).asInt());
            }
        }
    }

    @Test
    void readTxRolledBackWhenExceptionIsThrown() {
        try (var driver = newDriverWithoutRetries();
                var session = driver.session()) {
            assertTrue(session.lastBookmarks().isEmpty());

            assertThrows(
                    IllegalStateException.class,
                    () -> session.executeRead(tx -> {
                        var result = tx.run("RETURN 42");
                        if (result.single().get(0).asLong() == 42) {
                            throw new IllegalStateException();
                        }
                        return 1L;
                    }));

            // bookmark should remain null after rollback
            assertTrue(session.lastBookmarks().isEmpty());
        }
    }

    @Test
    void writeTxRolledBackWhenExceptionIsThrown() {
        try (var driver = newDriverWithoutRetries()) {
            try (var session = driver.session()) {
                assertThrows(
                        IllegalStateException.class,
                        () -> session.executeWrite(tx -> {
                            tx.run("CREATE (:Person {name: 'Loki Odinson'})");
                            throw new IllegalStateException();
                        }));
            }

            try (var session = driver.session()) {
                var result = session.run("MATCH (p:Person {name: 'Natasha Romanoff'}) RETURN count(p)");
                assertEquals(0, result.single().get(0).asInt());
            }
        }
    }

    @Test
    @SuppressWarnings("resource")
    void transactionRunShouldFailOnDeadlocks() throws Exception {
        final var nodeId1 = 42;
        final var nodeId2 = 4242;
        final var newNodeId1 = 1;
        final var newNodeId2 = 2;

        createNodeWithId(nodeId1);
        createNodeWithId(nodeId2);

        final var latch1 = new CountDownLatch(1);
        final var latch2 = new CountDownLatch(1);

        Future<Void> result1 = executeInDifferentThread(() -> {
            try (var session = neo4j.driver().session();
                    var tx = session.beginTransaction()) {
                // lock first node
                updateNodeId(tx, nodeId1, newNodeId1).consume();

                latch1.await();
                latch2.countDown();

                // lock second node
                updateNodeId(tx, nodeId2, newNodeId1).consume();

                tx.commit();
            }
            return null;
        });

        Future<Void> result2 = executeInDifferentThread(() -> {
            try (var session = neo4j.driver().session();
                    var tx = session.beginTransaction()) {
                // lock second node
                updateNodeId(tx, nodeId2, newNodeId2).consume();

                latch1.countDown();
                latch2.await();

                // lock first node
                updateNodeId(tx, nodeId1, newNodeId2).consume();

                tx.commit();
            }
            return null;
        });

        var firstResultFailed = assertOneOfTwoFuturesFailWithDeadlock(result1, result2);
        if (firstResultFailed) {
            assertEquals(0, countNodesWithId(newNodeId1));
            assertEquals(2, countNodesWithId(newNodeId2));
        } else {
            assertEquals(2, countNodesWithId(newNodeId1));
            assertEquals(0, countNodesWithId(newNodeId2));
        }
    }

    @Test
    @SuppressWarnings("resource")
    void writeTransactionFunctionShouldRetryDeadlocks() throws Exception {
        final var nodeId1 = 42;
        final var nodeId2 = 4242;
        final var nodeId3 = 424242;
        final var newNodeId1 = 1;
        final var newNodeId2 = 2;

        createNodeWithId(nodeId1);
        createNodeWithId(nodeId2);

        final var latch1 = new CountDownLatch(1);
        final var latch2 = new CountDownLatch(1);

        Future<Void> result1 = executeInDifferentThread(() -> {
            try (var session = neo4j.driver().session();
                    var tx = session.beginTransaction()) {
                // lock first node
                updateNodeId(tx, nodeId1, newNodeId1).consume();

                latch1.await();
                latch2.countDown();

                // lock second node
                updateNodeId(tx, nodeId2, newNodeId1).consume();

                tx.commit();
            }
            return null;
        });

        Future<Void> result2 = executeInDifferentThread(() -> {
            try (var session = neo4j.driver().session()) {
                session.executeWrite(tx -> {
                    // lock second node
                    updateNodeId(tx, nodeId2, newNodeId2).consume();

                    latch1.countDown();
                    await(latch2);

                    // lock first node
                    updateNodeId(tx, nodeId1, newNodeId2).consume();

                    createNodeWithId(nodeId3);

                    return null;
                });
            }
            return null;
        });

        var firstResultFailed = false;
        try {
            // first future may:
            // 1) succeed, when it's tx was able to grab both locks and tx in other future was
            //    terminated because of a deadlock
            // 2) fail, when it's tx was terminated because of a deadlock
            assertNull(result1.get(20, TimeUnit.SECONDS));
        } catch (ExecutionException e) {
            firstResultFailed = true;
        }

        // second future can't fail because deadlocks are retried
        assertNull(result2.get(20, TimeUnit.SECONDS));

        if (firstResultFailed) {
            // tx with retries was successful and updated ids
            assertEquals(0, countNodesWithId(newNodeId1));
            assertEquals(2, countNodesWithId(newNodeId2));
        } else {
            // tx without retries was successful and updated ids
            // tx with retries did not manage to find nodes because their ids were updated
            assertEquals(2, countNodesWithId(newNodeId1));
            assertEquals(0, countNodesWithId(newNodeId2));
        }
        // tx with retries was successful and created an additional node
        assertEquals(1, countNodesWithId(nodeId3));
    }

    @Test
    @SuppressWarnings("resource")
    void shouldExecuteTransactionWorkInCallerThread() {
        var maxFailures = 3;
        var callerThread = Thread.currentThread();

        try (var session = neo4j.driver().session()) {
            var result = session.executeRead(new TransactionCallback<String>() {
                int failures;

                @Override
                public String execute(TransactionContext tx) {
                    assertSame(callerThread, Thread.currentThread());
                    if (failures++ < maxFailures) {
                        throw new ServiceUnavailableException("Oh no");
                    }
                    return "Hello";
                }
            });

            assertEquals("Hello", result);
        }
    }

    @Test
    @SuppressWarnings("resource")
    void shouldThrowRunFailureImmediatelyAndCloseSuccessfully() {
        try (var session = neo4j.driver().session()) {
            var e = assertThrows(ClientException.class, () -> session.run("RETURN 1 * \"x\""));

            assertThat(e.getMessage(), containsString("Type mismatch"));
        }
    }

    @EnabledOnNeo4jWith(BOLT_V4)
    @Test
    @SuppressWarnings("resource")
    void shouldNotPropagateFailureWhenStreamingIsCancelled() {
        var session = neo4j.driver().session();
        session.run("UNWIND range(20000, 0, -1) AS x RETURN 10 / x");
        session.close();
    }

    @Test
    @SuppressWarnings("resource")
    void shouldNotBePossibleToConsumeResultAfterSessionIsClosed() {
        Result result;
        try (var session = neo4j.driver().session()) {
            result = session.run("UNWIND range(1, 20000) AS x RETURN x");
        }

        assertThrows(
                ResultConsumedException.class,
                () -> result.list(record -> record.get(0).asInt()));
    }

    @Test
    @SuppressWarnings("resource")
    void shouldThrowRunFailureImmediatelyAfterMultipleSuccessfulRunsAndCloseSuccessfully() {
        try (var session = neo4j.driver().session()) {
            session.run("CREATE ()");
            session.run("CREATE ()");

            var e = assertThrows(ClientException.class, () -> session.run("RETURN 1 * \"x\""));

            assertThat(e.getMessage(), containsString("Type mismatch"));
        }
    }

    @Test
    @SuppressWarnings("resource")
    void shouldThrowRunFailureImmediatelyAndAcceptSubsequentRun() {
        try (var session = neo4j.driver().session()) {
            session.run("CREATE ()");
            session.run("CREATE ()");
            var e = assertThrows(ClientException.class, () -> session.run("RETURN 1 * \"x\""));
            assertThat(e.getMessage(), containsString("Type mismatch"));
            session.run("CREATE ()");
        }
    }

    @Test
    @SuppressWarnings("resource")
    void shouldCloseCleanlyWhenRunErrorConsumed() {
        var session = neo4j.driver().session();

        session.run("CREATE ()");

        var e = assertThrows(
                ClientException.class, () -> session.run("RETURN 10 / 0").consume());
        assertThat(e.getMessage(), containsString("/ by zero"));

        session.run("CREATE ()");

        session.close();
        assertFalse(session.isOpen());
    }

    @Test
    @SuppressWarnings("resource")
    void shouldConsumePreviousResultBeforeRunningNewQuery() {
        try (var session = neo4j.driver().session()) {
            session.run("UNWIND range(1000, 0, -1) AS x RETURN 42 / x");

            var e = assertThrows(ClientException.class, () -> session.run("RETURN 1"));
            assertThat(e.getMessage(), containsString("/ by zero"));
        }
    }

    @Test
    void shouldNotRetryOnConnectionAcquisitionTimeout() {
        var maxPoolSize = 3;
        var config = Config.builder()
                .withMaxConnectionPoolSize(maxPoolSize)
                .withConnectionAcquisitionTimeout(0, TimeUnit.SECONDS)
                .withMaxTransactionRetryTime(42, TimeUnit.DAYS) // retry for a really long time
                .withEventLoopThreads(1)
                .build();

        driver = GraphDatabase.driver(neo4j.uri(), neo4j.authTokenManager(), config);

        for (var i = 0; i < maxPoolSize; i++) {
            driver.session().beginTransaction();
        }

        var invocations = new AtomicInteger();
        var e = assertThrows(
                ClientException.class, () -> driver.session().executeWrite(tx -> invocations.incrementAndGet()));
        assertThat(e, is(connectionAcquisitionTimeoutError(0)));

        // work should never be invoked
        assertEquals(0, invocations.get());
    }

    @Test
    @SuppressWarnings("resource")
    void shouldReportFailureInClose() {
        var session = neo4j.driver().session();

        session.run("CYPHER runtime=interpreted UNWIND [2, 4, 8, 0] AS x RETURN 32 / x");

        var e = assertThrows(ClientException.class, session::close);
        assertThat(e, is(arithmeticError()));
    }

    @Test
    @SuppressWarnings("resource")
    void shouldNotAllowAccessingRecordsAfterSummary() {
        var recordCount = 10_000;
        var query = "UNWIND range(1, " + recordCount + ") AS x RETURN x";

        try (var session = neo4j.driver().session()) {
            var result = session.run(query);

            var summary = result.consume();
            assertEquals(query, summary.query().text());
            assertEquals(QueryType.READ_ONLY, summary.queryType());

            assertThrows(ResultConsumedException.class, result::list);
        }
    }

    @Test
    @SuppressWarnings("resource")
    void shouldNotAllowAccessingRecordsAfterSessionClosed() {
        var recordCount = 11_333;
        var query = "UNWIND range(1, " + recordCount + ") AS x RETURN 'Result-' + x";

        Result result;
        try (var session = neo4j.driver().session()) {
            result = session.run(query);
        }

        assertThrows(ResultConsumedException.class, result::list);
    }

    @Test
    @DisabledOnNeo4jWith(BOLT_V4)
    @SuppressWarnings("resource")
    void shouldAllowToConsumeRecordsSlowlyAndCloseSession() throws InterruptedException {
        var session = neo4j.driver().session();

        var result = session.run("UNWIND range(10000, 0, -1) AS x RETURN 10 / x");

        // summary couple records slowly with a sleep in-between
        for (var i = 0; i < 10; i++) {
            assertTrue(result.hasNext());
            assertNotNull(result.next());
            Thread.sleep(50);
        }

        var e = assertThrows(ClientException.class, session::close);
        assertThat(e, is(arithmeticError()));
    }

    @Test
    @SuppressWarnings("resource")
    void shouldAllowToConsumeRecordsSlowlyAndRetrieveSummary() throws InterruptedException {
        try (var session = neo4j.driver().session()) {
            var result = session.run("UNWIND range(8000, 1, -1) AS x RETURN 42 / x");

            // summary couple records slowly with a sleep in-between
            for (var i = 0; i < 12; i++) {
                assertTrue(result.hasNext());
                assertNotNull(result.next());
                Thread.sleep(50);
            }

            var summary = result.consume();
            assertNotNull(summary);
        }
    }

    @Test
    @SuppressWarnings({"resource", "ResultOfMethodCallIgnored"})
    void shouldBeResponsiveToThreadInterruptWhenWaitingForResult() {
        try (var session1 = neo4j.driver().session();
                var session2 = neo4j.driver().session()) {
            session1.run("CREATE (:Person {name: 'Beta Ray Bill'})").consume();

            var tx = session1.beginTransaction();
            tx.run("MATCH (n:Person {name: 'Beta Ray Bill'}) SET n.hammer = 'Mjolnir'")
                    .consume();

            // now 'Beta Ray Bill' node is locked

            // setup other thread to interrupt current thread when it blocks
            TestUtil.interruptWhenInWaitingState(Thread.currentThread());

            try {
                var e = assertThrows(ServiceUnavailableException.class, () -> session2.run(
                                "MATCH (n:Person {name: 'Beta Ray Bill'}) SET n.hammer = 'Stormbreaker'")
                        .consume());
                assertThat(e.getMessage(), containsString("Connection to the database terminated"));
                assertThat(e.getMessage(), containsString("Thread interrupted"));
            } finally {
                // clear interrupted flag
                Thread.interrupted();
            }
        }
    }

    @Test
    void shouldAllowLongRunningQueryWithConnectTimeout() throws Exception {
        var connectionTimeoutMs = 3_000;
        @SuppressWarnings("deprecation")
        var config = Config.builder()
                .withLogging(DEV_NULL_LOGGING)
                .withConnectionTimeout(connectionTimeoutMs, TimeUnit.MILLISECONDS)
                .build();

        try (var driver = GraphDatabase.driver(neo4j.uri(), neo4j.authTokenManager(), config)) {
            var session1 = driver.session();
            var session2 = driver.session();

            session1.run("CREATE (:Avenger {name: 'Hulk'})").consume();

            var tx = session1.beginTransaction();
            tx.run("MATCH (a:Avenger {name: 'Hulk'}) SET a.power = 100 RETURN a")
                    .consume();

            // Hulk node is now locked

            var latch = new CountDownLatch(1);
            var updateFuture = executeInDifferentThread(() -> {
                latch.countDown();
                return session2.run("MATCH (a:Avenger {name: 'Hulk'}) SET a.weight = 1000 RETURN a.power")
                        .single()
                        .get(0)
                        .asLong();
            });

            latch.await();
            // sleep more than connection timeout
            Thread.sleep(connectionTimeoutMs + 1_000);
            // verify that query is still executing and has not failed because of the read timeout
            assertFalse(updateFuture.isDone());

            tx.commit();

            long hulkPower = updateFuture.get(10, TimeUnit.SECONDS);
            assertEquals(100, hulkPower);
        }
    }

    @Test
    @SuppressWarnings("resource")
    void shouldAllowReturningNullFromTransactionFunction() {
        try (var session = neo4j.driver().session()) {
            assertNull(session.executeRead(tx -> null));
            assertNull(session.executeWrite(tx -> null));
        }
    }

    @Test
    @SuppressWarnings("resource")
    void shouldAllowIteratingOverEmptyResult() {
        try (var session = neo4j.driver().session()) {
            var result = session.run("UNWIND [] AS x RETURN x");
            assertFalse(result.hasNext());

            assertThrows(NoSuchElementException.class, result::next);
        }
    }

    @Test
    @SuppressWarnings("resource")
    void shouldAllowConsumingEmptyResult() {
        try (var session = neo4j.driver().session()) {
            var result = session.run("UNWIND [] AS x RETURN x");
            var summary = result.consume();
            assertNotNull(summary);
            assertEquals(QueryType.READ_ONLY, summary.queryType());
        }
    }

    @Test
    @SuppressWarnings("resource")
    void shouldAllowListEmptyResult() {
        try (var session = neo4j.driver().session()) {
            var result = session.run("UNWIND [] AS x RETURN x");
            assertEquals(emptyList(), result.list());
        }
    }

    @Test
    @SuppressWarnings("resource")
    void shouldReportFailureInSummary() {
        try (var session = neo4j.driver().session()) {
            var query = "UNWIND [1, 2, 3, 4, 0] AS x RETURN 10 / x";
            var result = session.run(query);

            var e = assertThrows(ClientException.class, result::consume);
            assertThat(e, is(arithmeticError()));

            var summary = result.consume();
            assertEquals(query, summary.query().text());
        }
    }

    @Test
    @SuppressWarnings("resource")
    void shouldNotAllowStartingMultipleTransactions() {
        try (var session = neo4j.driver().session()) {
            var tx = session.beginTransaction();
            assertNotNull(tx);

            for (var i = 0; i < 3; i++) {
                var e = assertThrows(ClientException.class, session::beginTransaction);
                assertThat(
                        e.getMessage(),
                        containsString("You cannot begin a transaction on a session with an open transaction"));
            }

            tx.close();

            assertNotNull(session.beginTransaction());
        }
    }

    @Test
    @SuppressWarnings("resource")
    void shouldCloseOpenTransactionWhenClosed() {
        try (var session = neo4j.driver().session()) {
            var tx = session.beginTransaction();
            tx.run("CREATE (:Node {id: 123})");
            tx.run("CREATE (:Node {id: 456})");

            tx.commit();
        }

        assertEquals(1, countNodesWithId(123));
        assertEquals(1, countNodesWithId(456));
    }

    @Test
    @SuppressWarnings("resource")
    void shouldRollbackOpenTransactionWhenClosed() {
        try (var session = neo4j.driver().session()) {
            var tx = session.beginTransaction();
            tx.run("CREATE (:Node {id: 123})");
            tx.run("CREATE (:Node {id: 456})");

            tx.rollback();
        }

        assertEquals(0, countNodesWithId(123));
        assertEquals(0, countNodesWithId(456));
    }

    @Test
    @SuppressWarnings("resource")
    void shouldSupportNestedQueries() {
        try (var session = neo4j.driver().session()) {
            // populate db with test data
            session.run("UNWIND range(1, 100) AS x CREATE (:Property {id: x})").consume();
            session.run("UNWIND range(1, 10) AS x CREATE (:Resource {id: x})").consume();

            var seenProperties = 0;
            var seenResources = 0;

            // read properties and resources using a single session
            var properties = session.run("MATCH (p:Property) RETURN p");
            while (properties.hasNext()) {
                assertNotNull(properties.next());
                seenProperties++;

                var resources = session.run("MATCH (r:Resource) RETURN r");
                while (resources.hasNext()) {
                    assertNotNull(resources.next());
                    seenResources++;
                }
            }

            assertEquals(100, seenProperties);
            assertEquals(1000, seenResources);
        }
    }

    @Test
    @DisabledOnNeo4jWith(BOLT_V4)
    @SuppressWarnings("resource")
    void shouldErrorWhenTryingToUseRxAPIWithoutBoltV4() {
        // Given
        var session = neo4j.driver().session(ReactiveSession.class);
        var result = Mono.fromDirect(session.run("RETURN 1"));

        // When trying to run the query on a server that is using a protocol that is lower than V4
        StepVerifier.create(result.flatMapMany(ReactiveResult::records))
                .expectErrorSatisfies(error -> {
                    // Then
                    assertThat(error, instanceOf(ClientException.class));
                    assertThat(
                            error.getMessage(),
                            containsString(
                                    "Driver is connected to the database that does not support driver reactive API"));
                })
                .verify();
    }

    @Test
    @DisabledOnNeo4jWith(BOLT_V4)
    @SuppressWarnings("resource")
    void shouldErrorWhenTryingToUseDatabaseNameWithoutBoltV4() {
        // Given
        var session = neo4j.driver().session(forDatabase("foo"));

        // When trying to run the query on a server that is using a protocol that is lower than V4
        var error = assertThrows(ClientException.class, () -> session.run("RETURN 1"));
        assertThat(error, instanceOf(ClientException.class));
        assertThat(
                error.getMessage(), containsString("Database name parameter for selecting database is not supported"));
    }

    @Test
    @DisabledOnNeo4jWith(BOLT_V4)
    @SuppressWarnings("resource")
    void shouldErrorWhenTryingToUseDatabaseNameWithoutBoltV4UsingTx() {
        // Given
        var session = neo4j.driver().session(forDatabase("foo"));

        // When trying to run the query on a server that is using a protocol that is lower than V4
        var error = assertThrows(ClientException.class, session::beginTransaction);
        assertThat(error, instanceOf(ClientException.class));
        assertThat(
                error.getMessage(), containsString("Database name parameter for selecting database is not supported"));
    }

    @Test
    @EnabledOnNeo4jWith(BOLT_V4)
    @SuppressWarnings("resource")
    void shouldAllowDatabaseName() {
        // Given
        try (var session = neo4j.driver().session(forDatabase("neo4j"))) {
            var result = session.run("RETURN 1");
            assertThat(result.single().get(0).asInt(), equalTo(1));
        }
    }

    @Test
    @EnabledOnNeo4jWith(BOLT_V4)
    @SuppressWarnings("resource")
    void shouldAllowDatabaseNameUsingTx() {
        try (var session = neo4j.driver().session(forDatabase("neo4j"));
                var transaction = session.beginTransaction()) {
            var result = transaction.run("RETURN 1");
            assertThat(result.single().get(0).asInt(), equalTo(1));
        }
    }

    @Test
    @EnabledOnNeo4jWith(BOLT_V4)
    @SuppressWarnings("resource")
    void shouldAllowDatabaseNameUsingTxWithRetries() {
        try (var session = neo4j.driver().session(forDatabase("neo4j"))) {
            int num =
                    session.executeRead(tx -> tx.run("RETURN 1").single().get(0).asInt());
            assertThat(num, equalTo(1));
        }
    }

    @Test
    @EnabledOnNeo4jWith(BOLT_V4)
    @SuppressWarnings("resource")
    void shouldErrorDatabaseWhenDatabaseIsAbsent() {
        var session = neo4j.driver().session(forDatabase("foo"));

        var error = assertThrows(ClientException.class, () -> {
            var result = session.run("RETURN 1");
            result.consume();
        });

        assertThat(error.getMessage(), containsString("Database does not exist. Database name: 'foo'"));
        session.close();
    }

    @Test
    @EnabledOnNeo4jWith(BOLT_V4)
    @SuppressWarnings("resource")
    void shouldErrorDatabaseNameUsingTxWhenDatabaseIsAbsent() {
        // Given
        var session = neo4j.driver().session(forDatabase("foo"));

        // When trying to run the query on a server that is using a protocol that is lower than V4
        var error = assertThrows(ClientException.class, () -> {
            var transaction = session.beginTransaction();
            var result = transaction.run("RETURN 1");
            result.consume();
        });
        assertThat(error.getMessage(), containsString("Database does not exist. Database name: 'foo'"));
        session.close();
    }

    @Test
    @EnabledOnNeo4jWith(BOLT_V4)
    @SuppressWarnings("resource")
    void shouldErrorDatabaseNameUsingTxWithRetriesWhenDatabaseIsAbsent() {
        // Given
        var session = neo4j.driver().session(forDatabase("foo"));

        // When trying to run the query on a database that does not exist
        var error = assertThrows(
                ClientException.class,
                () -> session.executeRead(tx -> tx.run("RETURN 1").consume()));
        assertThat(error.getMessage(), containsString("Database does not exist. Database name: 'foo'"));
        session.close();
    }

    @ParameterizedTest
    @MethodSource("managedTransactionsReturningResult")
    @SuppressWarnings("resource")
    void shouldErrorWhenResultIsReturned(Function<Session, Result> fn) {
        // GIVEN
        var session = neo4j.driver().session();

        // WHEN & THEN
        var error = assertThrows(ClientException.class, () -> fn.apply(session));
        assertEquals(
                "org.neo4j.driver.Result is not a valid return value, it should be consumed before producing a return value",
                error.getMessage());
        session.close();
    }

    static List<Function<Session, Result>> managedTransactionsReturningResult() {
        return List.of(
                session -> session.executeWrite(tx -> tx.run("RETURN 1")),
                session -> session.executeRead(tx -> tx.run("RETURN 1")),
                session -> session.executeWrite(tx -> tx.run("RETURN 1")),
                session -> session.executeRead(tx -> tx.run("RETURN 1")));
    }

    @SuppressWarnings("resource")
    private void testExecuteReadTx(AccessMode sessionMode) {
        var driver = neo4j.driver();

        // write some test data
        try (var session = driver.session()) {
            session.run("CREATE (:Person {name: 'Tony Stark'})");
            session.run("CREATE (:Person {name: 'Steve Rogers'})");
        }

        // read previously committed data
        try (var session =
                driver.session(builder().withDefaultAccessMode(sessionMode).build())) {
            var names = session.executeRead(tx -> {
                var records = tx.run("MATCH (p:Person) RETURN p.name AS name").list();
                return records.stream()
                        .map(record -> record.get("name").asString())
                        .collect(Collectors.toCollection(() -> new HashSet<>(records.size())));
            });

            assertThat(names, containsInAnyOrder("Tony Stark", "Steve Rogers"));
        }
    }

    @SuppressWarnings("resource")
    private void testExecuteWriteTx(AccessMode sessionMode) {
        var driver = neo4j.driver();

        // write some test data
        try (var session =
                driver.session(builder().withDefaultAccessMode(sessionMode).build())) {
            var material = session.executeWrite(tx -> {
                var result = tx.run("CREATE (s:Shield {material: 'Vibranium'}) RETURN s");
                var record = result.single();
                return record.get(0).asNode().get("material").asString();
            });

            assertEquals("Vibranium", material);
        }

        // read previously committed data
        try (var session = driver.session()) {
            var record = session.run("MATCH (s:Shield) RETURN s.material").single();
            assertEquals("Vibranium", record.get(0).asString());
        }
    }

    @SuppressWarnings("resource")
    private void testTxRollbackWhenFunctionThrows(AccessMode sessionMode) {
        var driver = neo4j.driver();

        try (var session =
                driver.session(builder().withDefaultAccessMode(sessionMode).build())) {
            assertThrows(
                    ClientException.class,
                    () -> session.executeWrite(tx -> {
                        tx.run("CREATE (:Person {name: 'Thanos'})");
                        // trigger division by zero error:
                        tx.run("UNWIND range(0, 1) AS i RETURN 10/i");
                        return null;
                    }));
        }

        // no data should have been committed
        try (var session = driver.session()) {
            var record = session.run("MATCH (p:Person {name: 'Thanos'}) RETURN count(p)")
                    .single();
            assertEquals(0, record.get(0).asInt());
        }
    }

    private Driver newDriverWithoutRetries() {
        return newDriverWithFixedRetries(0);
    }

    private Driver newDriverWithFixedRetries(int maxRetriesCount) {
        DriverFactory driverFactory = new DriverFactoryWithFixedRetryLogic(maxRetriesCount);
        return driverFactory.newInstance(
                neo4j.uri(),
                neo4j.authTokenManager(),
                noLoggingConfig(),
                BoltSecurityPlanManager.insecure(),
                null,
                null);
    }

    private Driver newDriverWithLimitedRetries(int maxTxRetryTime) {
        @SuppressWarnings("deprecation")
        var config = Config.builder()
                .withLogging(DEV_NULL_LOGGING)
                .withMaxTransactionRetryTime(maxTxRetryTime, TimeUnit.SECONDS)
                .build();
        return GraphDatabase.driver(neo4j.uri(), neo4j.authTokenManager(), config);
    }

    @SuppressWarnings("deprecation")
    private static Config noLoggingConfig() {
        return Config.builder().withLogging(DEV_NULL_LOGGING).build();
    }

    private static ThrowingWork newThrowingWorkSpy(String query, int failures) {
        return spy(new ThrowingWork(query, failures));
    }

    @SuppressWarnings("resource")
    private int countNodesWithId(int id) {
        try (var session = neo4j.driver().session()) {
            var result = session.run("MATCH (n {id: $id}) RETURN count(n)", parameters("id", id));
            return result.single().get(0).asInt();
        }
    }

    @SuppressWarnings("resource")
    private void createNodeWithId(int id) {
        try (var session = neo4j.driver().session()) {
            session.run("CREATE (n {id: $id})", parameters("id", id));
        }
    }

    private static Result updateNodeId(SimpleQueryRunner queryRunner, int currentId, int newId) {
        return queryRunner.run(
                "MATCH (n {id: $currentId}) SET n.id = $newId", parameters("currentId", currentId, "newId", newId));
    }

    @SuppressWarnings("ConstantValue")
    private static boolean assertOneOfTwoFuturesFailWithDeadlock(Future<Void> future1, Future<Void> future2)
            throws Exception {
        var firstFailed = false;
        try {
            assertNull(future1.get(20, TimeUnit.SECONDS));
        } catch (ExecutionException e) {
            assertDeadlockDetectedError(e);
            firstFailed = true;
        }

        try {
            assertNull(future2.get(20, TimeUnit.SECONDS));
        } catch (ExecutionException e) {
            assertFalse(firstFailed, "Both futures failed");
            assertDeadlockDetectedError(e);
        }
        return firstFailed;
    }

    private static void assertDeadlockDetectedError(ExecutionException e) {
        assertThat(e.getCause(), instanceOf(TransientException.class));
        var errorCode = ((TransientException) e.getCause()).code();
        assertEquals("Neo.TransientError.Transaction.DeadlockDetected", errorCode);
    }

    private <T> Future<T> executeInDifferentThread(Callable<T> callable) {
        if (executor == null) {
            executor = Executors.newCachedThreadPool(daemon(getClass().getSimpleName() + "-thread-"));
        }
        return executor.submit(callable);
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static class ThrowingWork implements TransactionCallback<Record> {
        final String query;
        final int failures;

        int invoked;

        ThrowingWork(String query, int failures) {
            this.query = query;
            this.failures = failures;
        }

        @Override
        public Record execute(TransactionContext tx) {
            var result = tx.run(query);
            if (invoked++ < failures) {
                throw new ServiceUnavailableException("");
            }
            return result.single();
        }
    }
}
