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

import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.testutil.DaemonThreadFactory.daemon;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SimpleQueryRunner;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.TransactionTerminatedException;
import org.neo4j.driver.internal.InternalSession;
import org.neo4j.driver.testutil.DatabaseExtension;
import org.neo4j.driver.testutil.ParallelizableIT;

@SuppressWarnings("deprecation")
@ParallelizableIT
class SessionResetIT {
    private static final String LONG_QUERY = "UNWIND range(0, 10000000) AS i CREATE (n:Node {idx: i}) DELETE n";

    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    private ExecutorService executor;

    @BeforeEach
    void setUp() {
        executor = Executors.newCachedThreadPool(daemon(getClass().getSimpleName() + "-thread"));
    }

    @AfterEach
    void tearDown() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Test
    void shouldTerminateAutoCommitQuery() {
        testQueryTermination(true);
    }

    @Test
    void shouldTerminateQueryInUnmanagedTransaction() {
        testQueryTermination(false);
    }

    @Test
    @SuppressWarnings("resource")
    void shouldAllowMoreQueriesAfterSessionReset() {
        // Given
        try (var session = (InternalSession) neo4j.driver().session()) {

            session.run("RETURN 1").consume();

            // When reset the state of this session
            session.reset();

            // Then can run successfully more queries without any error
            session.run("RETURN 2").consume();
        }
    }

    @Test
    @SuppressWarnings("resource")
    void shouldAllowMoreTxAfterSessionReset() {
        // Given
        try (var session = (InternalSession) neo4j.driver().session()) {
            try (var tx = session.beginTransaction()) {
                tx.run("RETURN 1");
                tx.commit();
            }

            // When reset the state of this session
            session.reset();

            // Then can run more Tx
            try (var tx = session.beginTransaction()) {
                tx.run("RETURN 2");
                tx.commit();
            }
        }
    }

    @Test
    @SuppressWarnings("resource")
    void shouldMarkTxAsFailedAndDisallowRunAfterSessionReset() {
        // Given
        try (var session = (InternalSession) neo4j.driver().session()) {
            var tx = session.beginTransaction();
            // When reset the state of this session
            session.reset();

            // Then
            assertThrows(TransactionTerminatedException.class, () -> {
                tx.run("RETURN 1");
                tx.commit();
            });
        }
    }

    @Test
    @SuppressWarnings("resource")
    void shouldAllowMoreTxAfterSessionResetInTx() {
        // Given
        try (var session = (InternalSession) neo4j.driver().session()) {
            try (var ignore = session.beginTransaction()) {
                // When reset the state of this session
                session.reset();
            }

            // Then can run more Tx
            try (var tx = session.beginTransaction()) {
                tx.run("RETURN 2");
                tx.commit();
            }
        }
    }

    @Test
    void resetShouldStopQueryWaitingForALock() throws Exception {
        testResetOfQueryWaitingForLock(new NodeIdUpdater() {
            @Override
            void performUpdate(
                    Driver driver,
                    int nodeId,
                    int newNodeId,
                    AtomicReference<InternalSession> usedSessionRef,
                    CountDownLatch latchToWait)
                    throws Exception {
                try (var session = (InternalSession) driver.session()) {
                    usedSessionRef.set(session);
                    latchToWait.await();
                    var result = updateNodeId(session, nodeId, newNodeId);
                    result.consume();
                }
            }
        });
    }

    @Test
    @SuppressWarnings("resource")
    void resetShouldStopTransactionWaitingForALock() throws Exception {
        testResetOfQueryWaitingForLock(new NodeIdUpdater() {
            @Override
            public void performUpdate(
                    Driver driver,
                    int nodeId,
                    int newNodeId,
                    AtomicReference<InternalSession> usedSessionRef,
                    CountDownLatch latchToWait)
                    throws Exception {
                try (var session = (InternalSession) neo4j.driver().session();
                        var tx = session.beginTransaction()) {
                    usedSessionRef.set(session);
                    latchToWait.await();
                    var result = updateNodeId(tx, nodeId, newNodeId);
                    result.consume();
                }
            }
        });
    }

    @Test
    void resetShouldStopWriteTransactionWaitingForALock() throws Exception {
        var invocationsOfWork = new AtomicInteger();

        testResetOfQueryWaitingForLock(new NodeIdUpdater() {
            @Override
            public void performUpdate(
                    Driver driver,
                    int nodeId,
                    int newNodeId,
                    AtomicReference<InternalSession> usedSessionRef,
                    CountDownLatch latchToWait)
                    throws Exception {
                try (var session = (InternalSession) driver.session()) {
                    usedSessionRef.set(session);
                    latchToWait.await();

                    session.executeWrite(tx -> {
                        invocationsOfWork.incrementAndGet();
                        var result = updateNodeId(tx, nodeId, newNodeId);
                        result.consume();
                        return null;
                    });
                }
            }
        });

        assertEquals(1, invocationsOfWork.get());
    }

    @Test
    @SuppressWarnings("resource")
    void shouldBeAbleToRunMoreQueriesAfterResetOnNoErrorState() {
        try (var session = (InternalSession) neo4j.driver().session()) {
            // Given
            session.reset();

            // When
            var tx = session.beginTransaction();
            tx.run("CREATE (n:FirstNode)");
            tx.commit();

            // Then the outcome of both queries should be visible
            var result = session.run("MATCH (n) RETURN count(n)");
            var nodes = result.single().get("count(n)").asLong();
            assertThat(nodes, equalTo(1L));
        }
    }

    @Test
    @SuppressWarnings("resource")
    void shouldHandleResetBeforeRun() {
        try (var session = (InternalSession) neo4j.driver().session();
                var tx = session.beginTransaction()) {
            session.reset();

            assertThrows(TransactionTerminatedException.class, () -> tx.run("CREATE (n:FirstNode)"));
        }
    }

    @Test
    @SuppressWarnings({"resource", "ResultOfMethodCallIgnored"})
    void shouldHandleResetFromMultipleThreads() throws Throwable {
        var session = (InternalSession) neo4j.driver().session();

        var beforeCommit = new CountDownLatch(1);
        var afterReset = new CountDownLatch(1);

        Future<Void> txFuture = executor.submit(() -> {
            var tx1 = session.beginTransaction();
            tx1.run("CREATE (n:FirstNode)");
            beforeCommit.countDown();
            afterReset.await();

            // session has been reset, it should not be possible to commit the transaction
            try {
                tx1.commit();
            } catch (Neo4jException ignore) {
            }

            try (var tx2 = session.beginTransaction()) {
                tx2.run("CREATE (n:SecondNode)");
                tx2.commit();
            }

            return null;
        });

        Future<Void> resetFuture = executor.submit(() -> {
            beforeCommit.await();
            session.reset();
            afterReset.countDown();
            return null;
        });

        executor.shutdown();
        executor.awaitTermination(20, SECONDS);

        txFuture.get(20, SECONDS);
        resetFuture.get(20, SECONDS);

        assertEquals(0, countNodes("FirstNode"));
        assertEquals(1, countNodes("SecondNode"));
    }

    @SuppressWarnings("resource")
    private void testResetOfQueryWaitingForLock(NodeIdUpdater nodeIdUpdater) throws Exception {
        var nodeId = 42;
        var newNodeId1 = 4242;
        var newNodeId2 = 424242;

        createNodeWithId(nodeId);

        var nodeLocked = new CountDownLatch(1);
        var otherSessionRef = new AtomicReference<InternalSession>();

        try (var session = (InternalSession) neo4j.driver().session();
                var tx = session.beginTransaction()) {
            var txResult = nodeIdUpdater.update(nodeId, newNodeId1, otherSessionRef, nodeLocked);

            var result = updateNodeId(tx, nodeId, newNodeId2);
            result.consume();

            nodeLocked.countDown();
            // give separate thread some time to block on a lock
            Thread.sleep(2_000);
            otherSessionRef.get().reset();

            assertTransactionTerminated(txResult);
            tx.commit();
        }

        try (var session = neo4j.driver().session()) {
            var result = session.run("MATCH (n) RETURN n.id AS id");
            var value = result.single().get("id").asInt();
            assertEquals(newNodeId2, value);
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

    private static void assertTransactionTerminated(Future<Void> work) {
        var e = assertThrows(ExecutionException.class, () -> work.get(20, TimeUnit.SECONDS));
        assertThat(e.getCause(), instanceOf(ClientException.class));
        assertThat(e.getCause().getMessage(), startsWith("The transaction has been terminated"));
    }

    private void testQueryTermination(boolean autoCommit) {
        var queryResult = runQueryInDifferentThreadAndResetSession(SessionResetIT.LONG_QUERY, autoCommit);
        var e = assertThrows(ExecutionException.class, () -> queryResult.get(10, SECONDS));
        assertThat(e.getCause(), instanceOf(Neo4jException.class));
        awaitNoActiveQueries();
    }

    @SuppressWarnings("resource")
    private Future<Void> runQueryInDifferentThreadAndResetSession(
            @SuppressWarnings("SameParameterValue") String query, boolean autoCommit) {
        var sessionRef = new AtomicReference<InternalSession>();

        Future<Void> queryResult = runAsync(() -> {
            var session = (InternalSession) neo4j.driver().session();
            sessionRef.set(session);
            runQuery(session, query, autoCommit);
        });

        awaitActiveQueriesToContain(query);

        var session = sessionRef.get();
        assertNotNull(session);
        session.reset();

        return queryResult;
    }

    private static void runQuery(Session session, String query, boolean autoCommit) {
        if (autoCommit) {
            session.run(query).consume();
        } else {
            try (var tx = session.beginTransaction()) {
                tx.run(query);
                tx.commit();
            }
        }
    }

    private void awaitNoActiveQueries() {
        awaitCondition(() -> activeQueryCount(neo4j.driver()) == 0);
    }

    private void awaitActiveQueriesToContain(String value) {
        awaitCondition(() -> activeQueryNames(neo4j.driver()).stream().anyMatch(query -> query.contains(value)));
    }

    @SuppressWarnings("resource")
    private long countNodes(String label) {
        try (var session = neo4j.driver().session()) {
            var result = session.run("MATCH (n" + (label == null ? "" : ":" + label) + ") RETURN count(n) AS result");
            return result.single().get(0).asLong();
        }
    }

    private abstract class NodeIdUpdater {
        final Future<Void> update(
                int nodeId,
                int newNodeId,
                AtomicReference<InternalSession> usedSessionRef,
                CountDownLatch latchToWait) {
            return executor.submit(() -> {
                performUpdate(neo4j.driver(), nodeId, newNodeId, usedSessionRef, latchToWait);
                return null;
            });
        }

        abstract void performUpdate(
                Driver driver,
                int nodeId,
                int newNodeId,
                AtomicReference<InternalSession> usedSessionRef,
                CountDownLatch latchToWait)
                throws Exception;
    }

    private static void awaitCondition(BooleanSupplier condition) {
        awaitCondition(condition, MINUTES.toMillis(2));
    }

    private static void awaitCondition(BooleanSupplier condition, long value) {
        var deadline = System.currentTimeMillis() + TimeUnit.MILLISECONDS.toMillis(value);
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                fail("Condition was not met in time");
            }
            try {
                MILLISECONDS.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("Interrupted while waiting");
            }
        }
    }

    private static int activeQueryCount(Driver driver) {
        return activeQueryNames(driver).size();
    }

    private static List<String> activeQueryNames(Driver driver) {
        try (var session = driver.session()) {
            if (neo4j.isNeo4j44OrEarlier()) {
                return session.run("CALL dbms.listQueries() YIELD query RETURN query").list().stream()
                        .map(record -> record.get(0).asString())
                        .filter(query -> !query.contains("dbms.listQueries")) // do not include listQueries procedure
                        .collect(toList());
            } else {
                return session.run("SHOW TRANSACTIONS").list().stream()
                        .map(record -> record.get("currentQuery").asString())
                        .filter(query -> !query.contains("SHOW TRANSACTIONS")) // do not include listQueries procedure
                        .toList();
            }
        }
    }
}
