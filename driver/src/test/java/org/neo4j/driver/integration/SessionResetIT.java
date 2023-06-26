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

import static java.util.Collections.newSetFromMap;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.testutil.DaemonThreadFactory.daemon;
import static org.neo4j.driver.testutil.TestUtil.await;

import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.driver.Driver;
import org.neo4j.driver.QueryRunner;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.TransactionTerminatedException;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.driver.internal.InternalSession;
import org.neo4j.driver.testutil.DatabaseExtension;
import org.neo4j.driver.testutil.ParallelizableIT;

@SuppressWarnings("deprecation")
@ParallelizableIT
class SessionResetIT {
    private static final String SHORT_QUERY_1 = "CREATE (n:Node {name: 'foo', occupation: 'bar'})";
    private static final String SHORT_QUERY_2 = "MATCH (n:Node {name: 'foo'}) RETURN count(n)";
    private static final String LONG_QUERY = "UNWIND range(0, 10000000) AS i CREATE (n:Node {idx: i}) DELETE n";
    ;

    private static final int STRESS_TEST_THREAD_COUNT = Runtime.getRuntime().availableProcessors() * 2;
    private static final long STRESS_TEST_DURATION_MS = SECONDS.toMillis(5);
    private static final String[] STRESS_TEST_QUERIES = {SHORT_QUERY_1, SHORT_QUERY_2, LONG_QUERY};

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
        testQueryTermination(LONG_QUERY, true);
    }

    @Test
    void shouldTerminateQueryInUnmanagedTransaction() {
        testQueryTermination(LONG_QUERY, false);
    }

    @Test
    void shouldTerminateAutoCommitQueriesRandomly() throws Exception {
        testRandomQueryTermination(true);
    }

    @Test
    void shouldTerminateQueriesInUnmanagedTransactionsRandomly() throws Exception {
        testRandomQueryTermination(false);
    }

    @Test
    void shouldAllowMoreQueriesAfterSessionReset() {
        // Given
        try (InternalSession session = (InternalSession) neo4j.driver().session()) {

            session.run("RETURN 1").consume();

            // When reset the state of this session
            session.reset();

            // Then can run successfully more queries without any error
            session.run("RETURN 2").consume();
        }
    }

    @Test
    void shouldAllowMoreTxAfterSessionReset() {
        // Given
        try (InternalSession session = (InternalSession) neo4j.driver().session()) {
            try (Transaction tx = session.beginTransaction()) {
                tx.run("RETURN 1");
                tx.commit();
            }

            // When reset the state of this session
            session.reset();

            // Then can run more Tx
            try (Transaction tx = session.beginTransaction()) {
                tx.run("RETURN 2");
                tx.commit();
            }
        }
    }

    @Test
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
    void shouldAllowMoreTxAfterSessionResetInTx() {
        // Given
        try (InternalSession session = (InternalSession) neo4j.driver().session()) {
            try (Transaction ignore = session.beginTransaction()) {
                // When reset the state of this session
                session.reset();
            }

            // Then can run more Tx
            try (Transaction tx = session.beginTransaction()) {
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
                try (InternalSession session = (InternalSession) driver.session()) {
                    usedSessionRef.set(session);
                    latchToWait.await();
                    Result result = updateNodeId(session, nodeId, newNodeId);
                    result.consume();
                }
            }
        });
    }

    @Test
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
                try (InternalSession session = (InternalSession) neo4j.driver().session();
                        Transaction tx = session.beginTransaction()) {
                    usedSessionRef.set(session);
                    latchToWait.await();
                    Result result = updateNodeId(tx, nodeId, newNodeId);
                    result.consume();
                }
            }
        });
    }

    @Test
    void resetShouldStopWriteTransactionWaitingForALock() throws Exception {
        AtomicInteger invocationsOfWork = new AtomicInteger();

        testResetOfQueryWaitingForLock(new NodeIdUpdater() {
            @Override
            public void performUpdate(
                    Driver driver,
                    int nodeId,
                    int newNodeId,
                    AtomicReference<InternalSession> usedSessionRef,
                    CountDownLatch latchToWait)
                    throws Exception {
                try (InternalSession session = (InternalSession) driver.session()) {
                    usedSessionRef.set(session);
                    latchToWait.await();

                    session.writeTransaction(tx -> {
                        invocationsOfWork.incrementAndGet();
                        Result result = updateNodeId(tx, nodeId, newNodeId);
                        result.consume();
                        return null;
                    });
                }
            }
        });

        assertEquals(1, invocationsOfWork.get());
    }

    @Test
    void shouldBeAbleToRunMoreQueriesAfterResetOnNoErrorState() {
        try (InternalSession session = (InternalSession) neo4j.driver().session()) {
            // Given
            session.reset();

            // When
            Transaction tx = session.beginTransaction();
            tx.run("CREATE (n:FirstNode)");
            tx.commit();

            // Then the outcome of both queries should be visible
            Result result = session.run("MATCH (n) RETURN count(n)");
            long nodes = result.single().get("count(n)").asLong();
            assertThat(nodes, equalTo(1L));
        }
    }

    @Test
    void shouldHandleResetBeforeRun() {
        try (var session = (InternalSession) neo4j.driver().session();
                var tx = session.beginTransaction()) {
            session.reset();

            assertThrows(TransactionTerminatedException.class, () -> tx.run("CREATE (n:FirstNode)"));
        }
    }

    @Test
    void shouldHandleResetFromMultipleThreads() throws Throwable {
        InternalSession session = (InternalSession) neo4j.driver().session();

        CountDownLatch beforeCommit = new CountDownLatch(1);
        CountDownLatch afterReset = new CountDownLatch(1);

        Future<Void> txFuture = executor.submit(() -> {
            Transaction tx1 = session.beginTransaction();
            tx1.run("CREATE (n:FirstNode)");
            beforeCommit.countDown();
            afterReset.await();

            // session has been reset, it should not be possible to commit the transaction
            try {
                tx1.commit();
            } catch (Neo4jException ignore) {
            }

            try (Transaction tx2 = session.beginTransaction()) {
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

    private void testResetOfQueryWaitingForLock(NodeIdUpdater nodeIdUpdater) throws Exception {
        int nodeId = 42;
        int newNodeId1 = 4242;
        int newNodeId2 = 424242;

        createNodeWithId(nodeId);

        CountDownLatch nodeLocked = new CountDownLatch(1);
        AtomicReference<InternalSession> otherSessionRef = new AtomicReference<>();

        try (InternalSession session = (InternalSession) neo4j.driver().session();
                Transaction tx = session.beginTransaction()) {
            Future<Void> txResult = nodeIdUpdater.update(nodeId, newNodeId1, otherSessionRef, nodeLocked);

            Result result = updateNodeId(tx, nodeId, newNodeId2);
            result.consume();

            nodeLocked.countDown();
            // give separate thread some time to block on a lock
            Thread.sleep(2_000);
            otherSessionRef.get().reset();

            assertTransactionTerminated(txResult);
            tx.commit();
        }

        try (Session session = neo4j.driver().session()) {
            Result result = session.run("MATCH (n) RETURN n.id AS id");
            int value = result.single().get("id").asInt();
            assertEquals(newNodeId2, value);
        }
    }

    private void createNodeWithId(int id) {
        try (Session session = neo4j.driver().session()) {
            session.run("CREATE (n {id: $id})", parameters("id", id));
        }
    }

    private static Result updateNodeId(QueryRunner queryRunner, int currentId, int newId) {
        return queryRunner.run(
                "MATCH (n {id: $currentId}) SET n.id = $newId", parameters("currentId", currentId, "newId", newId));
    }

    private static void assertTransactionTerminated(Future<Void> work) {
        ExecutionException e = assertThrows(ExecutionException.class, () -> work.get(20, TimeUnit.SECONDS));
        assertThat(e.getCause(), instanceOf(ClientException.class));
        assertThat(e.getCause().getMessage(), startsWith("The transaction has been terminated"));
    }

    private void testRandomQueryTermination(boolean autoCommit) throws InterruptedException {
        Set<InternalSession> runningSessions = newSetFromMap(new ConcurrentHashMap<>());
        AtomicBoolean stop = new AtomicBoolean();
        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < STRESS_TEST_THREAD_COUNT; i++) {
            futures.add(executor.submit(() -> {
                ThreadLocalRandom random = ThreadLocalRandom.current();
                while (!stop.get()) {
                    runRandomQuery(autoCommit, random, runningSessions, stop);
                }
            }));
        }

        long deadline = System.currentTimeMillis() + STRESS_TEST_DURATION_MS;
        while (!stop.get()) {
            if (System.currentTimeMillis() > deadline) {
                stop.set(true);
            }

            resetAny(runningSessions);

            MILLISECONDS.sleep(30);
        }

        awaitAllFutures(futures);
        awaitNoActiveQueries();
    }

    private void runRandomQuery(
            boolean autoCommit, Random random, Set<InternalSession> runningSessions, AtomicBoolean stop) {
        try {
            InternalSession session = (InternalSession) neo4j.driver().session();
            runningSessions.add(session);
            try {
                String query = STRESS_TEST_QUERIES[random.nextInt(STRESS_TEST_QUERIES.length - 1)];
                runQuery(session, query, autoCommit);
            } finally {
                runningSessions.remove(session);
                session.close();
            }
        } catch (Throwable error) {
            if (!stop.get() && !isAcceptable(error)) {
                stop.set(true);
                throw error;
            }
            // else it is fine to receive some errors from the driver because
            // sessions are being reset concurrently by the main thread, driver can also be closed concurrently
        }
    }

    private void testQueryTermination(String query, boolean autoCommit) {
        Future<Void> queryResult = runQueryInDifferentThreadAndResetSession(query, autoCommit);
        ExecutionException e = assertThrows(ExecutionException.class, () -> queryResult.get(10, SECONDS));
        assertThat(e.getCause(), instanceOf(Neo4jException.class));
        awaitNoActiveQueries();
    }

    private Future<Void> runQueryInDifferentThreadAndResetSession(String query, boolean autoCommit) {
        AtomicReference<InternalSession> sessionRef = new AtomicReference<>();

        Future<Void> queryResult = runAsync(() -> {
            InternalSession session = (InternalSession) neo4j.driver().session();
            sessionRef.set(session);
            runQuery(session, query, autoCommit);
        });

        awaitActiveQueriesToContain(query);

        InternalSession session = sessionRef.get();
        assertNotNull(session);
        session.reset();

        return queryResult;
    }

    private static void runQuery(Session session, String query, boolean autoCommit) {
        if (autoCommit) {
            session.run(query).consume();
        } else {
            try (Transaction tx = session.beginTransaction()) {
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

    private long countNodes(String label) {
        try (Session session = neo4j.driver().session()) {
            Result result =
                    session.run("MATCH (n" + (label == null ? "" : ":" + label) + ") RETURN count(n) AS result");
            return result.single().get(0).asLong();
        }
    }

    private static void resetAny(Set<InternalSession> sessions) {
        sessions.stream().findAny().ifPresent(session -> {
            if (sessions.remove(session)) {
                resetSafely(session);
            }
        });
    }

    private static void resetSafely(InternalSession session) {
        try {
            if (session.isOpen()) {
                session.reset();
            }
        } catch (ClientException e) {
            if (session.isOpen()) {
                throw e;
            }
            // else this thread lost race with close and it's fine
        }
    }

    private static boolean isAcceptable(Throwable error) {
        // get the root cause
        while (error.getCause() != null) {
            error = error.getCause();
        }

        return isTransactionTerminatedException(error)
                || error instanceof ServiceUnavailableException
                || error instanceof ClientException
                || error instanceof ClosedChannelException;
    }

    private static boolean isTransactionTerminatedException(Throwable error) {
        return error instanceof TransientException
                        && error.getMessage().startsWith("The transaction has been terminated")
                || error.getMessage().startsWith("Trying to execute query in a terminated transaction");
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

    private static void awaitAllFutures(List<Future<?>> futures) {
        for (Future<?> future : futures) {
            await(future);
        }
    }

    private static void awaitCondition(BooleanSupplier condition) {
        awaitCondition(condition, MINUTES.toMillis(2));
    }

    private static void awaitCondition(BooleanSupplier condition, long value) {
        long deadline = System.currentTimeMillis() + TimeUnit.MILLISECONDS.toMillis(value);
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
        try (Session session = driver.session()) {
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
