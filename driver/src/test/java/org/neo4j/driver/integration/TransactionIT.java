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
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.testutil.TestUtil.assertNoCircularReferences;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.driver.Config;
import org.neo4j.driver.Record;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.TransactionTerminatedException;
import org.neo4j.driver.internal.InternalTransaction;
import org.neo4j.driver.testutil.ParallelizableIT;
import org.neo4j.driver.testutil.SessionExtension;
import org.neo4j.driver.testutil.TestUtil;

@ParallelizableIT
class TransactionIT {
    @RegisterExtension
    static final SessionExtension session = new SessionExtension();

    @Test
    void shouldAllowRunRollbackAndClose() {
        shouldRunAndCloseAfterAction(Transaction::rollback, false);
    }

    @Test
    void shouldAllowRunCommitAndClose() {
        shouldRunAndCloseAfterAction(Transaction::commit, true);
    }

    @Test
    void shouldAllowRunCloseAndClose() {
        shouldRunAndCloseAfterAction(Transaction::close, false);
    }

    @Test
    void shouldRunAndRollbackByDefault() {
        // When
        try (var tx = session.beginTransaction()) {
            tx.run("CREATE (n:FirstNode)");
            tx.run("CREATE (n:SecondNode)");
        }

        // Then there should be no visible effect of the transaction
        var cursor = session.run("MATCH (n) RETURN count(n)");
        var nodes = cursor.single().get("count(n)").asLong();
        assertThat(nodes, equalTo(0L));
    }

    @Test
    void shouldRetrieveResults() {
        // Given
        session.run("CREATE (n {name:'Steve Brook'})");

        // When
        try (var tx = session.beginTransaction()) {
            var res = tx.run("MATCH (n) RETURN n.name");

            // Then
            assertThat(res.single().get("n.name").asString(), equalTo("Steve Brook"));
        }
    }

    @Test
    void shouldNotAllowSessionLevelQueriesWhenThereIsATransaction() {
        session.beginTransaction();

        assertThrows(ClientException.class, () -> session.run("anything"));
    }

    @Test
    void shouldFailToRunQueryAfterTxIsCommitted() {
        shouldFailToRunQueryAfterTxAction(Transaction::commit);
    }

    @Test
    void shouldFailToRunQueryAfterTxIsRolledBack() {
        shouldFailToRunQueryAfterTxAction(Transaction::rollback);
    }

    @Test
    void shouldFailToRunQueryAfterTxIsClosed() {
        shouldFailToRunQueryAfterTxAction(Transaction::close);
    }

    @Test
    void shouldFailToCommitAfterRolledBack() {
        var tx = session.beginTransaction();
        tx.run("CREATE (:MyLabel)");
        tx.rollback();

        var e = assertThrows(ClientException.class, tx::commit);
        assertThat(e.getMessage(), startsWith("Can't commit, transaction has been rolled back"));
    }

    @Test
    void shouldFailToRollbackAfterTxIsCommitted() {
        var tx = session.beginTransaction();
        tx.run("CREATE (:MyLabel)");
        tx.commit();

        var e = assertThrows(ClientException.class, tx::rollback);
        assertThat(e.getMessage(), startsWith("Can't rollback, transaction has been committed"));
    }

    @Test
    void shouldFailToCommitAfterCommit() {
        var tx = session.beginTransaction();
        tx.run("CREATE (:MyLabel)");
        tx.commit();

        var e = assertThrows(ClientException.class, tx::commit);
        assertThat(e.getMessage(), startsWith("Can't commit, transaction has been committed"));
    }

    @Test
    void shouldFailToRollbackAfterRollback() {
        var tx = session.beginTransaction();
        tx.run("CREATE (:MyLabel)");
        tx.rollback();

        var e = assertThrows(ClientException.class, tx::rollback);
        assertThat(e.getMessage(), startsWith("Can't rollback, transaction has been rolled back"));
    }

    @Test
    void shouldBeClosedAfterClose() {
        shouldBeClosedAfterAction(Transaction::close);
    }

    @Test
    void shouldBeClosedAfterRollback() {
        shouldBeClosedAfterAction(Transaction::rollback);
    }

    @Test
    void shouldBeClosedAfterCommit() {
        shouldBeClosedAfterAction(Transaction::commit);
    }

    @Test
    void shouldBeOpenBeforeCommit() {
        // When
        var tx = session.beginTransaction();

        // Then
        assertTrue(tx.isOpen());
    }

    @Test
    void shouldHandleNullParametersGracefully() {
        // When
        session.run("match (n) return count(n)", (Value) null);

        // Then
        // pass - no exception thrown

    }

    // See GH #146
    @Test
    void shouldHandleFailureAfterClosingTransaction() {
        // GIVEN a successful query in a transaction
        var tx = session.beginTransaction();
        var result = tx.run("CREATE (n) RETURN n");
        result.consume();
        tx.commit();
        tx.close();

        // WHEN when running a malformed query in the original session
        assertThrows(
                ClientException.class, () -> session.run("CREAT (n) RETURN n").consume());
    }

    @SuppressWarnings("ConstantValue")
    @Test
    void shouldHandleNullRecordParameters() {
        // When
        try (var tx = session.beginTransaction()) {
            Record params = null;
            tx.run("CREATE (n:FirstNode)", params);
            tx.commit();
        }

        // Then it wasn't the end of the world as we know it
    }

    @SuppressWarnings("ConstantValue")
    @Test
    void shouldHandleNullValueParameters() {
        // When
        try (var tx = session.beginTransaction()) {
            Value params = null;
            tx.run("CREATE (n:FirstNode)", params);
            tx.commit();
        }

        // Then it wasn't the end of the world as we know it
    }

    @SuppressWarnings("ConstantValue")
    @Test
    void shouldHandleNullMapParameters() {
        // When
        try (var tx = session.beginTransaction()) {
            Map<String, Object> params = null;
            tx.run("CREATE (n:FirstNode)", params);
            tx.commit();
        }

        // Then it wasn't the end of the world as we know it
    }

    @Test
    void shouldRollbackTransactionAfterFailedRunAndCommitAndSessionShouldSuccessfullyBeginNewTransaction() {
        // Given
        var tx = session.beginTransaction();

        assertThrows(ClientException.class, () -> tx.run("invalid")); // send run, pull_all
        var e = assertThrows(ClientException.class, tx::commit);
        assertNoCircularReferences(e);
        try (var anotherTx = session.beginTransaction()) {
            var cursor = anotherTx.run("RETURN 1");
            var val = cursor.single().get("1").asInt();
            assertThat(val, equalTo(1));
        }
    }

    @Test
    void shouldRollBackTxIfErrorWithConsume() {
        assertThrows(ClientException.class, () -> {
            try (var tx = session.beginTransaction()) {
                var result = tx.run("invalid");
                result.consume();
            }
        });

        try (var tx = session.beginTransaction()) {
            var cursor = tx.run("RETURN 1");
            var val = cursor.single().get("1").asInt();
            assertThat(val, equalTo(1));
        }
    }

    @Test
    void shouldFailRun() {
        try (var tx = session.beginTransaction()) {
            var e = assertThrows(ClientException.class, () -> tx.run("RETURN Wrong"));

            assertThat(e.code(), containsString("SyntaxError"));
        }
    }

    @Test
    @SuppressWarnings("ResultOfMethodCallIgnored")
    void shouldBeResponsiveToThreadInterruptWhenWaitingForResult() {
        try (var otherSession = session.driver().session()) {
            session.run("CREATE (:Person {name: 'Beta Ray Bill'})").consume();

            var tx1 = session.beginTransaction();
            var tx2 = otherSession.beginTransaction();
            tx1.run("MATCH (n:Person {name: 'Beta Ray Bill'}) SET n.hammer = 'Mjolnir'")
                    .consume();

            // now 'Beta Ray Bill' node is locked

            // setup other thread to interrupt current thread when it blocks
            TestUtil.interruptWhenInWaitingState(Thread.currentThread());

            try {
                var e = assertThrows(ServiceUnavailableException.class, () -> tx2.run(
                                "MATCH (n:Person {name: 'Beta Ray Bill'}) SET n.hammer = 'Stormbreaker'")
                        .consume());
                assertThat(e.getMessage(), containsString("Connection to the database terminated"));
                assertThat(e.getMessage(), containsString("Thread interrupted while running query in transaction"));
            } finally {
                // clear interrupted flag
                Thread.interrupted();
            }
        }
    }

    @Test
    @SuppressWarnings("ResultOfMethodCallIgnored")
    void shouldBeResponsiveToThreadInterruptWhenWaitingForCommit() {
        try (var otherSession = session.driver().session()) {
            session.run("CREATE (:Person {name: 'Beta Ray Bill'})").consume();

            var tx1 = session.beginTransaction();
            var tx2 = otherSession.beginTransaction();
            tx1.run("MATCH (n:Person {name: 'Beta Ray Bill'}) SET n.hammer = 'Mjolnir'")
                    .consume();

            // now 'Beta Ray Bill' node is locked

            // setup other thread to interrupt current thread when it blocks
            TestUtil.interruptWhenInWaitingState(Thread.currentThread());

            try {
                assertThrows(
                        ServiceUnavailableException.class,
                        () -> tx2.run("MATCH (n:Person {name: 'Beta Ray Bill'}) SET n.hammer = 'Stormbreaker'"));
            } finally {
                // clear interrupted flag
                Thread.interrupted();
            }
        }
    }

    @Test
    void shouldFailToCommitAfterFailure() {
        try (var tx = session.beginTransaction()) {
            var xs = tx.run("UNWIND [1,2,3] AS x CREATE (:Node) RETURN x")
                    .list(record -> record.get(0).asInt());
            assertEquals(asList(1, 2, 3), xs);

            var error1 = assertThrows(
                    ClientException.class, () -> tx.run("RETURN unknown").consume());
            assertThat(error1.code(), containsString("SyntaxError"));

            var error2 = assertThrows(ClientException.class, tx::commit);
            assertThat(error2.getMessage(), startsWith("Transaction can't be committed. It has been rolled back"));
        }
    }

    @Test
    void shouldDisallowQueriesAfterFailureWhenResultsAreConsumed() {
        try (var tx = session.beginTransaction()) {
            var xs = tx.run("UNWIND [1,2,3] AS x CREATE (:Node) RETURN x")
                    .list(record -> record.get(0).asInt());
            assertEquals(asList(1, 2, 3), xs);

            var error1 = assertThrows(
                    ClientException.class, () -> tx.run("RETURN unknown").consume());
            assertThat(error1.code(), containsString("SyntaxError"));

            var error2 = assertThrows(
                    ClientException.class, () -> tx.run("CREATE (:OtherNode)").consume());
            assertThat(error2.getMessage(), startsWith("Cannot run more queries in this transaction"));

            var error3 = assertThrows(
                    ClientException.class, () -> tx.run("RETURN 42").consume());
            assertThat(error3.getMessage(), startsWith("Cannot run more queries in this transaction"));
        }

        assertEquals(0, countNodesByLabel("Node"));
        assertEquals(0, countNodesByLabel("OtherNode"));
    }

    @Test
    void shouldRollbackWhenOneOfQueriesFails() {
        var error = assertThrows(ClientException.class, () -> {
            try (var tx = session.beginTransaction()) {
                tx.run("CREATE (:Node1)");
                tx.run("CREATE (:Node2)");
                tx.run("CREATE SmthStrange");
            }
        });

        assertThat(error.code(), containsString("SyntaxError"));

        assertEquals(0, countNodesByLabel("Node1"));
        assertEquals(0, countNodesByLabel("Node2"));
        assertEquals(0, countNodesByLabel("Node3"));
        assertEquals(0, countNodesByLabel("Node4"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldPreventPullAfterTransactionTermination(boolean iterate) {
        // Given
        var tx = session.beginTransaction();
        var streamSize = Config.defaultConfig().fetchSize() + 1;
        var result0 = tx.run("UNWIND range(1, $limit) AS x RETURN x", Map.of("limit", streamSize));
        var result1 = tx.run("UNWIND range(1, $limit) AS x RETURN x", Map.of("limit", streamSize));

        // When
        var terminationException = assertThrows(ClientException.class, () -> tx.run("invalid"));
        assertEquals(terminationException.code(), "Neo.ClientError.Statement.SyntaxError");

        // Then
        for (var result : List.of(result0, result1)) {
            var exception = assertThrows(ClientException.class, () -> {
                if (iterate) {
                    LongStream.range(0, streamSize).forEach(ignored -> result.next());
                } else {
                    result.list();
                }
            });
            assertEquals(terminationException, exception);
        }
        tx.close();
    }

    @Test
    void shouldPreventDiscardAfterTransactionTermination() {
        // Given
        var tx = session.beginTransaction();
        var streamSize = Config.defaultConfig().fetchSize() + 1;
        var result0 = tx.run("UNWIND range(1, $limit) AS x RETURN x", Map.of("limit", streamSize));
        var result1 = tx.run("UNWIND range(1, $limit) AS x RETURN x", Map.of("limit", streamSize));

        // When
        var terminationException = assertThrows(ClientException.class, () -> tx.run("invalid"));
        assertEquals(terminationException.code(), "Neo.ClientError.Statement.SyntaxError");

        // Then
        for (var result : List.of(result0, result1)) {
            var exception = assertThrows(ClientException.class, result::consume);
            assertEquals(terminationException, exception);
        }
        tx.close();
    }

    @Test
    void shouldPreventRunAfterTransactionTermination() {
        // Given
        var tx = session.beginTransaction();
        var terminationException = assertThrows(ClientException.class, () -> tx.run("invalid"));
        assertEquals(terminationException.code(), "Neo.ClientError.Statement.SyntaxError");

        // When
        var exception = assertThrows(TransactionTerminatedException.class, () -> tx.run("RETURN 1"));

        // Then
        assertEquals(terminationException, exception.getCause());
        tx.close();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldPreventPullAfterDriverTransactionTermination(boolean iterate) {
        // Given
        var tx = (InternalTransaction) session.beginTransaction();
        var streamSize = Config.defaultConfig().fetchSize() + 1;
        var result0 = tx.run("UNWIND range(1, $limit) AS x RETURN x", Map.of("limit", streamSize));
        var result1 = tx.run("UNWIND range(1, $limit) AS x RETURN x", Map.of("limit", streamSize));

        // When
        tx.terminate();

        // Then
        for (var result : List.of(result0, result1)) {
            assertThrows(TransactionTerminatedException.class, () -> {
                if (iterate) {
                    LongStream.range(0, streamSize).forEach(ignored -> result.next());
                } else {
                    result.list();
                }
            });
        }
        tx.close();
    }

    @Test
    void shouldPreventDiscardAfterDriverTransactionTermination() {
        // Given
        var tx = (InternalTransaction) session.beginTransaction();
        var streamSize = Config.defaultConfig().fetchSize() + 1;
        var result0 = tx.run("UNWIND range(1, $limit) AS x RETURN x", Map.of("limit", streamSize));
        var result1 = tx.run("UNWIND range(1, $limit) AS x RETURN x", Map.of("limit", streamSize));

        // When
        tx.terminate();

        // Then
        for (var result : List.of(result0, result1)) {
            assertThrows(TransactionTerminatedException.class, result::consume);
        }
        tx.close();
    }

    @Test
    void shouldPreventRunAfterDriverTransactionTermination() {
        // Given
        var tx = (InternalTransaction) session.beginTransaction();
        var streamSize = Config.defaultConfig().fetchSize() + 1;
        var result = tx.run("UNWIND range(1, $limit) AS x RETURN x", Map.of("limit", streamSize));
        result.next();

        // When
        tx.terminate();

        // Then
        assertThrows(TransactionTerminatedException.class, () -> tx.run("UNWIND range(0, 5) AS x RETURN x"));
        // the result handle has the pending error
        assertThrows(TransactionTerminatedException.class, tx::close);
        // all errors have been surfaced
        tx.close();
    }

    @Test
    void shouldTerminateTransactionAndHandleFailureResponseOrPreventFurtherPulls() {
        // Given
        var tx = (InternalTransaction) session.beginTransaction();
        var streamSize = Config.defaultConfig().fetchSize() + 1;
        var result = tx.run("UNWIND range(1, $limit) AS x RETURN x", Map.of("limit", streamSize));

        // When
        tx.terminate();

        // Then
        assertThrows(TransactionTerminatedException.class, () -> LongStream.range(0, streamSize)
                .forEach(ignored -> assertNotNull(result.next())));
        tx.close();
    }

    private void shouldRunAndCloseAfterAction(Consumer<Transaction> txConsumer, boolean isCommit) {
        // When
        try (var tx = session.beginTransaction()) {
            tx.run("CREATE (n:FirstNode)");
            tx.run("CREATE (n:SecondNode)");
            txConsumer.accept(tx);
        }

        // Then the outcome of both queries should be visible
        var result = session.run("MATCH (n) RETURN count(n)");
        var nodes = result.single().get("count(n)").asLong();
        if (isCommit) {
            assertThat(nodes, equalTo(2L));
        } else {
            assertThat(nodes, equalTo(0L));
        }
    }

    private void shouldBeClosedAfterAction(Consumer<Transaction> txConsumer) {
        // When
        var tx = session.beginTransaction();
        txConsumer.accept(tx);

        // Then
        assertFalse(tx.isOpen());
    }

    private void shouldFailToRunQueryAfterTxAction(Consumer<Transaction> txConsumer) {
        var tx = session.beginTransaction();
        tx.run("CREATE (:MyLabel)");
        txConsumer.accept(tx);

        var e = assertThrows(ClientException.class, () -> tx.run("CREATE (:MyOtherLabel)"));
        assertThat(e.getMessage(), startsWith("Cannot run more queries in this transaction"));
    }

    private static int countNodesByLabel(String label) {
        return session.run("MATCH (n:" + label + ") RETURN count(n)")
                .single()
                .get(0)
                .asInt();
    }
}
