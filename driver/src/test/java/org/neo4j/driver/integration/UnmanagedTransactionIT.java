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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.testutil.TestUtil.await;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.bolt.connection.TelemetryApi;
import org.neo4j.driver.NotificationConfig;
import org.neo4j.driver.Query;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.TransactionTerminatedException;
import org.neo4j.driver.internal.InternalDriver;
import org.neo4j.driver.internal.async.NetworkSession;
import org.neo4j.driver.internal.async.UnmanagedTransaction;
import org.neo4j.driver.internal.telemetry.ApiTelemetryWork;
import org.neo4j.driver.testutil.DatabaseExtension;
import org.neo4j.driver.testutil.ParallelizableIT;

@ParallelizableIT
class UnmanagedTransactionIT {
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    private NetworkSession session;

    @BeforeEach
    @SuppressWarnings("resource")
    void setUp() {
        session = ((InternalDriver) neo4j.driver())
                .newSession(SessionConfig.defaultConfig(), NotificationConfig.defaultConfig(), null);
    }

    @AfterEach
    void tearDown() {
        session.closeAsync();
    }

    private UnmanagedTransaction beginTransaction() {
        return beginTransaction(session);
    }

    private UnmanagedTransaction beginTransaction(NetworkSession session) {
        var apiTelemetryWork = new ApiTelemetryWork(TelemetryApi.UNMANAGED_TRANSACTION);
        return await(session.beginTransactionAsync(TransactionConfig.empty(), apiTelemetryWork));
    }

    private ResultCursor sessionRun(NetworkSession session, Query query) {
        return await(session.runAsync(query, TransactionConfig.empty()));
    }

    private void txRun(UnmanagedTransaction tx, String query) {
        await(tx.runAsync(new Query(query)));
    }

    @Test
    void shouldDoNothingWhenCommittedSecondTime() {
        var tx = beginTransaction();

        assertNull(await(tx.commitAsync()));

        assertTrue(tx.commitAsync().toCompletableFuture().isDone());
        assertFalse(tx.isOpen());
    }

    @Test
    void shouldFailToCommitAfterRollback() {
        var tx = beginTransaction();

        assertNull(await(tx.rollbackAsync()));

        var e = assertThrows(ClientException.class, () -> await(tx.commitAsync()));
        assertEquals("Can't commit, transaction has been rolled back", e.getMessage());
        assertFalse(tx.isOpen());
    }

    @Test
    void shouldFailToCommitAfterTermination() {
        var tx = beginTransaction();

        tx.markTerminated(null);

        var e = assertThrows(TransactionTerminatedException.class, () -> await(tx.commitAsync()));
        assertThat(e.getMessage(), startsWith("Transaction can't be committed"));
    }

    @Test
    void shouldDoNothingWhenRolledBackSecondTime() {
        var tx = beginTransaction();

        assertNull(await(tx.rollbackAsync()));

        assertTrue(tx.rollbackAsync().toCompletableFuture().isDone());
        assertFalse(tx.isOpen());
    }

    @Test
    void shouldFailToRollbackAfterCommit() {
        var tx = beginTransaction();

        assertNull(await(tx.commitAsync()));

        var e = assertThrows(ClientException.class, () -> await(tx.rollbackAsync()));
        assertEquals("Can't rollback, transaction has been committed", e.getMessage());
        assertFalse(tx.isOpen());
    }

    @Test
    void shouldRollbackAfterTermination() {
        var tx = beginTransaction();

        tx.markTerminated(null);

        assertNull(await(tx.rollbackAsync()));
        assertFalse(tx.isOpen());
    }

    @Test
    void shouldFailToRunQueryWhenTerminated() {
        var tx = beginTransaction();
        txRun(tx, "CREATE (:MyLabel)");
        var terminationException = mock(Neo4jException.class);
        tx.markTerminated(terminationException);

        var e = assertThrows(TransactionTerminatedException.class, () -> txRun(tx, "CREATE (:MyOtherLabel)"));
        assertThat(e.getMessage(), startsWith("Cannot run more queries in this transaction"));
        assertEquals(e.getCause(), terminationException);
    }

    @Test
    void shouldBePossibleToRunMoreTransactionsAfterOneIsTerminated() {
        var tx1 = beginTransaction();
        tx1.markTerminated(null);
        var apiTelemetryWork = new ApiTelemetryWork(TelemetryApi.UNMANAGED_TRANSACTION);

        // commit should fail, make session forget about this transaction and release the connection to the pool
        var e = assertThrows(TransactionTerminatedException.class, () -> await(tx1.commitAsync()));
        assertThat(e.getMessage(), startsWith("Transaction can't be committed"));

        await(session.beginTransactionAsync(TransactionConfig.empty(), apiTelemetryWork)
                .thenCompose(tx -> tx.runAsync(new Query("CREATE (:Node {id: 42})"))
                        .thenCompose(ResultCursor::consumeAsync)
                        .thenApply(ignore -> tx))
                .thenCompose(UnmanagedTransaction::commitAsync));

        assertEquals(1, countNodesWithId(42));
    }

    @SuppressWarnings("SameParameterValue")
    private int countNodesWithId(Object id) {
        var query = new Query("MATCH (n:Node {id: $id}) RETURN count(n)", parameters("id", id));
        var cursor = sessionRun(session, query);
        return await(cursor.singleAsync()).get(0).asInt();
    }
}
