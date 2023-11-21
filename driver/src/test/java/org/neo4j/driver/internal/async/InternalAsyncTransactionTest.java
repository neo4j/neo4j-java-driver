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
package org.neo4j.driver.internal.async;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.testutil.TestUtil.await;
import static org.neo4j.driver.testutil.TestUtil.connectionMock;
import static org.neo4j.driver.testutil.TestUtil.newSession;
import static org.neo4j.driver.testutil.TestUtil.setupFailingCommit;
import static org.neo4j.driver.testutil.TestUtil.setupFailingRollback;
import static org.neo4j.driver.testutil.TestUtil.setupSuccessfulRunAndPull;
import static org.neo4j.driver.testutil.TestUtil.verifyCommitTx;
import static org.neo4j.driver.testutil.TestUtil.verifyRollbackTx;
import static org.neo4j.driver.testutil.TestUtil.verifyRunAndPull;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.driver.Query;
import org.neo4j.driver.Value;
import org.neo4j.driver.async.AsyncTransaction;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.internal.DatabaseNameUtil;
import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.messaging.v4.BoltProtocolV4;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionProvider;
import org.neo4j.driver.internal.value.IntegerValue;

class InternalAsyncTransactionTest {
    private static final String DATABASE = "neo4j";
    private Connection connection;
    private InternalAsyncTransaction tx;

    @BeforeEach
    void setUp() {
        connection = connectionMock(BoltProtocolV4.INSTANCE);
        var connectionProvider = mock(ConnectionProvider.class);
        when(connectionProvider.acquireConnection(any(ConnectionContext.class))).thenAnswer(invocation -> {
            var context = (ConnectionContext) invocation.getArgument(0);
            context.databaseNameFuture().complete(DatabaseNameUtil.database(DATABASE));
            return completedFuture(connection);
        });
        var networkSession = newSession(connectionProvider);
        var session = new InternalAsyncSession(networkSession);
        tx = (InternalAsyncTransaction) await(session.beginTransactionAsync());
    }

    private static Stream<Function<AsyncTransaction, CompletionStage<ResultCursor>>> allSessionRunMethods() {
        return Stream.of(
                tx -> tx.runAsync("RETURN 1"),
                tx -> tx.runAsync("RETURN $x", parameters("x", 1)),
                tx -> tx.runAsync("RETURN $x", singletonMap("x", 1)),
                tx -> tx.runAsync(
                        "RETURN $x", new InternalRecord(singletonList("x"), new Value[] {new IntegerValue(1)})),
                tx -> tx.runAsync(new Query("RETURN $x", parameters("x", 1))));
    }

    @ParameterizedTest
    @MethodSource("allSessionRunMethods")
    void shouldFlushOnRun(Function<AsyncTransaction, CompletionStage<ResultCursor>> runReturnOne) {
        setupSuccessfulRunAndPull(connection);

        var result = await(runReturnOne.apply(tx));
        var summary = await(result.consumeAsync());

        verifyRunAndPull(connection, summary.query().text());
    }

    @Test
    void shouldCommit() {
        await(tx.commitAsync());

        verifyCommitTx(connection);
        verify(connection).release();
        assertFalse(tx.isOpen());
    }

    @Test
    void shouldRollback() {
        await(tx.rollbackAsync());

        verifyRollbackTx(connection);
        verify(connection).release();
        assertFalse(tx.isOpen());
    }

    @Test
    void shouldReleaseConnectionWhenFailedToCommit() {
        setupFailingCommit(connection);
        assertThrows(Exception.class, () -> await(tx.commitAsync()));

        verify(connection).release();
        assertFalse(tx.isOpen());
    }

    @Test
    void shouldReleaseConnectionWhenFailedToRollback() {
        setupFailingRollback(connection);
        assertThrows(Exception.class, () -> await(tx.rollbackAsync()));

        verify(connection).release();
        assertFalse(tx.isOpen());
    }

    @Test
    void shouldDelegateIsOpenAsync() throws ExecutionException, InterruptedException {
        // GIVEN
        var utx = mock(UnmanagedTransaction.class);
        var expected = false;
        given(utx.isOpen()).willReturn(expected);
        tx = new InternalAsyncTransaction(utx);

        // WHEN
        boolean actual = tx.isOpenAsync().toCompletableFuture().get();

        // THEN
        assertEquals(expected, actual);
        then(utx).should().isOpen();
    }
}
