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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.testutil.TestUtil.await;
import static org.neo4j.driver.testutil.TestUtil.connectionMock;
import static org.neo4j.driver.testutil.TestUtil.newSession;
import static org.neo4j.driver.testutil.TestUtil.setupConnectionAnswers;
import static org.neo4j.driver.testutil.TestUtil.verifyCommitTx;
import static org.neo4j.driver.testutil.TestUtil.verifyRollbackTx;
import static org.neo4j.driver.testutil.TestUtil.verifyRunAndPull;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.stubbing.Answer;
import org.neo4j.driver.Query;
import org.neo4j.driver.Value;
import org.neo4j.driver.async.AsyncTransaction;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.bolt.api.BoltConnection;
import org.neo4j.driver.internal.bolt.api.BoltConnectionProvider;
import org.neo4j.driver.internal.bolt.api.DatabaseName;
import org.neo4j.driver.internal.bolt.api.summary.BeginSummary;
import org.neo4j.driver.internal.bolt.api.summary.CommitSummary;
import org.neo4j.driver.internal.bolt.api.summary.PullSummary;
import org.neo4j.driver.internal.bolt.api.summary.RollbackSummary;
import org.neo4j.driver.internal.bolt.api.summary.RunSummary;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v4.BoltProtocolV4;
import org.neo4j.driver.internal.value.IntegerValue;

class InternalAsyncTransactionTest {
    private BoltConnection connection;
    private InternalAsyncSession session;

    @BeforeEach
    void setUp() {
        connection = connectionMock(BoltProtocolV4.INSTANCE.version());
        var connectionProvider = mock(BoltConnectionProvider.class);
        given(connectionProvider.connect(any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .willAnswer((Answer<CompletionStage<BoltConnection>>) invocation -> {
                    var database = (DatabaseName) invocation.getArguments()[1];
                    @SuppressWarnings("unchecked")
                    var databaseConsumer = (Consumer<DatabaseName>) invocation.getArguments()[8];
                    databaseConsumer.accept(database);
                    return completedFuture(connection);
                });
        var networkSession = newSession(connectionProvider);
        session = new InternalAsyncSession(networkSession);
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
        given(connection.beginTransaction(any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .willReturn(completedFuture(connection));
        given(connection.run(any(), any())).willAnswer((Answer<CompletionStage<BoltConnection>>)
                invocation -> CompletableFuture.completedStage(connection));
        given(connection.pull(anyLong(), anyLong())).willAnswer((Answer<CompletionStage<BoltConnection>>)
                invocation -> CompletableFuture.completedStage(connection));
        setupConnectionAnswers(
                connection,
                List.of(
                        handler -> {
                            handler.onBeginSummary(mock(BeginSummary.class));
                            handler.onComplete();
                        },
                        handler -> {
                            handler.onRunSummary(mock(RunSummary.class));
                            handler.onPullSummary(mock(PullSummary.class));
                            handler.onComplete();
                        }));
        var tx = (InternalAsyncTransaction) await(session.beginTransactionAsync());

        var result = await(runReturnOne.apply(tx));
        var summary = await(result.consumeAsync());

        verifyRunAndPull(connection, summary.query().text());
    }

    @Test
    void shouldCommit() {
        given(connection.beginTransaction(any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .willReturn(completedFuture(connection));
        given(connection.commit()).willAnswer((Answer<CompletionStage<BoltConnection>>)
                invocation -> CompletableFuture.completedStage(connection));
        setupConnectionAnswers(
                connection,
                List.of(
                        handler -> {
                            handler.onBeginSummary(mock(BeginSummary.class));
                            handler.onComplete();
                        },
                        handler -> {
                            handler.onCommitSummary(mock(CommitSummary.class));
                            handler.onComplete();
                        }));
        given(connection.close()).willReturn(CompletableFuture.completedStage(null));
        var tx = (InternalAsyncTransaction) await(session.beginTransactionAsync());

        await(tx.commitAsync());

        verifyCommitTx(connection);
        verify(connection).close();
        assertFalse(tx.isOpen());
    }

    @Test
    void shouldRollback() {
        given(connection.beginTransaction(any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .willReturn(completedFuture(connection));
        given(connection.rollback()).willAnswer((Answer<CompletionStage<BoltConnection>>)
                invocation -> CompletableFuture.completedStage(connection));
        setupConnectionAnswers(
                connection,
                List.of(
                        handler -> {
                            handler.onBeginSummary(mock(BeginSummary.class));
                            handler.onComplete();
                        },
                        handler -> {
                            handler.onRollbackSummary(mock(RollbackSummary.class));
                            handler.onComplete();
                        }));
        given(connection.close()).willReturn(CompletableFuture.completedStage(null));
        var tx = (InternalAsyncTransaction) await(session.beginTransactionAsync());
        await(tx.rollbackAsync());

        verifyRollbackTx(connection);
        verify(connection).close();
        assertFalse(tx.isOpen());
    }

    @Test
    void shouldReleaseConnectionWhenFailedToCommit() {
        given(connection.beginTransaction(any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .willReturn(completedFuture(connection));
        given(connection.commit()).willAnswer((Answer<CompletionStage<BoltConnection>>)
                invocation -> CompletableFuture.completedStage(connection));
        setupConnectionAnswers(
                connection,
                List.of(
                        handler -> {
                            handler.onBeginSummary(mock(BeginSummary.class));
                            handler.onComplete();
                        },
                        handler -> {
                            handler.onError(new ServiceUnavailableException(""));
                            handler.onComplete();
                        }));
        given(connection.close()).willReturn(CompletableFuture.completedStage(null));
        var tx = (InternalAsyncTransaction) await(session.beginTransactionAsync());
        assertThrows(Exception.class, () -> await(tx.commitAsync()));

        verify(connection).close();
        assertFalse(tx.isOpen());
    }

    @Test
    void shouldReleaseConnectionWhenFailedToRollback() {
        given(connection.beginTransaction(any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .willReturn(completedFuture(connection));
        given(connection.rollback()).willAnswer((Answer<CompletionStage<BoltConnection>>)
                invocation -> CompletableFuture.completedStage(connection));
        setupConnectionAnswers(
                connection,
                List.of(
                        handler -> {
                            handler.onBeginSummary(mock(BeginSummary.class));
                            handler.onComplete();
                        },
                        handler -> {
                            handler.onError(new ServiceUnavailableException(""));
                            handler.onComplete();
                        }));
        given(connection.close()).willReturn(CompletableFuture.completedStage(null));
        var tx = (InternalAsyncTransaction) await(session.beginTransactionAsync());
        assertThrows(Exception.class, () -> await(tx.rollbackAsync()));

        verify(connection).close();
        assertFalse(tx.isOpen());
    }

    @Test
    void shouldDelegateIsOpenAsync() throws ExecutionException, InterruptedException {
        // GIVEN
        var utx = mock(UnmanagedTransaction.class);
        var expected = false;
        given(utx.isOpen()).willReturn(expected);
        var tx = new InternalAsyncTransaction(utx);

        // WHEN
        boolean actual = tx.isOpenAsync().toCompletableFuture().get();

        // THEN
        assertEquals(expected, actual);
        then(utx).should().isOpen();
    }
}
