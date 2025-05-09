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
import org.neo4j.bolt.connection.BoltProtocolVersion;
import org.neo4j.bolt.connection.DatabaseName;
import org.neo4j.bolt.connection.message.BeginMessage;
import org.neo4j.bolt.connection.message.CommitMessage;
import org.neo4j.bolt.connection.message.Message;
import org.neo4j.bolt.connection.message.PullMessage;
import org.neo4j.bolt.connection.message.RollbackMessage;
import org.neo4j.bolt.connection.message.RunMessage;
import org.neo4j.bolt.connection.summary.BeginSummary;
import org.neo4j.bolt.connection.summary.CommitSummary;
import org.neo4j.bolt.connection.summary.RollbackSummary;
import org.neo4j.bolt.connection.summary.RunSummary;
import org.neo4j.driver.Query;
import org.neo4j.driver.Value;
import org.neo4j.driver.async.AsyncTransaction;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.adaptedbolt.DriverBoltConnection;
import org.neo4j.driver.internal.adaptedbolt.DriverBoltConnectionProvider;
import org.neo4j.driver.internal.adaptedbolt.DriverResponseHandler;
import org.neo4j.driver.internal.adaptedbolt.summary.PullSummary;
import org.neo4j.driver.internal.value.IntegerValue;
import org.neo4j.driver.testutil.TestUtil;

class InternalAsyncTransactionTest {
    private DriverBoltConnection connection;
    private InternalAsyncSession session;

    @BeforeEach
    void setUp() {
        connection = connectionMock(new BoltProtocolVersion(4, 0));
        var connectionProvider = mock(DriverBoltConnectionProvider.class);
        given(connectionProvider.connect(any(), any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .willAnswer((Answer<CompletionStage<DriverBoltConnection>>) invocation -> {
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
        setupConnectionAnswers(
                connection,
                List.of(
                        new TestUtil.MessageHandler() {
                            @Override
                            public List<Class<? extends Message>> messageTypes() {
                                return List.of(BeginMessage.class);
                            }

                            @Override
                            public void handle(DriverResponseHandler handler) {
                                handler.onBeginSummary(mock(BeginSummary.class));
                                handler.onComplete();
                            }
                        },
                        new TestUtil.MessageHandler() {
                            @Override
                            public List<Class<? extends Message>> messageTypes() {
                                return List.of(RunMessage.class, PullMessage.class);
                            }

                            @Override
                            public void handle(DriverResponseHandler handler) {
                                handler.onRunSummary(mock(RunSummary.class));
                                handler.onPullSummary(mock(PullSummary.class));
                                handler.onComplete();
                            }
                        }));
        var tx = (InternalAsyncTransaction) await(session.beginTransactionAsync());

        var result = await(runReturnOne.apply(tx));
        var summary = await(result.consumeAsync());

        verifyRunAndPull(connection, summary.query().text());
    }

    @Test
    void shouldCommit() {
        setupConnectionAnswers(
                connection,
                List.of(
                        new TestUtil.MessageHandler() {
                            @Override
                            public List<Class<? extends Message>> messageTypes() {
                                return List.of(BeginMessage.class);
                            }

                            @Override
                            public void handle(DriverResponseHandler handler) {
                                handler.onBeginSummary(mock(BeginSummary.class));
                                handler.onComplete();
                            }
                        },
                        new TestUtil.MessageHandler() {
                            @Override
                            public List<Class<? extends Message>> messageTypes() {
                                return List.of(CommitMessage.class);
                            }

                            @Override
                            public void handle(DriverResponseHandler handler) {
                                handler.onCommitSummary(mock(CommitSummary.class));
                                handler.onComplete();
                            }
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
        setupConnectionAnswers(
                connection,
                List.of(
                        new TestUtil.MessageHandler() {
                            @Override
                            public List<Class<? extends Message>> messageTypes() {
                                return List.of(BeginMessage.class);
                            }

                            @Override
                            public void handle(DriverResponseHandler handler) {
                                handler.onBeginSummary(mock(BeginSummary.class));
                                handler.onComplete();
                            }
                        },
                        new TestUtil.MessageHandler() {
                            @Override
                            public List<Class<? extends Message>> messageTypes() {
                                return List.of(RollbackMessage.class);
                            }

                            @Override
                            public void handle(DriverResponseHandler handler) {
                                handler.onRollbackSummary(mock(RollbackSummary.class));
                                handler.onComplete();
                            }
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
        setupConnectionAnswers(
                connection,
                List.of(
                        new TestUtil.MessageHandler() {
                            @Override
                            public List<Class<? extends Message>> messageTypes() {
                                return List.of(BeginMessage.class);
                            }

                            @Override
                            public void handle(DriverResponseHandler handler) {
                                handler.onBeginSummary(mock(BeginSummary.class));
                                handler.onComplete();
                            }
                        },
                        new TestUtil.MessageHandler() {
                            @Override
                            public List<Class<? extends Message>> messageTypes() {
                                return List.of(CommitMessage.class);
                            }

                            @Override
                            public void handle(DriverResponseHandler handler) {
                                handler.onError(new ServiceUnavailableException(""));
                                handler.onComplete();
                            }
                        }));
        given(connection.close()).willReturn(CompletableFuture.completedStage(null));
        var tx = (InternalAsyncTransaction) await(session.beginTransactionAsync());
        assertThrows(Exception.class, () -> await(tx.commitAsync()));

        verify(connection).close();
        assertFalse(tx.isOpen());
    }

    @Test
    void shouldReleaseConnectionWhenFailedToRollback() {
        setupConnectionAnswers(
                connection,
                List.of(
                        new TestUtil.MessageHandler() {
                            @Override
                            public List<Class<? extends Message>> messageTypes() {
                                return List.of(BeginMessage.class);
                            }

                            @Override
                            public void handle(DriverResponseHandler handler) {
                                handler.onBeginSummary(mock(BeginSummary.class));
                                handler.onComplete();
                            }
                        },
                        new TestUtil.MessageHandler() {
                            @Override
                            public List<Class<? extends Message>> messageTypes() {
                                return List.of(RollbackMessage.class);
                            }

                            @Override
                            public void handle(DriverResponseHandler handler) {
                                handler.onError(new ServiceUnavailableException(""));
                                handler.onComplete();
                            }
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
