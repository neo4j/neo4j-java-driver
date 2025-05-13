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
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.driver.AccessMode.READ;
import static org.neo4j.driver.AccessMode.WRITE;
import static org.neo4j.driver.TransactionConfig.empty;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.testutil.TestUtil.await;
import static org.neo4j.driver.testutil.TestUtil.connectionMock;
import static org.neo4j.driver.testutil.TestUtil.newSession;
import static org.neo4j.driver.testutil.TestUtil.setupConnectionAnswers;
import static org.neo4j.driver.testutil.TestUtil.setupSuccessfulAutocommitRunAndPull;
import static org.neo4j.driver.testutil.TestUtil.verifyAutocommitRunAndPull;
import static org.neo4j.driver.testutil.TestUtil.verifyBegin;
import static org.neo4j.driver.testutil.TestUtil.verifyCommitTx;
import static org.neo4j.driver.testutil.TestUtil.verifyRollbackTx;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentMatchers;
import org.mockito.stubbing.Answer;
import org.neo4j.bolt.connection.BoltProtocolVersion;
import org.neo4j.bolt.connection.DatabaseName;
import org.neo4j.bolt.connection.message.BeginMessage;
import org.neo4j.bolt.connection.message.CommitMessage;
import org.neo4j.bolt.connection.message.Message;
import org.neo4j.bolt.connection.message.RollbackMessage;
import org.neo4j.bolt.connection.summary.BeginSummary;
import org.neo4j.bolt.connection.summary.RollbackSummary;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Query;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.Value;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.async.AsyncTransaction;
import org.neo4j.driver.async.AsyncTransactionCallback;
import org.neo4j.driver.async.AsyncTransactionContext;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.adaptedbolt.DriverBoltConnection;
import org.neo4j.driver.internal.adaptedbolt.DriverBoltConnectionProvider;
import org.neo4j.driver.internal.adaptedbolt.DriverResponseHandler;
import org.neo4j.driver.internal.retry.RetryLogic;
import org.neo4j.driver.internal.util.FixedRetryLogic;
import org.neo4j.driver.internal.value.IntegerValue;
import org.neo4j.driver.testutil.TestUtil;

class InternalAsyncSessionTest {
    private DriverBoltConnection connection;
    private DriverBoltConnectionProvider connectionProvider;
    private AsyncSession asyncSession;
    private NetworkSession session;

    @BeforeEach
    void setUp() {
        connection = connectionMock(new BoltProtocolVersion(4, 0));
        given(connection.close()).willReturn(completedFuture(null));
        connectionProvider = mock(DriverBoltConnectionProvider.class);
        given(connectionProvider.connect(any(), any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .willAnswer((Answer<CompletionStage<DriverBoltConnection>>) invocation -> {
                    var database = (DatabaseName) invocation.getArguments()[1];
                    @SuppressWarnings("unchecked")
                    var databaseConsumer = (Consumer<DatabaseName>) invocation.getArguments()[8];
                    databaseConsumer.accept(database);
                    return completedFuture(connection);
                });
        session = newSession(connectionProvider);
        asyncSession = new InternalAsyncSession(session);
    }

    private static Stream<Function<AsyncSession, CompletionStage<ResultCursor>>> allSessionRunMethods() {
        return Stream.of(
                session -> session.runAsync("RETURN 1"),
                session -> session.runAsync("RETURN $x", parameters("x", 1)),
                session -> session.runAsync("RETURN $x", singletonMap("x", 1)),
                session -> session.runAsync(
                        "RETURN $x", new InternalRecord(singletonList("x"), new Value[] {new IntegerValue(1)})),
                session -> session.runAsync(new Query("RETURN $x", parameters("x", 1))),
                session -> session.runAsync(new Query("RETURN $x", parameters("x", 1)), empty()),
                session -> session.runAsync("RETURN $x", singletonMap("x", 1), empty()),
                session -> session.runAsync("RETURN 1", empty()));
    }

    private static Stream<Function<AsyncSession, CompletionStage<AsyncTransaction>>> allBeginTxMethods() {
        return Stream.of(
                AsyncSession::beginTransactionAsync,
                session -> session.beginTransactionAsync(TransactionConfig.empty()));
    }

    private static Stream<Function<AsyncSession, CompletionStage<String>>> allRunTxMethods() {
        return Stream.of(
                session -> session.executeReadAsync(tx -> completedFuture("a")),
                session -> session.executeWriteAsync(tx -> completedFuture("a")),
                session -> session.executeReadAsync(tx -> completedFuture("a"), empty()),
                session -> session.executeWriteAsync(tx -> completedFuture("a"), empty()));
    }

    @ParameterizedTest
    @MethodSource("allSessionRunMethods")
    void shouldFlushOnRun(Function<AsyncSession, CompletionStage<ResultCursor>> runReturnOne) {
        setupSuccessfulAutocommitRunAndPull(connection);

        var cursor = await(runReturnOne.apply(asyncSession));

        verifyAutocommitRunAndPull(
                connection, await(cursor.consumeAsync()).query().text());
    }

    @ParameterizedTest
    @MethodSource("allBeginTxMethods")
    void shouldDelegateBeginTx(Function<AsyncSession, CompletionStage<AsyncTransaction>> beginTx) {
        setupConnectionAnswers(connection, List.of(new TestUtil.MessageHandler() {
            @Override
            public List<Class<? extends Message>> messageTypes() {
                return List.of(BeginMessage.class);
            }

            @Override
            public void handle(DriverResponseHandler handler) {
                handler.onBeginSummary(mock(BeginSummary.class));
                handler.onComplete();
            }
        }));

        var tx = await(beginTx.apply(asyncSession));

        verifyBegin(connection);
        assertNotNull(tx);
    }

    @ParameterizedTest
    @MethodSource("allRunTxMethods")
    void txRunShouldBeginAndCommitTx(Function<AsyncSession, CompletionStage<String>> runTx) {
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
                                handler.onCommitSummary(Optional::empty);
                                handler.onComplete();
                            }
                        }));

        var string = await(runTx.apply(asyncSession));

        verifyBegin(connection);
        verifyCommitTx(connection);
        verify(connection).close();
        assertThat(string, equalTo("a"));
    }

    @Test
    void rollsBackReadTxWhenFunctionThrows() {
        testTxRollbackWhenThrows(READ);
    }

    @Test
    void rollsBackWriteTxWhenFunctionThrows() {
        testTxRollbackWhenThrows(WRITE);
    }

    @Test
    void readTxRetriedUntilSuccessWhenFunctionThrows() {
        testTxIsRetriedUntilSuccessWhenFunctionThrows(READ);
    }

    @Test
    void writeTxRetriedUntilSuccessWhenFunctionThrows() {
        testTxIsRetriedUntilSuccessWhenFunctionThrows(WRITE);
    }

    @Test
    void readTxRetriedUntilSuccessWhenTxCloseThrows() {
        testTxIsRetriedUntilSuccessWhenCommitThrows(READ);
    }

    @Test
    void writeTxRetriedUntilSuccessWhenTxCloseThrows() {
        testTxIsRetriedUntilSuccessWhenCommitThrows(WRITE);
    }

    @Test
    void readTxRetriedUntilFailureWhenFunctionThrows() {
        testTxIsRetriedUntilFailureWhenFunctionThrows(READ);
    }

    @Test
    void writeTxRetriedUntilFailureWhenFunctionThrows() {
        testTxIsRetriedUntilFailureWhenFunctionThrows(WRITE);
    }

    @Test
    void readTxRetriedUntilFailureWhenTxCloseThrows() {
        testTxIsRetriedUntilFailureWhenCommitFails(READ);
    }

    @Test
    void writeTxRetriedUntilFailureWhenTxCloseThrows() {
        testTxIsRetriedUntilFailureWhenCommitFails(WRITE);
    }

    @Test
    void shouldCloseSession() {
        await(asyncSession.closeAsync());
        assertFalse(this.session.isOpen());
    }

    @Test
    void shouldReturnBookmark() {
        session = newSession(connectionProvider, Collections.singleton(Bookmark.from("Bookmark1")));
        asyncSession = new InternalAsyncSession(session);

        assertThat(asyncSession.lastBookmarks(), equalTo(session.lastBookmarks()));
    }

    @ParameterizedTest
    @MethodSource("executeVariations")
    void shouldDelegateExecuteReadToRetryLogic(ExecuteVariation executeVariation)
            throws ExecutionException, InterruptedException {
        // GIVEN
        var networkSession = mock(NetworkSession.class);
        AsyncSession session = new InternalAsyncSession(networkSession);
        var logic = mock(RetryLogic.class);
        var expected = "";
        given(networkSession.retryLogic()).willReturn(logic);
        AsyncTransactionCallback<CompletionStage<String>> tc = (ignored) -> CompletableFuture.completedFuture(expected);
        given(logic.<String>retryAsync(any())).willReturn(tc.execute(null));
        var config = TransactionConfig.builder().build();

        // WHEN
        var actual = executeVariation.readOnly
                ? (executeVariation.explicitTxConfig
                        ? session.executeReadAsync(tc, config)
                        : session.executeReadAsync(tc))
                : (executeVariation.explicitTxConfig
                        ? session.executeWriteAsync(tc, config)
                        : session.executeWriteAsync(tc));

        // THEN
        assertEquals(expected, actual.toCompletableFuture().get());
        then(networkSession).should().retryLogic();
        then(logic).should().retryAsync(any());
    }

    @SuppressWarnings("deprecation")
    private void testTxRollbackWhenThrows(AccessMode transactionMode) {
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
        final RuntimeException error = new IllegalStateException("Oh!");
        AsyncTransactionCallback<CompletionStage<Void>> work = tx -> {
            throw error;
        };

        var e = assertThrows(Exception.class, () -> executeTransaction(asyncSession, transactionMode, work));
        assertEquals(error, e);

        verify(connectionProvider).connect(any(), any(), any(), any(), any(), any(), any(), any(), any(), any());
        verifyBegin(connection);
        verifyRollbackTx(connection);
    }

    private void testTxIsRetriedUntilSuccessWhenFunctionThrows(AccessMode mode) {
        var failures = 12;
        var handlers = List.of(
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
                },
                new TestUtil.MessageHandler() {
                    @Override
                    public List<Class<? extends Message>> messageTypes() {
                        return List.of(CommitMessage.class);
                    }

                    @Override
                    public void handle(DriverResponseHandler handler) {
                        handler.onCommitSummary(Optional::empty);
                        handler.onComplete();
                    }
                });
        var retries = failures + 1;
        setupConnectionAnswers(connection, handlers);

        RetryLogic retryLogic = new FixedRetryLogic(retries);
        session = newSession(connectionProvider, retryLogic);
        asyncSession = new InternalAsyncSession(session);

        var work = spy(new TxWork(failures, new SessionExpiredException("")));
        int answer = executeTransaction(asyncSession, mode, work);

        assertEquals(42, answer);
        verifyInvocationCount(work, failures + 1);
        verifyRollbackTx(connection, times(failures));
        verifyCommitTx(connection);
    }

    private void testTxIsRetriedUntilSuccessWhenCommitThrows(AccessMode mode) {
        var failures = 13;
        var handlers = List.of(
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
                    int expectedFailures = failures;

                    @Override
                    public List<Class<? extends Message>> messageTypes() {
                        return List.of(CommitMessage.class);
                    }

                    @Override
                    public void handle(DriverResponseHandler handler) {
                        if (expectedFailures-- > 0) {
                            handler.onError(new ServiceUnavailableException(""));
                        } else {
                            handler.onCommitSummary(Optional::empty);
                        }
                        handler.onComplete();
                    }
                });
        var retries = failures + 1;
        setupConnectionAnswers(connection, handlers);

        RetryLogic retryLogic = new FixedRetryLogic(retries);
        session = newSession(connectionProvider, retryLogic);
        asyncSession = new InternalAsyncSession(session);

        var work = spy(new TxWork(43));
        int answer = executeTransaction(asyncSession, mode, work);

        assertEquals(43, answer);
        verifyInvocationCount(work, failures + 1);
        verifyCommitTx(connection, times(retries));
    }

    private void testTxIsRetriedUntilFailureWhenFunctionThrows(AccessMode mode) {
        var failures = 14;
        var handlers = List.of(
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
                });
        var retries = failures - 1;
        setupConnectionAnswers(connection, handlers);

        RetryLogic retryLogic = new FixedRetryLogic(retries);
        session = newSession(connectionProvider, retryLogic);
        asyncSession = new InternalAsyncSession(session);

        var work = spy(new TxWork(failures, new SessionExpiredException("Oh!")));

        var e = assertThrows(Exception.class, () -> executeTransaction(asyncSession, mode, work));

        assertThat(e, instanceOf(SessionExpiredException.class));
        assertEquals("Oh!", e.getMessage());
        verifyInvocationCount(work, failures);
        then(connection)
                .should(never())
                .writeAndFlush(
                        any(),
                        ArgumentMatchers.<List<Message>>argThat(
                                messages -> messages.size() == 1 && messages.get(0) instanceof CommitMessage));
        verifyRollbackTx(connection, times(failures));
    }

    private void testTxIsRetriedUntilFailureWhenCommitFails(AccessMode mode) {
        var failures = 17;
        var handlers = List.of(
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
                });
        var retries = failures - 1;
        setupConnectionAnswers(connection, handlers);

        RetryLogic retryLogic = new FixedRetryLogic(retries);
        session = newSession(connectionProvider, retryLogic);
        asyncSession = new InternalAsyncSession(session);

        var work = spy(new TxWork(42));

        var e = assertThrows(Exception.class, () -> executeTransaction(asyncSession, mode, work));

        assertThat(e, instanceOf(ServiceUnavailableException.class));
        verifyInvocationCount(work, failures);
        verifyCommitTx(connection, times(failures));
    }

    private static <T> T executeTransaction(
            AsyncSession session, AccessMode mode, AsyncTransactionCallback<CompletionStage<T>> work) {
        if (mode == READ) {
            return await(session.executeReadAsync(work));
        } else if (mode == WRITE) {
            return await(session.executeWriteAsync(work));
        } else {
            throw new IllegalArgumentException("Unknown mode " + mode);
        }
    }

    @SuppressWarnings("deprecation")
    private static void verifyInvocationCount(AsyncTransactionCallback<?> workSpy, int expectedInvocationCount) {
        verify(workSpy, times(expectedInvocationCount)).execute(any(AsyncTransactionContext.class));
    }

    private static class TxWork implements AsyncTransactionCallback<CompletionStage<Integer>> {
        final int result;
        final int timesToThrow;
        final Supplier<RuntimeException> errorSupplier;

        int invoked;

        TxWork(int result) {
            this(result, (Supplier<RuntimeException>) null);
        }

        TxWork(int timesToThrow, final RuntimeException error) {
            this.result = 42;
            this.timesToThrow = timesToThrow;
            this.errorSupplier = () -> error;
        }

        TxWork(int result, Supplier<RuntimeException> errorSupplier) {
            this.result = result;
            this.timesToThrow = 0;
            this.errorSupplier = errorSupplier;
        }

        @Override
        public CompletionStage<Integer> execute(AsyncTransactionContext tx) {
            if (timesToThrow > 0 && invoked++ < timesToThrow) {
                throw errorSupplier.get();
            }
            return completedFuture(result);
        }
    }

    static List<ExecuteVariation> executeVariations() {
        return Arrays.asList(
                new ExecuteVariation(false, false),
                new ExecuteVariation(false, true),
                new ExecuteVariation(true, false),
                new ExecuteVariation(true, true));
    }

    private record ExecuteVariation(boolean readOnly, boolean explicitTxConfig) {}
}
