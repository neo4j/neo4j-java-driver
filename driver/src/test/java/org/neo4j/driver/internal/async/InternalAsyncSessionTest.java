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
import static org.mockito.Mockito.when;
import static org.neo4j.driver.AccessMode.READ;
import static org.neo4j.driver.AccessMode.WRITE;
import static org.neo4j.driver.TransactionConfig.empty;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.testutil.TestUtil.await;
import static org.neo4j.driver.testutil.TestUtil.connectionMock;
import static org.neo4j.driver.testutil.TestUtil.newSession;
import static org.neo4j.driver.testutil.TestUtil.setupFailingCommit;
import static org.neo4j.driver.testutil.TestUtil.setupSuccessfulRunAndPull;
import static org.neo4j.driver.testutil.TestUtil.verifyBeginTx;
import static org.neo4j.driver.testutil.TestUtil.verifyCommitTx;
import static org.neo4j.driver.testutil.TestUtil.verifyRollbackTx;
import static org.neo4j.driver.testutil.TestUtil.verifyRunAndPull;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Query;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.Value;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.async.AsyncTransaction;
import org.neo4j.driver.async.AsyncTransactionCallback;
import org.neo4j.driver.async.AsyncTransactionWork;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.driver.internal.DatabaseNameUtil;
import org.neo4j.driver.internal.InternalBookmark;
import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.messaging.v4.BoltProtocolV4;
import org.neo4j.driver.internal.retry.RetryLogic;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionProvider;
import org.neo4j.driver.internal.util.FixedRetryLogic;
import org.neo4j.driver.internal.value.IntegerValue;

class InternalAsyncSessionTest {
    private static final String DATABASE = "neo4j";
    private Connection connection;
    private ConnectionProvider connectionProvider;
    private AsyncSession asyncSession;
    private NetworkSession session;

    @BeforeEach
    void setUp() {
        connection = connectionMock(BoltProtocolV4.INSTANCE);
        connectionProvider = mock(ConnectionProvider.class);
        when(connectionProvider.acquireConnection(any(ConnectionContext.class))).thenAnswer(invocation -> {
            var context = (ConnectionContext) invocation.getArgument(0);
            context.databaseNameFuture().complete(DatabaseNameUtil.database(DATABASE));
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

    @SuppressWarnings("deprecation")
    private static Stream<Function<AsyncSession, CompletionStage<String>>> allRunTxMethods() {
        return Stream.of(
                session -> session.readTransactionAsync(tx -> completedFuture("a")),
                session -> session.writeTransactionAsync(tx -> completedFuture("a")),
                session -> session.readTransactionAsync(tx -> completedFuture("a"), empty()),
                session -> session.writeTransactionAsync(tx -> completedFuture("a"), empty()));
    }

    @ParameterizedTest
    @MethodSource("allSessionRunMethods")
    void shouldFlushOnRun(Function<AsyncSession, CompletionStage<ResultCursor>> runReturnOne) {
        setupSuccessfulRunAndPull(connection);

        var cursor = await(runReturnOne.apply(asyncSession));

        verifyRunAndPull(connection, await(cursor.consumeAsync()).query().text());
    }

    @ParameterizedTest
    @MethodSource("allBeginTxMethods")
    void shouldDelegateBeginTx(Function<AsyncSession, CompletionStage<AsyncTransaction>> beginTx) {
        var tx = await(beginTx.apply(asyncSession));

        verifyBeginTx(connection);
        assertNotNull(tx);
    }

    @ParameterizedTest
    @MethodSource("allRunTxMethods")
    void txRunShouldBeginAndCommitTx(Function<AsyncSession, CompletionStage<String>> runTx) {
        var string = await(runTx.apply(asyncSession));

        verifyBeginTx(connection);
        verifyCommitTx(connection);
        verify(connection).release();
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
        session = newSession(connectionProvider, Collections.singleton(InternalBookmark.parse("Bookmark1")));
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
        final RuntimeException error = new IllegalStateException("Oh!");
        AsyncTransactionWork<CompletionStage<Void>> work = tx -> {
            throw error;
        };

        var e = assertThrows(Exception.class, () -> executeTransaction(asyncSession, transactionMode, work));
        assertEquals(error, e);

        verify(connectionProvider).acquireConnection(any(ConnectionContext.class));
        verifyBeginTx(connection);
        verifyRollbackTx(connection);
    }

    private void testTxIsRetriedUntilSuccessWhenFunctionThrows(AccessMode mode) {
        var failures = 12;
        var retries = failures + 1;

        RetryLogic retryLogic = new FixedRetryLogic(retries);
        session = newSession(connectionProvider, retryLogic);
        asyncSession = new InternalAsyncSession(session);

        var work = spy(new TxWork(failures, new SessionExpiredException("")));
        int answer = executeTransaction(asyncSession, mode, work);

        assertEquals(42, answer);
        verifyInvocationCount(work, failures + 1);
        verifyCommitTx(connection);
        verifyRollbackTx(connection, times(failures));
    }

    private void testTxIsRetriedUntilSuccessWhenCommitThrows(AccessMode mode) {
        var failures = 13;
        var retries = failures + 1;

        RetryLogic retryLogic = new FixedRetryLogic(retries);
        setupFailingCommit(connection, failures);
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
        var retries = failures - 1;

        RetryLogic retryLogic = new FixedRetryLogic(retries);
        session = newSession(connectionProvider, retryLogic);
        asyncSession = new InternalAsyncSession(session);

        var work = spy(new TxWork(failures, new SessionExpiredException("Oh!")));

        var e = assertThrows(Exception.class, () -> executeTransaction(asyncSession, mode, work));

        assertThat(e, instanceOf(SessionExpiredException.class));
        assertEquals("Oh!", e.getMessage());
        verifyInvocationCount(work, failures);
        verifyCommitTx(connection, never());
        verifyRollbackTx(connection, times(failures));
    }

    private void testTxIsRetriedUntilFailureWhenCommitFails(AccessMode mode) {
        var failures = 17;
        var retries = failures - 1;

        RetryLogic retryLogic = new FixedRetryLogic(retries);
        setupFailingCommit(connection, failures);
        session = newSession(connectionProvider, retryLogic);
        asyncSession = new InternalAsyncSession(session);

        var work = spy(new TxWork(42));

        var e = assertThrows(Exception.class, () -> executeTransaction(asyncSession, mode, work));

        assertThat(e, instanceOf(ServiceUnavailableException.class));
        verifyInvocationCount(work, failures);
        verifyCommitTx(connection, times(failures));
    }

    @SuppressWarnings("deprecation")
    private static <T> T executeTransaction(
            AsyncSession session, AccessMode mode, AsyncTransactionWork<CompletionStage<T>> work) {
        if (mode == READ) {
            return await(session.readTransactionAsync(work));
        } else if (mode == WRITE) {
            return await(session.writeTransactionAsync(work));
        } else {
            throw new IllegalArgumentException("Unknown mode " + mode);
        }
    }

    @SuppressWarnings("deprecation")
    private static void verifyInvocationCount(AsyncTransactionWork<?> workSpy, int expectedInvocationCount) {
        verify(workSpy, times(expectedInvocationCount)).execute(any(AsyncTransaction.class));
    }

    @SuppressWarnings("deprecation")
    private static class TxWork implements AsyncTransactionWork<CompletionStage<Integer>> {
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
        public CompletionStage<Integer> execute(AsyncTransaction tx) {
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
