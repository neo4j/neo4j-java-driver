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
package org.neo4j.driver.internal.async;

import static java.util.Collections.emptyMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.driver.internal.handlers.pulln.FetchSizeUtil.UNLIMITED_FETCH_SIZE;
import static org.neo4j.driver.testutil.TestUtil.assertNoCircularReferences;
import static org.neo4j.driver.testutil.TestUtil.await;
import static org.neo4j.driver.testutil.TestUtil.beginMessage;
import static org.neo4j.driver.testutil.TestUtil.connectionMock;
import static org.neo4j.driver.testutil.TestUtil.setupFailingRun;
import static org.neo4j.driver.testutil.TestUtil.setupSuccessfulRunAndPull;
import static org.neo4j.driver.testutil.TestUtil.setupSuccessfulRunRx;
import static org.neo4j.driver.testutil.TestUtil.verifyBeginTx;
import static org.neo4j.driver.testutil.TestUtil.verifyRollbackTx;
import static org.neo4j.driver.testutil.TestUtil.verifyRunAndPull;
import static org.neo4j.driver.testutil.TestUtil.verifyRunRx;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Query;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.exceptions.AuthorizationExpiredException;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.ConnectionReadTimeoutException;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.TransactionTerminatedException;
import org.neo4j.driver.internal.DatabaseBookmark;
import org.neo4j.driver.internal.FailableCursor;
import org.neo4j.driver.internal.InternalBookmark;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.messaging.v4.BoltProtocolV4;
import org.neo4j.driver.internal.messaging.v53.BoltProtocolV53;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ResponseHandler;

class UnmanagedTransactionTest {
    @Test
    void shouldFlushOnRunAsync() {
        // Given
        var connection = connectionMock(BoltProtocolV4.INSTANCE);
        var tx = beginTx(connection);
        setupSuccessfulRunAndPull(connection);

        // When
        await(tx.runAsync(new Query("RETURN 1")));

        // Then
        verifyRunAndPull(connection, "RETURN 1");
    }

    @Test
    void shouldFlushOnRunRx() {
        // Given
        var connection = connectionMock(BoltProtocolV4.INSTANCE);
        var tx = beginTx(connection);
        setupSuccessfulRunRx(connection);

        // When
        await(tx.runRx(new Query("RETURN 1")));

        // Then
        verifyRunRx(connection, "RETURN 1");
    }

    @Test
    void shouldRollbackOnImplicitFailure() {
        // Given
        var connection = connectionMock();
        var tx = beginTx(connection);

        // When
        await(tx.closeAsync());

        // Then
        var order = inOrder(connection);
        verifyBeginTx(connection);
        verifyRollbackTx(connection);
        order.verify(connection).release();
    }

    @Test
    void shouldOnlyQueueMessagesWhenNoBookmarkGiven() {
        var connection = connectionMock();

        beginTx(connection, Collections.emptySet());

        verifyBeginTx(connection);
    }

    @Test
    void shouldFlushWhenBookmarkGiven() {
        var bookmarks = Collections.singleton(InternalBookmark.parse("hi, I'm bookmark"));
        var connection = connectionMock();

        beginTx(connection, bookmarks);

        verifyBeginTx(connection);
    }

    @Test
    void shouldBeOpenAfterConstruction() {
        var tx = beginTx(connectionMock());

        assertTrue(tx.isOpen());
    }

    @Test
    @SuppressWarnings("ThrowableNotThrown")
    void shouldBeClosedWhenMarkedAsTerminated() {
        var tx = beginTx(connectionMock());

        tx.markTerminated(null);

        assertTrue(tx.isOpen());
    }

    @Test
    @SuppressWarnings("ThrowableNotThrown")
    void shouldBeClosedWhenMarkedTerminatedAndClosed() {
        var tx = beginTx(connectionMock());

        tx.markTerminated(null);
        await(tx.closeAsync());

        assertFalse(tx.isOpen());
    }

    @Test
    void shouldReleaseConnectionWhenBeginFails() {
        var error = new RuntimeException("Wrong bookmark!");
        var connection = connectionWithBegin(handler -> handler.onFailure(error));
        var tx = new UnmanagedTransaction(connection, (ignored) -> {}, UNLIMITED_FETCH_SIZE, null, Logging.none());

        var bookmarks = Collections.singleton(InternalBookmark.parse("SomeBookmark"));
        var txConfig = TransactionConfig.empty();

        var e = assertThrows(RuntimeException.class, () -> await(tx.beginAsync(bookmarks, txConfig, null, true)));

        assertEquals(error, e);
        verify(connection).release();
    }

    @Test
    void shouldNotReleaseConnectionWhenBeginSucceeds() {
        var connection = connectionWithBegin(handler -> handler.onSuccess(emptyMap()));
        var tx = new UnmanagedTransaction(connection, (ignored) -> {}, UNLIMITED_FETCH_SIZE, null, Logging.none());

        var bookmarks = Collections.singleton(InternalBookmark.parse("SomeBookmark"));
        var txConfig = TransactionConfig.empty();

        await(tx.beginAsync(bookmarks, txConfig, null, true));

        verify(connection, never()).release();
    }

    @Test
    @SuppressWarnings("ThrowableNotThrown")
    void shouldReleaseConnectionWhenTerminatedAndCommitted() {
        var connection = connectionMock();
        var tx = new UnmanagedTransaction(connection, (ignored) -> {}, UNLIMITED_FETCH_SIZE, null, Logging.none());

        tx.markTerminated(null);

        assertThrows(TransactionTerminatedException.class, () -> await(tx.commitAsync()));

        assertFalse(tx.isOpen());
        verify(connection).release();
    }

    @Test
    @SuppressWarnings("ThrowableNotThrown")
    void shouldNotCreateCircularExceptionWhenTerminationCauseEqualsToCursorFailure() {
        var connection = connectionMock();
        var terminationCause = new ClientException("Custom exception");
        var resultCursorsHolder = mockResultCursorWith(terminationCause);
        var tx = new UnmanagedTransaction(
                connection, (ignored) -> {}, UNLIMITED_FETCH_SIZE, resultCursorsHolder, null, Logging.none());

        tx.markTerminated(terminationCause);

        var e = assertThrows(ClientException.class, () -> await(tx.commitAsync()));
        assertNoCircularReferences(e);
        assertEquals(terminationCause, e);
    }

    @Test
    @SuppressWarnings("ThrowableNotThrown")
    void shouldNotCreateCircularExceptionWhenTerminationCauseDifferentFromCursorFailure() {
        var connection = connectionMock();
        var terminationCause = new ClientException("Custom exception");
        var resultCursorsHolder = mockResultCursorWith(new ClientException("Cursor error"));
        var tx = new UnmanagedTransaction(
                connection, (ignored) -> {}, UNLIMITED_FETCH_SIZE, resultCursorsHolder, null, Logging.none());

        tx.markTerminated(terminationCause);

        var e = assertThrows(ClientException.class, () -> await(tx.commitAsync()));
        assertNoCircularReferences(e);
        assertEquals(1, e.getSuppressed().length);

        var suppressed = e.getSuppressed()[0];
        assertEquals(terminationCause, suppressed.getCause());
    }

    @Test
    @SuppressWarnings("ThrowableNotThrown")
    void shouldNotCreateCircularExceptionWhenTerminatedWithoutFailure() {
        var connection = connectionMock();
        var terminationCause = new ClientException("Custom exception");
        var tx = new UnmanagedTransaction(connection, (ignored) -> {}, UNLIMITED_FETCH_SIZE, null, Logging.none());

        tx.markTerminated(terminationCause);

        var e = assertThrows(TransactionTerminatedException.class, () -> await(tx.commitAsync()));
        assertNoCircularReferences(e);

        assertEquals(terminationCause, e.getCause());
    }

    @Test
    @SuppressWarnings("ThrowableNotThrown")
    void shouldReleaseConnectionWhenTerminatedAndRolledBack() {
        var connection = connectionMock();
        var tx = new UnmanagedTransaction(connection, (ignored) -> {}, UNLIMITED_FETCH_SIZE, null, Logging.none());

        tx.markTerminated(null);
        await(tx.rollbackAsync());

        verify(connection).release();
    }

    @Test
    void shouldReleaseConnectionWhenClose() {
        var connection = connectionMock();
        var tx = new UnmanagedTransaction(connection, (ignored) -> {}, UNLIMITED_FETCH_SIZE, null, Logging.none());

        await(tx.closeAsync());

        verify(connection).release();
    }

    @Test
    void shouldReleaseConnectionOnConnectionAuthorizationExpiredExceptionFailure() {
        var exception = new AuthorizationExpiredException("code", "message");
        var connection = connectionWithBegin(handler -> handler.onFailure(exception));
        var tx = new UnmanagedTransaction(connection, (ignored) -> {}, UNLIMITED_FETCH_SIZE, null, Logging.none());
        var bookmarks = Collections.singleton(InternalBookmark.parse("SomeBookmark"));
        var txConfig = TransactionConfig.empty();

        var actualException = assertThrows(
                AuthorizationExpiredException.class, () -> await(tx.beginAsync(bookmarks, txConfig, null, true)));

        assertSame(exception, actualException);
        verify(connection).terminateAndRelease(AuthorizationExpiredException.DESCRIPTION);
        verify(connection, never()).release();
    }

    @Test
    void shouldReleaseConnectionOnConnectionReadTimeoutExceptionFailure() {
        var connection = connectionWithBegin(handler -> handler.onFailure(ConnectionReadTimeoutException.INSTANCE));
        var tx = new UnmanagedTransaction(connection, (ignored) -> {}, UNLIMITED_FETCH_SIZE, null, Logging.none());
        var bookmarks = Collections.singleton(InternalBookmark.parse("SomeBookmark"));
        var txConfig = TransactionConfig.empty();

        var actualException = assertThrows(
                ConnectionReadTimeoutException.class, () -> await(tx.beginAsync(bookmarks, txConfig, null, true)));

        assertSame(ConnectionReadTimeoutException.INSTANCE, actualException);
        verify(connection).terminateAndRelease(ConnectionReadTimeoutException.INSTANCE.getMessage());
        verify(connection, never()).release();
    }

    private static Stream<Arguments> similarTransactionCompletingActionArgs() {
        return Stream.of(
                Arguments.of(true, "commit", "commit"),
                Arguments.of(false, "rollback", "rollback"),
                Arguments.of(false, "rollback", "close"),
                Arguments.of(false, "close", "rollback"),
                Arguments.of(false, "close", "close"));
    }

    @ParameterizedTest
    @MethodSource("similarTransactionCompletingActionArgs")
    void shouldReturnExistingStageOnSimilarCompletingAction(
            boolean protocolCommit, String initialAction, String similarAction) {
        var connection = mock(Connection.class);
        var protocol = mock(BoltProtocol.class);
        given(connection.protocol()).willReturn(protocol);
        given(protocolCommit ? protocol.commitTransaction(connection) : protocol.rollbackTransaction(connection))
                .willReturn(new CompletableFuture<>());
        var tx = new UnmanagedTransaction(connection, (ignored) -> {}, UNLIMITED_FETCH_SIZE, null, Logging.none());

        var initialStage = mapTransactionAction(initialAction, tx).get();
        var similarStage = mapTransactionAction(similarAction, tx).get();

        assertSame(initialStage, similarStage);
        if (protocolCommit) {
            then(protocol).should(times(1)).commitTransaction(connection);
        } else {
            then(protocol).should(times(1)).rollbackTransaction(connection);
        }
    }

    private static Stream<Arguments> conflictingTransactionCompletingActionArgs() {
        return Stream.of(
                Arguments.of(true, true, "commit", "commit", UnmanagedTransaction.CANT_COMMIT_COMMITTED_MSG),
                Arguments.of(true, true, "commit", "rollback", UnmanagedTransaction.CANT_ROLLBACK_COMMITTED_MSG),
                Arguments.of(true, false, "commit", "rollback", UnmanagedTransaction.CANT_ROLLBACK_COMMITTING_MSG),
                Arguments.of(true, false, "commit", "close", UnmanagedTransaction.CANT_ROLLBACK_COMMITTING_MSG),
                Arguments.of(false, true, "rollback", "rollback", UnmanagedTransaction.CANT_ROLLBACK_ROLLED_BACK_MSG),
                Arguments.of(false, true, "rollback", "commit", UnmanagedTransaction.CANT_COMMIT_ROLLED_BACK_MSG),
                Arguments.of(false, false, "rollback", "commit", UnmanagedTransaction.CANT_COMMIT_ROLLING_BACK_MSG),
                Arguments.of(false, true, "close", "commit", UnmanagedTransaction.CANT_COMMIT_ROLLED_BACK_MSG),
                Arguments.of(false, true, "close", "rollback", UnmanagedTransaction.CANT_ROLLBACK_ROLLED_BACK_MSG),
                Arguments.of(false, false, "close", "commit", UnmanagedTransaction.CANT_COMMIT_ROLLING_BACK_MSG));
    }

    @ParameterizedTest
    @MethodSource("conflictingTransactionCompletingActionArgs")
    void shouldReturnFailingStageOnConflictingCompletingAction(
            boolean protocolCommit,
            boolean protocolActionCompleted,
            String initialAction,
            String conflictingAction,
            String expectedErrorMsg) {
        var connection = mock(Connection.class);
        var protocol = mock(BoltProtocol.class);
        given(connection.protocol()).willReturn(protocol);
        given(protocolCommit ? protocol.commitTransaction(connection) : protocol.rollbackTransaction(connection))
                .willReturn(protocolActionCompleted ? completedFuture(null) : new CompletableFuture<>());
        var tx = new UnmanagedTransaction(connection, (ignored) -> {}, UNLIMITED_FETCH_SIZE, null, Logging.none());

        var originalActionStage = mapTransactionAction(initialAction, tx).get();
        var conflictingActionStage = mapTransactionAction(conflictingAction, tx).get();

        assertNotNull(originalActionStage);
        if (protocolCommit) {
            then(protocol).should(times(1)).commitTransaction(connection);
        } else {
            then(protocol).should(times(1)).rollbackTransaction(connection);
        }
        assertTrue(conflictingActionStage.toCompletableFuture().isCompletedExceptionally());
        var throwable = assertThrows(
                        ExecutionException.class,
                        () -> conflictingActionStage.toCompletableFuture().get())
                .getCause();
        assertTrue(throwable instanceof ClientException);
        assertEquals(expectedErrorMsg, throwable.getMessage());
    }

    private static Stream<Arguments> closingNotActionTransactionArgs() {
        return Stream.of(
                Arguments.of(true, 1, "commit", null),
                Arguments.of(false, 1, "rollback", null),
                Arguments.of(false, 0, "terminate", null),
                Arguments.of(true, 1, "commit", true),
                Arguments.of(false, 1, "rollback", true),
                Arguments.of(true, 1, "commit", false),
                Arguments.of(false, 1, "rollback", false),
                Arguments.of(false, 0, "terminate", false));
    }

    @ParameterizedTest
    @MethodSource("closingNotActionTransactionArgs")
    void shouldReturnCompletedWithNullStageOnClosingInactiveTransactionExceptCommittingAborted(
            boolean protocolCommit, int expectedProtocolInvocations, String originalAction, Boolean commitOnClose) {
        var connection = mock(Connection.class);
        var protocol = mock(BoltProtocol.class);
        given(connection.protocol()).willReturn(protocol);
        given(protocolCommit ? protocol.commitTransaction(connection) : protocol.rollbackTransaction(connection))
                .willReturn(completedFuture(null));
        var tx = new UnmanagedTransaction(connection, (ignored) -> {}, UNLIMITED_FETCH_SIZE, null, Logging.none());

        var originalActionStage = mapTransactionAction(originalAction, tx).get();
        var closeStage = commitOnClose != null ? tx.closeAsync(commitOnClose) : tx.closeAsync();

        assertTrue(originalActionStage.toCompletableFuture().isDone());
        assertFalse(originalActionStage.toCompletableFuture().isCompletedExceptionally());
        if (protocolCommit) {
            then(protocol).should(times(expectedProtocolInvocations)).commitTransaction(connection);
        } else {
            then(protocol).should(times(expectedProtocolInvocations)).rollbackTransaction(connection);
        }
        assertNull(closeStage.toCompletableFuture().join());
    }

    @Test
    void shouldTerminateOnTerminateAsync() {
        // Given
        var connection = connectionMock(BoltProtocolV4.INSTANCE);
        var tx = beginTx(connection);

        // When
        await(tx.terminateAsync());

        // Then
        then(connection).should().reset(any());
    }

    @Test
    void shouldServeTheSameStageOnTerminateAsync() {
        // Given
        var connection = connectionMock(BoltProtocolV4.INSTANCE);
        var tx = beginTx(connection);

        // When
        var stage0 = tx.terminateAsync();
        var stage1 = tx.terminateAsync();

        // Then
        assertEquals(stage0, stage1);
    }

    @Test
    void shouldHandleTerminationWhenAlreadyTerminated() throws ExecutionException, InterruptedException {
        // Given
        var connection = connectionMock(BoltProtocolV4.INSTANCE);
        var exception = new Neo4jException("message");
        setupFailingRun(connection, exception);
        var tx = beginTx(connection);
        Throwable actualException = null;

        // When
        try {
            tx.runAsync(new Query("RETURN 1")).toCompletableFuture().get();
        } catch (ExecutionException e) {
            actualException = e.getCause();
        }
        tx.terminateAsync().toCompletableFuture().get();

        // Then
        assertEquals(exception, actualException);
    }

    @ParameterizedTest
    @MethodSource("transactionClosingTestParams")
    void shouldThrowOnRunningNewQueriesWhenTransactionIsClosing(TransactionClosingTestParams testParams) {
        // Given
        var boltProtocol = mock(BoltProtocol.class);
        given(boltProtocol.version()).willReturn(BoltProtocolV53.VERSION);
        var closureStage = new CompletableFuture<DatabaseBookmark>();
        var connection = connectionMock(boltProtocol);
        given(boltProtocol.beginTransaction(eq(connection), any(), any(), any(), any(), any(), eq(true)))
                .willReturn(completedFuture(null));
        given(boltProtocol.commitTransaction(connection)).willReturn(closureStage);
        given(boltProtocol.rollbackTransaction(connection)).willReturn(closureStage.thenApply(ignored -> null));
        var tx = beginTx(connection);

        // When
        testParams.closeAction().apply(tx);
        var exception = assertThrows(
                ClientException.class, () -> await(testParams.runAction().apply(tx)));

        // Then
        assertEquals(testParams.expectedMessage(), exception.getMessage());
    }

    static List<Arguments> transactionClosingTestParams() {
        Function<UnmanagedTransaction, CompletionStage<?>> asyncRun = tx -> tx.runAsync(new Query("query"));
        Function<UnmanagedTransaction, CompletionStage<?>> reactiveRun = tx -> tx.runRx(new Query("query"));
        return List.of(
                Arguments.of(Named.of(
                        "commit and run async",
                        new TransactionClosingTestParams(
                                UnmanagedTransaction::commitAsync,
                                asyncRun,
                                "Cannot run more queries in this transaction, it is being committed"))),
                Arguments.of(Named.of(
                        "commit and run reactive",
                        new TransactionClosingTestParams(
                                UnmanagedTransaction::commitAsync,
                                reactiveRun,
                                "Cannot run more queries in this transaction, it is being committed"))),
                Arguments.of(Named.of(
                        "rollback and run async",
                        new TransactionClosingTestParams(
                                UnmanagedTransaction::rollbackAsync,
                                asyncRun,
                                "Cannot run more queries in this transaction, it is being rolled back"))),
                Arguments.of(Named.of(
                        "rollback and run reactive",
                        new TransactionClosingTestParams(
                                UnmanagedTransaction::rollbackAsync,
                                reactiveRun,
                                "Cannot run more queries in this transaction, it is being rolled back"))),
                Arguments.of(Named.of(
                        "close and run async",
                        new TransactionClosingTestParams(
                                UnmanagedTransaction::closeAsync,
                                asyncRun,
                                "Cannot run more queries in this transaction, it is being rolled back"))),
                Arguments.of(Named.of(
                        "close and run reactive",
                        new TransactionClosingTestParams(
                                UnmanagedTransaction::closeAsync,
                                reactiveRun,
                                "Cannot run more queries in this transaction, it is being rolled back"))));
    }

    private record TransactionClosingTestParams(
            Function<UnmanagedTransaction, CompletionStage<?>> closeAction,
            Function<UnmanagedTransaction, CompletionStage<?>> runAction,
            String expectedMessage) {}

    private static UnmanagedTransaction beginTx(Connection connection) {
        return beginTx(connection, Collections.emptySet());
    }

    private static UnmanagedTransaction beginTx(Connection connection, Set<Bookmark> initialBookmarks) {
        var tx = new UnmanagedTransaction(connection, (ignored) -> {}, UNLIMITED_FETCH_SIZE, null, Logging.none());
        return await(tx.beginAsync(initialBookmarks, TransactionConfig.empty(), null, true));
    }

    private static Connection connectionWithBegin(Consumer<ResponseHandler> beginBehaviour) {
        var connection = connectionMock();

        doAnswer(invocation -> {
                    ResponseHandler beginHandler = invocation.getArgument(1);
                    beginBehaviour.accept(beginHandler);
                    return null;
                })
                .when(connection)
                .writeAndFlush(argThat(beginMessage()), any());

        return connection;
    }

    private ResultCursorsHolder mockResultCursorWith(ClientException clientException) {
        var resultCursorsHolder = new ResultCursorsHolder();
        var cursor = mock(FailableCursor.class);
        doReturn(completedFuture(clientException)).when(cursor).discardAllFailureAsync();
        resultCursorsHolder.add(completedFuture(cursor));
        return resultCursorsHolder;
    }

    @SuppressWarnings("ThrowableNotThrown")
    private Supplier<CompletionStage<Void>> mapTransactionAction(String actionName, UnmanagedTransaction tx) {
        Supplier<CompletionStage<Void>> action;
        if ("commit".equals(actionName)) {
            action = tx::commitAsync;
        } else if ("rollback".equals(actionName)) {
            action = tx::rollbackAsync;
        } else if ("terminate".equals(actionName)) {
            action = () -> {
                tx.markTerminated(mock(Throwable.class));
                return completedFuture(null);
            };
        } else if ("close".equals(actionName)) {
            action = tx::closeAsync;
        } else {
            throw new RuntimeException(String.format("Unknown completing action type '%s'", actionName));
        }
        return action;
    }
}
