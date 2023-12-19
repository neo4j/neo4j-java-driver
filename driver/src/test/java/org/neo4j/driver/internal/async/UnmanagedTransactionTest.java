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
import static org.neo4j.driver.util.TestUtil.assertNoCircularReferences;
import static org.neo4j.driver.util.TestUtil.await;
import static org.neo4j.driver.util.TestUtil.beginMessage;
import static org.neo4j.driver.util.TestUtil.connectionMock;
import static org.neo4j.driver.util.TestUtil.setupSuccessfulRunAndPull;
import static org.neo4j.driver.util.TestUtil.setupSuccessfulRunRx;
import static org.neo4j.driver.util.TestUtil.verifyBeginTx;
import static org.neo4j.driver.util.TestUtil.verifyRollbackTx;
import static org.neo4j.driver.util.TestUtil.verifyRunAndPull;
import static org.neo4j.driver.util.TestUtil.verifyRunRx;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InOrder;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Query;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.exceptions.AuthorizationExpiredException;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.ConnectionReadTimeoutException;
import org.neo4j.driver.internal.DefaultBookmarkHolder;
import org.neo4j.driver.internal.FailableCursor;
import org.neo4j.driver.internal.InternalBookmark;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.messaging.v4.BoltProtocolV4;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ResponseHandler;

class UnmanagedTransactionTest {
    @Test
    void shouldFlushOnRunAsync() {
        // Given
        Connection connection = connectionMock(BoltProtocolV4.INSTANCE);
        UnmanagedTransaction tx = beginTx(connection);
        setupSuccessfulRunAndPull(connection);

        // When
        await(tx.runAsync(new Query("RETURN 1")));

        // Then
        verifyRunAndPull(connection, "RETURN 1");
    }

    @Test
    void shouldFlushOnRunRx() {
        // Given
        Connection connection = connectionMock(BoltProtocolV4.INSTANCE);
        UnmanagedTransaction tx = beginTx(connection);
        setupSuccessfulRunRx(connection);

        // When
        await(tx.runRx(new Query("RETURN 1")));

        // Then
        verifyRunRx(connection, "RETURN 1");
    }

    @Test
    void shouldRollbackOnImplicitFailure() {
        // Given
        Connection connection = connectionMock();
        UnmanagedTransaction tx = beginTx(connection);

        // When
        await(tx.closeAsync());

        // Then
        InOrder order = inOrder(connection);
        verifyBeginTx(connection);
        verifyRollbackTx(connection);
        order.verify(connection).release();
    }

    @Test
    void shouldOnlyQueueMessagesWhenNoBookmarkGiven() {
        Connection connection = connectionMock();

        beginTx(connection, InternalBookmark.empty());

        verifyBeginTx(connection);
        verify(connection, never()).writeAndFlush(any(), any(), any(), any());
    }

    @Test
    void shouldFlushWhenBookmarkGiven() {
        Bookmark bookmark = InternalBookmark.parse("hi, I'm bookmark");
        Connection connection = connectionMock();

        beginTx(connection, bookmark);

        verifyBeginTx(connection);
        verify(connection, never()).write(any(), any(), any(), any());
    }

    @Test
    void shouldBeOpenAfterConstruction() {
        UnmanagedTransaction tx = beginTx(connectionMock());

        assertTrue(tx.isOpen());
    }

    @Test
    void shouldBeClosedWhenMarkedAsTerminated() {
        UnmanagedTransaction tx = beginTx(connectionMock());

        tx.markTerminated(null);

        assertTrue(tx.isOpen());
    }

    @Test
    void shouldBeClosedWhenMarkedTerminatedAndClosed() {
        UnmanagedTransaction tx = beginTx(connectionMock());

        tx.markTerminated(null);
        await(tx.closeAsync());

        assertFalse(tx.isOpen());
    }

    @Test
    void shouldReleaseConnectionWhenBeginFails() {
        RuntimeException error = new RuntimeException("Wrong bookmark!");
        Connection connection = connectionWithBegin(handler -> handler.onFailure(error));
        UnmanagedTransaction tx =
                new UnmanagedTransaction(connection, new DefaultBookmarkHolder(), UNLIMITED_FETCH_SIZE, Logging.none());

        Bookmark bookmark = InternalBookmark.parse("SomeBookmark");
        TransactionConfig txConfig = TransactionConfig.empty();

        RuntimeException e = assertThrows(RuntimeException.class, () -> await(tx.beginAsync(bookmark, txConfig)));

        assertEquals(error, e);
        verify(connection).release();
    }

    @Test
    void shouldNotReleaseConnectionWhenBeginSucceeds() {
        Connection connection = connectionWithBegin(handler -> handler.onSuccess(emptyMap()));
        UnmanagedTransaction tx =
                new UnmanagedTransaction(connection, new DefaultBookmarkHolder(), UNLIMITED_FETCH_SIZE, Logging.none());

        Bookmark bookmark = InternalBookmark.parse("SomeBookmark");
        TransactionConfig txConfig = TransactionConfig.empty();

        await(tx.beginAsync(bookmark, txConfig));

        verify(connection, never()).release();
    }

    @Test
    void shouldReleaseConnectionWhenTerminatedAndCommitted() {
        Connection connection = connectionMock();
        UnmanagedTransaction tx =
                new UnmanagedTransaction(connection, new DefaultBookmarkHolder(), UNLIMITED_FETCH_SIZE, Logging.none());

        tx.markTerminated(null);

        assertThrows(ClientException.class, () -> await(tx.commitAsync()));

        assertFalse(tx.isOpen());
        verify(connection).release();
    }

    @Test
    void shouldNotCreateCircularExceptionWhenTerminationCauseEqualsToCursorFailure() {
        Connection connection = connectionMock();
        ClientException terminationCause = new ClientException("Custom exception");
        ResultCursorsHolder resultCursorsHolder = mockResultCursorWith(terminationCause);
        UnmanagedTransaction tx = new UnmanagedTransaction(
                connection, new DefaultBookmarkHolder(), UNLIMITED_FETCH_SIZE, resultCursorsHolder, Logging.none());

        tx.markTerminated(terminationCause);

        ClientException e = assertThrows(ClientException.class, () -> await(tx.commitAsync()));
        assertNoCircularReferences(e);
        assertEquals(terminationCause, e);
    }

    @Test
    void shouldNotCreateCircularExceptionWhenTerminationCauseDifferentFromCursorFailure() {
        Connection connection = connectionMock();
        ClientException terminationCause = new ClientException("Custom exception");
        ResultCursorsHolder resultCursorsHolder = mockResultCursorWith(new ClientException("Cursor error"));
        UnmanagedTransaction tx = new UnmanagedTransaction(
                connection, new DefaultBookmarkHolder(), UNLIMITED_FETCH_SIZE, resultCursorsHolder, Logging.none());

        tx.markTerminated(terminationCause);

        ClientException e = assertThrows(ClientException.class, () -> await(tx.commitAsync()));
        assertNoCircularReferences(e);
        assertEquals(1, e.getSuppressed().length);

        Throwable suppressed = e.getSuppressed()[0];
        assertEquals(terminationCause, suppressed.getCause());
    }

    @Test
    void shouldNotCreateCircularExceptionWhenTerminatedWithoutFailure() {
        Connection connection = connectionMock();
        ClientException terminationCause = new ClientException("Custom exception");
        UnmanagedTransaction tx =
                new UnmanagedTransaction(connection, new DefaultBookmarkHolder(), UNLIMITED_FETCH_SIZE, Logging.none());

        tx.markTerminated(terminationCause);

        ClientException e = assertThrows(ClientException.class, () -> await(tx.commitAsync()));
        assertNoCircularReferences(e);

        assertEquals(terminationCause, e.getCause());
    }

    @Test
    void shouldReleaseConnectionWhenTerminatedAndRolledBack() {
        Connection connection = connectionMock();
        UnmanagedTransaction tx =
                new UnmanagedTransaction(connection, new DefaultBookmarkHolder(), UNLIMITED_FETCH_SIZE, Logging.none());

        tx.markTerminated(null);
        await(tx.rollbackAsync());

        verify(connection).release();
    }

    @Test
    void shouldReleaseConnectionWhenClose() throws Throwable {
        Connection connection = connectionMock();
        UnmanagedTransaction tx =
                new UnmanagedTransaction(connection, new DefaultBookmarkHolder(), UNLIMITED_FETCH_SIZE, Logging.none());

        await(tx.closeAsync());

        verify(connection).release();
    }

    @Test
    void shouldReleaseConnectionOnConnectionAuthorizationExpiredExceptionFailure() {
        AuthorizationExpiredException exception = new AuthorizationExpiredException("code", "message");
        Connection connection = connectionWithBegin(handler -> handler.onFailure(exception));
        UnmanagedTransaction tx =
                new UnmanagedTransaction(connection, new DefaultBookmarkHolder(), UNLIMITED_FETCH_SIZE, Logging.none());
        Bookmark bookmark = InternalBookmark.parse("SomeBookmark");
        TransactionConfig txConfig = TransactionConfig.empty();

        AuthorizationExpiredException actualException =
                assertThrows(AuthorizationExpiredException.class, () -> await(tx.beginAsync(bookmark, txConfig)));

        assertSame(exception, actualException);
        verify(connection).terminateAndRelease(AuthorizationExpiredException.DESCRIPTION);
        verify(connection, never()).release();
    }

    @Test
    void shouldReleaseConnectionOnConnectionReadTimeoutExceptionFailure() {
        Connection connection =
                connectionWithBegin(handler -> handler.onFailure(ConnectionReadTimeoutException.INSTANCE));
        UnmanagedTransaction tx =
                new UnmanagedTransaction(connection, new DefaultBookmarkHolder(), UNLIMITED_FETCH_SIZE, Logging.none());
        Bookmark bookmark = InternalBookmark.parse("SomeBookmark");
        TransactionConfig txConfig = TransactionConfig.empty();

        ConnectionReadTimeoutException actualException =
                assertThrows(ConnectionReadTimeoutException.class, () -> await(tx.beginAsync(bookmark, txConfig)));

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
        Connection connection = mock(Connection.class);
        BoltProtocol protocol = mock(BoltProtocol.class);
        given(connection.protocol()).willReturn(protocol);
        given(protocolCommit ? protocol.commitTransaction(connection) : protocol.rollbackTransaction(connection))
                .willReturn(new CompletableFuture<>());
        UnmanagedTransaction tx =
                new UnmanagedTransaction(connection, new DefaultBookmarkHolder(), UNLIMITED_FETCH_SIZE, Logging.none());

        CompletionStage<Void> initialStage =
                mapTransactionAction(initialAction, tx).get();
        CompletionStage<Void> similarStage =
                mapTransactionAction(similarAction, tx).get();

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
        Connection connection = mock(Connection.class);
        BoltProtocol protocol = mock(BoltProtocol.class);
        given(connection.protocol()).willReturn(protocol);
        given(protocolCommit ? protocol.commitTransaction(connection) : protocol.rollbackTransaction(connection))
                .willReturn(protocolActionCompleted ? completedFuture(null) : new CompletableFuture<>());
        UnmanagedTransaction tx =
                new UnmanagedTransaction(connection, new DefaultBookmarkHolder(), UNLIMITED_FETCH_SIZE, Logging.none());

        CompletionStage<Void> originalActionStage =
                mapTransactionAction(initialAction, tx).get();
        CompletionStage<Void> conflictingActionStage =
                mapTransactionAction(conflictingAction, tx).get();

        assertNotNull(originalActionStage);
        if (protocolCommit) {
            then(protocol).should(times(1)).commitTransaction(connection);
        } else {
            then(protocol).should(times(1)).rollbackTransaction(connection);
        }
        assertTrue(conflictingActionStage.toCompletableFuture().isCompletedExceptionally());
        Throwable throwable = assertThrows(
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
        Connection connection = mock(Connection.class);
        BoltProtocol protocol = mock(BoltProtocol.class);
        given(connection.protocol()).willReturn(protocol);
        given(protocolCommit ? protocol.commitTransaction(connection) : protocol.rollbackTransaction(connection))
                .willReturn(completedFuture(null));
        UnmanagedTransaction tx =
                new UnmanagedTransaction(connection, new DefaultBookmarkHolder(), UNLIMITED_FETCH_SIZE, Logging.none());

        CompletionStage<Void> originalActionStage =
                mapTransactionAction(originalAction, tx).get();
        CompletionStage<Void> closeStage = commitOnClose != null ? tx.closeAsync(commitOnClose) : tx.closeAsync();

        assertTrue(originalActionStage.toCompletableFuture().isDone());
        assertFalse(originalActionStage.toCompletableFuture().isCompletedExceptionally());
        if (protocolCommit) {
            then(protocol).should(times(expectedProtocolInvocations)).commitTransaction(connection);
        } else {
            then(protocol).should(times(expectedProtocolInvocations)).rollbackTransaction(connection);
        }
        assertNull(closeStage.toCompletableFuture().join());
    }

    private static UnmanagedTransaction beginTx(Connection connection) {
        return beginTx(connection, InternalBookmark.empty());
    }

    private static UnmanagedTransaction beginTx(Connection connection, Bookmark initialBookmark) {
        UnmanagedTransaction tx =
                new UnmanagedTransaction(connection, new DefaultBookmarkHolder(), UNLIMITED_FETCH_SIZE, Logging.none());
        return await(tx.beginAsync(initialBookmark, TransactionConfig.empty()));
    }

    private static Connection connectionWithBegin(Consumer<ResponseHandler> beginBehaviour) {
        Connection connection = connectionMock();

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
        ResultCursorsHolder resultCursorsHolder = new ResultCursorsHolder();
        FailableCursor cursor = mock(FailableCursor.class);
        doReturn(completedFuture(clientException)).when(cursor).discardAllFailureAsync();
        resultCursorsHolder.add(completedFuture(cursor));
        return resultCursorsHolder;
    }

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
