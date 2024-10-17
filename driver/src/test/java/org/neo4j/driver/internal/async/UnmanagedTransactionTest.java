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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.driver.testutil.TestUtil.assertNoCircularReferences;
import static org.neo4j.driver.testutil.TestUtil.await;
import static org.neo4j.driver.testutil.TestUtil.connectionMock;
import static org.neo4j.driver.testutil.TestUtil.setupConnectionAnswers;
import static org.neo4j.driver.testutil.TestUtil.verifyRunAndPull;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
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
import org.neo4j.driver.internal.FailableCursor;
import org.neo4j.driver.internal.InternalBookmark;
import org.neo4j.driver.internal.bolt.api.AccessMode;
import org.neo4j.driver.internal.bolt.api.BoltConnection;
import org.neo4j.driver.internal.bolt.api.BoltProtocolVersion;
import org.neo4j.driver.internal.bolt.api.DatabaseNameUtil;
import org.neo4j.driver.internal.bolt.api.TelemetryApi;
import org.neo4j.driver.internal.bolt.api.summary.BeginSummary;
import org.neo4j.driver.internal.bolt.api.summary.CommitSummary;
import org.neo4j.driver.internal.bolt.api.summary.PullSummary;
import org.neo4j.driver.internal.bolt.api.summary.ResetSummary;
import org.neo4j.driver.internal.bolt.api.summary.RollbackSummary;
import org.neo4j.driver.internal.bolt.api.summary.RunSummary;
import org.neo4j.driver.internal.telemetry.ApiTelemetryWork;

class UnmanagedTransactionTest {
    @Test
    void shouldFlushOnRunAsync() {
        // Given
        var connection = connectionMock(new BoltProtocolVersion(5, 0));
        given(connection.beginTransaction(any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .willReturn(completedFuture(connection));
        given(connection.run(any(), any())).willReturn(CompletableFuture.completedStage(connection));
        given(connection.pull(anyLong(), anyLong())).willReturn(CompletableFuture.completedStage(connection));
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
        var tx = beginTx(connection);

        // When
        await(tx.runAsync(new Query("RETURN 1")));

        // Then
        verifyRunAndPull(connection, "RETURN 1");
    }

    @Test
    void shouldFlushOnRunRx() {
        // Given
        var connection = connectionMock(new BoltProtocolVersion(5, 0));
        given(connection.beginTransaction(any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .willReturn(completedFuture(connection));
        given(connection.run(any(), any())).willReturn(CompletableFuture.completedStage(connection));
        setupConnectionAnswers(
                connection,
                List.of(
                        handler -> {
                            handler.onBeginSummary(mock(BeginSummary.class));
                            handler.onComplete();
                        },
                        handler -> {
                            handler.onRunSummary(mock(RunSummary.class));
                            handler.onComplete();
                        }));
        var tx = beginTx(connection);

        // When
        await(tx.runRx(new Query("RETURN 1")));

        // Then
        then(connection).should().run("RETURN 1", Collections.emptyMap());
        then(connection).should(times(2)).flush(any());
    }

    @Test
    void shouldRollbackOnImplicitFailure() {
        // Given
        var connection = connectionMock();
        given(connection.beginTransaction(any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .willReturn(completedFuture(connection));
        given(connection.rollback()).willReturn(CompletableFuture.completedStage(connection));
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
        var tx = beginTx(connection);

        // When
        await(tx.closeAsync());

        // Then
        then(connection).should().beginTransaction(any(), any(), any(), any(), any(), any(), any(), any(), any());
        then(connection).should().rollback();
        then(connection).should(times(2)).flush(any());
        then(connection).should().close();
    }

    @Test
    void shouldBeginTransaction() {
        var connection = connectionMock();
        given(connection.beginTransaction(any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .willReturn(completedFuture(connection));
        setupConnectionAnswers(connection, List.of(handler -> {
            handler.onBeginSummary(mock(BeginSummary.class));
            handler.onComplete();
        }));

        beginTx(connection, Collections.emptySet());

        then(connection).should().beginTransaction(any(), any(), any(), any(), any(), any(), any(), any(), any());
        then(connection).should().flush(any());
    }

    @Test
    void shouldBeOpenAfterConstruction() {
        var connection = connectionMock();
        given(connection.beginTransaction(any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .willReturn(completedFuture(connection));
        setupConnectionAnswers(connection, List.of(handler -> {
            handler.onBeginSummary(mock(BeginSummary.class));
            handler.onComplete();
        }));

        var tx = beginTx(connection);

        assertTrue(tx.isOpen());
    }

    @Test
    void shouldBeClosedWhenMarkedAsTerminated() {
        var connection = connectionMock();
        given(connection.beginTransaction(any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .willReturn(completedFuture(connection));
        setupConnectionAnswers(connection, List.of(handler -> {
            handler.onBeginSummary(mock(BeginSummary.class));
            handler.onComplete();
        }));
        var tx = beginTx(connection);

        tx.markTerminated(null);

        assertTrue(tx.isOpen());
    }

    @Test
    void shouldBeClosedWhenMarkedTerminatedAndClosed() {
        var connection = connectionMock();
        given(connection.beginTransaction(any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .willReturn(completedFuture(connection));
        setupConnectionAnswers(connection, List.of(handler -> {
            handler.onBeginSummary(mock(BeginSummary.class));
            handler.onComplete();
        }));
        given(connection.close()).willReturn(CompletableFuture.completedStage(null));
        var tx = beginTx(connection);

        tx.markTerminated(null);
        await(tx.closeAsync());

        assertFalse(tx.isOpen());
    }

    @Test
    void shouldReleaseConnectionWhenBeginFails() {
        var error = new RuntimeException("Wrong bookmark!");
        var connection = connectionMock();
        given(connection.beginTransaction(any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .willReturn(CompletableFuture.completedStage(connection));
        setupConnectionAnswers(connection, List.of(handler -> {
            handler.onError(error);
            handler.onComplete();
        }));
        given(connection.close()).willReturn(CompletableFuture.completedStage(null));
        var apiTelemetryWork = new ApiTelemetryWork(TelemetryApi.UNMANAGED_TRANSACTION);
        var tx = new UnmanagedTransaction(
                connection,
                DatabaseNameUtil.defaultDatabase(),
                AccessMode.WRITE,
                null,
                (ignored) -> {},
                -1,
                null,
                apiTelemetryWork,
                Logging.none());

        var bookmarks = Collections.singleton(InternalBookmark.parse("SomeBookmark"));
        var txConfig = TransactionConfig.empty();

        var e = assertThrows(RuntimeException.class, () -> await(tx.beginAsync(bookmarks, txConfig, null, true)));

        assertEquals(error, e);
        verify(connection).close();
    }

    @Test
    void shouldNotReleaseConnectionWhenBeginSucceeds() {
        var connection = connectionMock();
        given(connection.beginTransaction(any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .willReturn(CompletableFuture.completedStage(connection));
        setupConnectionAnswers(connection, List.of(handler -> {
            handler.onBeginSummary(mock(BeginSummary.class));
            handler.onComplete();
        }));
        given(connection.close()).willReturn(CompletableFuture.completedStage(null));
        var apiTelemetryWork = new ApiTelemetryWork(TelemetryApi.UNMANAGED_TRANSACTION);
        var tx = new UnmanagedTransaction(
                connection,
                DatabaseNameUtil.defaultDatabase(),
                AccessMode.WRITE,
                null,
                (ignored) -> {},
                -1,
                null,
                apiTelemetryWork,
                Logging.none());

        var bookmarks = Collections.singleton(InternalBookmark.parse("SomeBookmark"));
        var txConfig = TransactionConfig.empty();

        await(tx.beginAsync(bookmarks, txConfig, null, true));

        verify(connection, never()).close();
    }

    @Test
    void shouldReleaseConnectionWhenTerminatedAndCommitted() {
        var connection = connectionMock();
        given(connection.close()).willReturn(CompletableFuture.completedStage(null));
        var apiTelemetryWork = new ApiTelemetryWork(TelemetryApi.UNMANAGED_TRANSACTION);
        var tx = new UnmanagedTransaction(
                connection,
                DatabaseNameUtil.defaultDatabase(),
                AccessMode.WRITE,
                null,
                (ignored) -> {},
                -1,
                null,
                apiTelemetryWork,
                Logging.none());

        tx.markTerminated(null);

        assertThrows(TransactionTerminatedException.class, () -> await(tx.commitAsync()));

        assertFalse(tx.isOpen());
        verify(connection).close();
    }

    @Test
    void shouldNotCreateCircularExceptionWhenTerminationCauseEqualsToCursorFailure() {
        var connection = connectionMock();
        given(connection.close()).willReturn(CompletableFuture.completedStage(null));
        var terminationCause = new ClientException("Custom exception");

        var apiTelemetryWork = new ApiTelemetryWork(TelemetryApi.UNMANAGED_TRANSACTION);
        var resultCursorsHolder = mockResultCursorWith(terminationCause);
        var tx = new UnmanagedTransaction(
                connection,
                DatabaseNameUtil.defaultDatabase(),
                AccessMode.WRITE,
                null,
                (ignored) -> {},
                -1,
                resultCursorsHolder,
                null,
                apiTelemetryWork,
                Logging.none());

        tx.markTerminated(terminationCause);

        var e = assertThrows(ClientException.class, () -> await(tx.commitAsync()));
        assertNoCircularReferences(e);
        assertEquals(terminationCause, e);
    }

    @Test
    void shouldNotCreateCircularExceptionWhenTerminationCauseDifferentFromCursorFailure() {
        var connection = connectionMock();
        given(connection.close()).willReturn(CompletableFuture.completedStage(null));
        var terminationCause = new ClientException("Custom exception");
        var resultCursorsHolder = mockResultCursorWith(new ClientException("Cursor error"));
        var apiTelemetryWork = new ApiTelemetryWork(TelemetryApi.UNMANAGED_TRANSACTION);
        var tx = new UnmanagedTransaction(
                connection,
                DatabaseNameUtil.defaultDatabase(),
                AccessMode.WRITE,
                null,
                (ignored) -> {},
                -1,
                resultCursorsHolder,
                null,
                apiTelemetryWork,
                Logging.none());

        tx.markTerminated(terminationCause);

        var e = assertThrows(ClientException.class, () -> await(tx.commitAsync()));
        assertNoCircularReferences(e);
        assertEquals(1, e.getSuppressed().length);

        var suppressed = e.getSuppressed()[0];
        assertEquals(terminationCause, suppressed.getCause());
    }

    @Test
    void shouldNotCreateCircularExceptionWhenTerminatedWithoutFailure() {
        var connection = connectionMock();
        given(connection.close()).willReturn(CompletableFuture.completedStage(null));
        var terminationCause = new ClientException("Custom exception");
        var apiTelemetryWork = new ApiTelemetryWork(TelemetryApi.UNMANAGED_TRANSACTION);
        var tx = new UnmanagedTransaction(
                connection,
                DatabaseNameUtil.defaultDatabase(),
                AccessMode.WRITE,
                null,
                (ignored) -> {},
                -1,
                null,
                apiTelemetryWork,
                Logging.none());

        tx.markTerminated(terminationCause);

        var e = assertThrows(TransactionTerminatedException.class, () -> await(tx.commitAsync()));
        assertNoCircularReferences(e);

        assertEquals(terminationCause, e.getCause());
    }

    @Test
    void shouldReleaseConnectionWhenTerminatedAndRolledBack() {
        var connection = connectionMock();
        given(connection.close()).willReturn(CompletableFuture.completedStage(null));
        var apiTelemetryWork = new ApiTelemetryWork(TelemetryApi.UNMANAGED_TRANSACTION);
        var tx = new UnmanagedTransaction(
                connection,
                DatabaseNameUtil.defaultDatabase(),
                AccessMode.WRITE,
                null,
                (ignored) -> {},
                -1,
                null,
                apiTelemetryWork,
                Logging.none());

        tx.markTerminated(null);
        await(tx.rollbackAsync());

        verify(connection).close();
    }

    @Test
    void shouldReleaseConnectionWhenClose() {
        var connection = connectionMock();
        given(connection.rollback()).willReturn(CompletableFuture.completedStage(connection));
        setupConnectionAnswers(connection, List.of(handler -> {
            handler.onRollbackSummary(mock(RollbackSummary.class));
            handler.onComplete();
        }));
        given(connection.close()).willReturn(CompletableFuture.completedStage(null));
        var apiTelemetryWork = new ApiTelemetryWork(TelemetryApi.UNMANAGED_TRANSACTION);
        var tx = new UnmanagedTransaction(
                connection,
                DatabaseNameUtil.defaultDatabase(),
                AccessMode.WRITE,
                null,
                (ignored) -> {},
                -1,
                null,
                apiTelemetryWork,
                Logging.none());

        await(tx.closeAsync());

        verify(connection).close();
    }

    @Test
    void shouldReleaseConnectionOnConnectionAuthorizationExpiredExceptionFailure() {
        var exception = new AuthorizationExpiredException("code", "message");
        var connection = connectionMock();
        given(connection.beginTransaction(any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .willReturn(CompletableFuture.completedStage(connection));
        setupConnectionAnswers(connection, List.of(handler -> {
            handler.onError(exception);
            handler.onComplete();
        }));
        given(connection.close()).willReturn(CompletableFuture.completedStage(null));
        var apiTelemetryWork = new ApiTelemetryWork(TelemetryApi.UNMANAGED_TRANSACTION);
        var tx = new UnmanagedTransaction(
                connection,
                DatabaseNameUtil.defaultDatabase(),
                AccessMode.WRITE,
                null,
                (ignored) -> {},
                -1,
                null,
                apiTelemetryWork,
                Logging.none());
        var bookmarks = Collections.singleton(InternalBookmark.parse("SomeBookmark"));
        var txConfig = TransactionConfig.empty();

        var actualException = assertThrows(
                AuthorizationExpiredException.class, () -> await(tx.beginAsync(bookmarks, txConfig, null, true)));

        assertSame(exception, actualException);
        verify(connection).close();
    }

    @Test
    void shouldReleaseConnectionOnConnectionReadTimeoutExceptionFailure() {
        var connection = connectionMock();
        given(connection.beginTransaction(any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .willReturn(CompletableFuture.completedStage(connection));
        setupConnectionAnswers(connection, List.of(handler -> {
            handler.onError(ConnectionReadTimeoutException.INSTANCE);
            handler.onComplete();
        }));
        given(connection.close()).willReturn(CompletableFuture.completedStage(null));
        var apiTelemetryWork = new ApiTelemetryWork(TelemetryApi.UNMANAGED_TRANSACTION);
        var tx = new UnmanagedTransaction(
                connection,
                DatabaseNameUtil.defaultDatabase(),
                AccessMode.WRITE,
                null,
                (ignored) -> {},
                -1,
                null,
                apiTelemetryWork,
                Logging.none());
        var bookmarks = Collections.singleton(InternalBookmark.parse("SomeBookmark"));
        var txConfig = TransactionConfig.empty();

        var actualException = assertThrows(
                ConnectionReadTimeoutException.class, () -> await(tx.beginAsync(bookmarks, txConfig, null, true)));

        assertSame(ConnectionReadTimeoutException.INSTANCE, actualException);
        verify(connection).close();
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
        var connection = connectionMock();
        given(connection.commit()).willReturn(CompletableFuture.completedStage(connection));
        given(connection.rollback()).willReturn(CompletableFuture.completedStage(connection));
        given(connection.flush(any())).willReturn(CompletableFuture.completedStage(null));
        given(connection.close()).willReturn(CompletableFuture.completedStage(null));
        var apiTelemetryWork = new ApiTelemetryWork(TelemetryApi.UNMANAGED_TRANSACTION);
        var tx = new UnmanagedTransaction(
                connection,
                DatabaseNameUtil.defaultDatabase(),
                AccessMode.WRITE,
                null,
                (ignored) -> {},
                -1,
                null,
                apiTelemetryWork,
                Logging.none());

        var initialStage = mapTransactionAction(initialAction, tx).get();
        var similarStage = mapTransactionAction(similarAction, tx).get();

        assertSame(initialStage, similarStage);
        if (protocolCommit) {
            then(connection).should(times(1)).commit();
        } else {
            then(connection).should(times(1)).rollback();
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
        var connection = connectionMock();
        given(connection.commit()).willReturn(CompletableFuture.completedStage(connection));
        given(connection.rollback()).willReturn(CompletableFuture.completedStage(connection));
        if (protocolActionCompleted) {
            setupConnectionAnswers(connection, List.of(handler -> {
                if (protocolCommit) {
                    handler.onCommitSummary(mock(CommitSummary.class));
                } else {
                    handler.onRollbackSummary(mock(RollbackSummary.class));
                }
                handler.onComplete();
            }));
        } else {
            given(connection.flush(any())).willReturn(CompletableFuture.completedStage(null));
        }
        given(connection.close()).willReturn(CompletableFuture.completedStage(null));
        var apiTelemetryWork = new ApiTelemetryWork(TelemetryApi.UNMANAGED_TRANSACTION);
        var tx = new UnmanagedTransaction(
                connection,
                DatabaseNameUtil.defaultDatabase(),
                AccessMode.WRITE,
                null,
                (ignored) -> {},
                -1,
                null,
                apiTelemetryWork,
                Logging.none());

        var originalActionStage = mapTransactionAction(initialAction, tx).get();
        var conflictingActionStage = mapTransactionAction(conflictingAction, tx).get();

        assertNotNull(originalActionStage);
        if (protocolCommit) {
            then(connection).should().commit();
        } else {
            then(connection).should().rollback();
        }
        then(connection).should().flush(any());
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
        var connection = connectionMock();
        given(connection.commit()).willReturn(CompletableFuture.completedStage(connection));
        given(connection.rollback()).willReturn(CompletableFuture.completedStage(connection));
        setupConnectionAnswers(connection, List.of(handler -> {
            if (protocolCommit) {
                handler.onCommitSummary(mock(CommitSummary.class));
            } else {
                handler.onRollbackSummary(mock(RollbackSummary.class));
            }
            handler.onComplete();
        }));
        given(connection.close()).willReturn(CompletableFuture.completedStage(null));
        var apiTelemetryWork = new ApiTelemetryWork(TelemetryApi.UNMANAGED_TRANSACTION);
        var tx = new UnmanagedTransaction(
                connection,
                DatabaseNameUtil.defaultDatabase(),
                AccessMode.WRITE,
                null,
                (ignored) -> {},
                -1,
                null,
                apiTelemetryWork,
                Logging.none());

        var originalActionStage = mapTransactionAction(originalAction, tx).get();
        var closeStage = commitOnClose != null ? tx.closeAsync(commitOnClose) : tx.closeAsync();

        assertTrue(originalActionStage.toCompletableFuture().isDone());
        assertFalse(originalActionStage.toCompletableFuture().isCompletedExceptionally());
        if (protocolCommit) {
            then(connection).should(times(expectedProtocolInvocations)).commit();
        } else {
            then(connection).should(times(expectedProtocolInvocations)).rollback();
        }
        then(connection).should(times(expectedProtocolInvocations)).flush(any());
        assertNull(closeStage.toCompletableFuture().join());
    }

    @Test
    void shouldTerminateOnTerminateAsync() {
        // Given
        var connection = connectionMock(new BoltProtocolVersion(4, 0));
        given(connection.beginTransaction(any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .willReturn(CompletableFuture.completedStage(connection));
        given(connection.clear()).willReturn(CompletableFuture.completedStage(connection));
        given(connection.reset()).willReturn(CompletableFuture.completedStage(connection));
        setupConnectionAnswers(
                connection,
                List.of(
                        handler -> {
                            handler.onBeginSummary(mock(BeginSummary.class));
                            handler.onComplete();
                        },
                        handler -> {
                            handler.onResetSummary(mock(ResetSummary.class));
                            handler.onComplete();
                        }));
        var tx = beginTx(connection);

        // When
        await(tx.terminateAsync());

        // Then
        then(connection).should().clear();
        then(connection).should().reset();
    }

    @Test
    void shouldServeTheSameStageOnTerminateAsync() {
        // Given
        var connection = connectionMock(new BoltProtocolVersion(4, 0));
        given(connection.beginTransaction(any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .willReturn(CompletableFuture.completedStage(connection));
        given(connection.clear()).willReturn(CompletableFuture.completedStage(connection));
        given(connection.reset()).willReturn(CompletableFuture.completedStage(connection));
        setupConnectionAnswers(
                connection,
                List.of(
                        handler -> {
                            handler.onBeginSummary(mock(BeginSummary.class));
                            handler.onComplete();
                        },
                        handler -> {
                            handler.onResetSummary(mock(ResetSummary.class));
                            handler.onComplete();
                        }));
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
        var connection = connectionMock(new BoltProtocolVersion(4, 0));
        given(connection.beginTransaction(any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .willReturn(CompletableFuture.completedStage(connection));
        given(connection.run(any(), any())).willReturn(CompletableFuture.completedStage(connection));
        given(connection.pull(anyLong(), anyLong())).willReturn(CompletableFuture.completedStage(connection));
        var exception = new Neo4jException("message");
        setupConnectionAnswers(
                connection,
                List.of(
                        handler -> {
                            handler.onBeginSummary(mock(BeginSummary.class));
                            handler.onComplete();
                        },
                        handler -> {
                            handler.onError(exception);
                            handler.onComplete();
                        }));
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
        var connection = connectionMock();
        given(connection.beginTransaction(any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .willReturn(CompletableFuture.completedStage(connection));
        given(connection.commit()).willReturn(CompletableFuture.completedStage(connection));
        given(connection.rollback()).willReturn(CompletableFuture.completedStage(connection));
        setupConnectionAnswers(
                connection,
                List.of(
                        handler -> {
                            handler.onBeginSummary(mock(BeginSummary.class));
                            handler.onComplete();
                        },
                        handler -> {}));
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

    private static UnmanagedTransaction beginTx(BoltConnection connection) {
        return beginTx(connection, Collections.emptySet());
    }

    private static UnmanagedTransaction beginTx(BoltConnection connection, Set<Bookmark> initialBookmarks) {
        var apiTelemetryWork = new ApiTelemetryWork(TelemetryApi.UNMANAGED_TRANSACTION);
        var tx = new UnmanagedTransaction(
                connection,
                DatabaseNameUtil.defaultDatabase(),
                AccessMode.WRITE,
                null,
                (ignored) -> {},
                -1,
                null,
                apiTelemetryWork,
                Logging.none());
        return await(tx.beginAsync(initialBookmarks, TransactionConfig.empty(), null, true));
    }

    private ResultCursorsHolder mockResultCursorWith(ClientException clientException) {
        var resultCursorsHolder = new ResultCursorsHolder();
        var cursor = mock(FailableCursor.class);
        given(cursor.consumed()).willReturn(new CompletableFuture<>());
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
