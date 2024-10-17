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
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.driver.AccessMode.READ;
import static org.neo4j.driver.AccessMode.WRITE;
import static org.neo4j.driver.testutil.TestUtil.await;
import static org.neo4j.driver.testutil.TestUtil.connectionMock;
import static org.neo4j.driver.testutil.TestUtil.newSession;
import static org.neo4j.driver.testutil.TestUtil.setupConnectionAnswers;
import static org.neo4j.driver.testutil.TestUtil.setupSuccessfulAutocommitRunAndPull;
import static org.neo4j.driver.testutil.TestUtil.verifyAutocommitRunAndPull;
import static org.neo4j.driver.testutil.TestUtil.verifyAutocommitRunRx;
import static org.neo4j.driver.testutil.TestUtil.verifyRollbackTx;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Query;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.InternalBookmark;
import org.neo4j.driver.internal.bolt.api.BoltConnection;
import org.neo4j.driver.internal.bolt.api.BoltConnectionProvider;
import org.neo4j.driver.internal.bolt.api.BoltProtocolVersion;
import org.neo4j.driver.internal.bolt.api.DatabaseName;
import org.neo4j.driver.internal.bolt.api.ResponseHandler;
import org.neo4j.driver.internal.bolt.api.TelemetryApi;
import org.neo4j.driver.internal.bolt.api.summary.BeginSummary;
import org.neo4j.driver.internal.bolt.api.summary.PullSummary;
import org.neo4j.driver.internal.bolt.api.summary.ResetSummary;
import org.neo4j.driver.internal.bolt.api.summary.RollbackSummary;
import org.neo4j.driver.internal.bolt.api.summary.RunSummary;
import org.neo4j.driver.internal.telemetry.ApiTelemetryWork;
import org.neo4j.driver.internal.util.FixedRetryLogic;

class NetworkSessionTest {
    private BoltConnection connection;
    private BoltConnectionProvider connectionProvider;
    private NetworkSession session;

    @BeforeEach
    void setUp() {
        connection = connectionMock(new BoltProtocolVersion(5, 4));
        given(connection.close()).willReturn(completedFuture(null));
        connectionProvider = mock(BoltConnectionProvider.class);
        given(connectionProvider.connect(any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .willAnswer((Answer<CompletionStage<BoltConnection>>) invocation -> {
                    var database = (DatabaseName) invocation.getArguments()[1];
                    @SuppressWarnings("unchecked")
                    var databaseConsumer = (Consumer<DatabaseName>) invocation.getArguments()[8];
                    databaseConsumer.accept(database);
                    return completedFuture(connection);
                });
        session = newSession(connectionProvider);
    }

    @Test
    void shouldFlushOnRunAsync() {
        setupSuccessfulAutocommitRunAndPull(connection);
        await(session.runAsync(new Query("RETURN 1"), TransactionConfig.empty()));

        verifyAutocommitRunAndPull(connection, "RETURN 1");
    }

    @Test
    void shouldFlushOnRunRx() {
        setupSuccessfulAutocommitRunAndPull(connection);
        await(session.runRx(new Query("RETURN 1"), TransactionConfig.empty(), CompletableFuture.completedStage(null)));

        verifyAutocommitRunRx(connection, "RETURN 1");
    }

    @Test
    void shouldNotAllowNewTxWhileOneIsRunning() {
        // Given
        setupSuccessfulBegin(connection);
        beginTransaction(session);

        // Expect
        assertThrows(ClientException.class, () -> beginTransaction(session));
    }

    @Test
    void shouldBeAbleToOpenTxAfterPreviousIsClosed() {
        // Given
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
                        },
                        handler -> {
                            handler.onBeginSummary(mock(BeginSummary.class));
                            handler.onComplete();
                        }));
        await(beginTransaction(session).closeAsync());

        // When
        var tx = beginTransaction(session);

        // Then we should've gotten a transaction object back
        assertNotNull(tx);
        verifyRollbackTx(connection);
    }

    @Test
    void shouldNotBeAbleToUseSessionWhileOngoingTransaction() {
        // Given
        setupSuccessfulBegin(connection);
        beginTransaction(session);

        // Expect
        assertThrows(ClientException.class, () -> run(session, "RETURN 1"));
    }

    @Test
    void shouldBeAbleToUseSessionAgainWhenTransactionIsClosed() {
        // Given
        given(connection.beginTransaction(any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .willReturn(completedFuture(connection));
        given(connection.rollback()).willReturn(CompletableFuture.completedFuture(connection));
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
        await(beginTransaction(session).closeAsync());
        Mockito.reset(connection);
        setupSuccessfulAutocommitRunAndPull(connection);
        given(connection.protocolVersion()).willReturn(new BoltProtocolVersion(5, 5));
        given(connection.close()).willReturn(CompletableFuture.completedFuture(null));
        var query = "RETURN 1";

        // When
        run(session, query);

        // Then
        verifyAutocommitRunAndPull(connection, query);
    }

    @Test
    void shouldNotCloseAlreadyClosedSession() {
        given(connection.beginTransaction(any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .willReturn(completedFuture(connection));
        given(connection.rollback()).willReturn(CompletableFuture.completedFuture(connection));
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
        beginTransaction(session);

        close(session);
        close(session);
        close(session);

        verifyRollbackTx(connection);
    }

    @Test
    void runThrowsWhenSessionIsClosed() {
        close(session);

        var e = assertThrows(Exception.class, () -> run(session, "CREATE ()"));
        assertThat(e, instanceOf(ClientException.class));
        assertThat(e.getMessage(), containsString("session is already closed"));
    }

    @Test
    void acquiresNewConnectionForRun() {
        var query = "RETURN 1";
        setupSuccessfulAutocommitRunAndPull(connection);

        run(session, query);

        verify(connectionProvider).connect(any(), any(), any(), any(), any(), any(), any(), any(), any());
    }

    @Test
    void releasesOpenConnectionUsedForRunWhenSessionIsClosed() {
        var query = "RETURN 1";
        setupSuccessfulAutocommitRunAndPull(connection);

        run(session, query);

        close(session);
        then(connection).should(atLeastOnce()).close();
    }

    @Test
    void resetDoesNothingWhenNoTransactionAndNoConnection() {
        await(session.resetAsync());

        verify(connectionProvider, never()).connect(any(), any(), any(), any(), any(), any(), any(), any(), any());
    }

    @Test
    void closeWithoutConnection() {
        var session = newSession(connectionProvider);

        close(session);

        verify(connectionProvider, never()).connect(any(), any(), any(), any(), any(), any(), any(), any(), any());
    }

    @Test
    void acquiresNewConnectionForBeginTx() {
        setupSuccessfulBegin(connection);
        var tx = beginTransaction(session);

        assertNotNull(tx);
        verify(connectionProvider).connect(any(), any(), any(), any(), any(), any(), any(), any(), any());
    }

    @Test
    void updatesBookmarkWhenTxIsClosed() {
        var bookmarkAfterCommit = InternalBookmark.parse("TheBookmark");
        given(connection.beginTransaction(any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .willReturn(completedFuture(connection));
        given(connection.commit()).willReturn(CompletableFuture.completedFuture(connection));
        setupConnectionAnswers(
                connection,
                List.of(
                        handler -> {
                            handler.onBeginSummary(mock(BeginSummary.class));
                            handler.onComplete();
                        },
                        handler -> {
                            handler.onCommitSummary(() -> Optional.of(bookmarkAfterCommit.value()));
                            handler.onComplete();
                        }));

        var tx = beginTransaction(session);
        assertThat(session.lastBookmarks(), instanceOf(Set.class));
        var bookmarks = session.lastBookmarks();
        assertTrue(bookmarks.isEmpty());

        await(tx.commitAsync());
        assertEquals(Collections.singleton(bookmarkAfterCommit), session.lastBookmarks());
    }

    @Test
    void releasesConnectionWhenTxIsClosed() {
        given(connection.beginTransaction(any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .willReturn(completedFuture(connection));
        given(connection.run(any(), any())).willAnswer((Answer<CompletionStage<BoltConnection>>)
                invocation -> CompletableFuture.completedStage(connection));
        given(connection.pull(anyLong(), anyLong())).willAnswer((Answer<CompletionStage<BoltConnection>>)
                invocation -> CompletableFuture.completedStage(connection));
        given(connection.rollback()).willReturn(CompletableFuture.completedFuture(connection));
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
                        },
                        handler -> {
                            handler.onRollbackSummary(mock(RollbackSummary.class));
                            handler.onComplete();
                        }));
        var tx = beginTransaction(session);
        verify(connectionProvider).connect(any(), any(), any(), any(), any(), any(), any(), any(), any());
        then(connection).should().flush(any());
        var query = "RETURN 42";
        await(tx.runAsync(new Query(query)));

        then(connection).should().run(eq(query), any());
        then(connection).should().pull(anyLong(), anyLong());
        then(connection).should(times(2)).flush(any());

        await(tx.closeAsync());
        verify(connection).close();
    }

    @Test
    void bookmarkIsPropagatedFromSession() {
        var bookmarks = Collections.singleton(InternalBookmark.parse("Bookmarks"));
        var session = newSession(connectionProvider, bookmarks);
        setupSuccessfulBegin(connection);

        var tx = beginTransaction(session);
        assertNotNull(tx);
        then(connection).should().beginTransaction(any(), any(), any(), any(), any(), any(), any(), any(), any());
        then(connection).should().flush(any());
    }

    @Test
    void bookmarkIsPropagatedBetweenTransactions() {
        var bookmark1 = InternalBookmark.parse("Bookmark1");
        var bookmark2 = InternalBookmark.parse("Bookmark2");

        var session = newSession(connectionProvider);

        given(connection.beginTransaction(any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .willReturn(completedFuture(connection));
        given(connection.commit()).willReturn(CompletableFuture.completedFuture(connection));
        setupConnectionAnswers(
                connection,
                List.of(
                        handler -> {
                            handler.onBeginSummary(mock(BeginSummary.class));
                            handler.onComplete();
                        },
                        handler -> {
                            handler.onCommitSummary(() -> Optional.of(bookmark1.value()));
                            handler.onComplete();
                        },
                        handler -> {
                            handler.onBeginSummary(mock(BeginSummary.class));
                            handler.onComplete();
                        },
                        handler -> {
                            handler.onCommitSummary(() -> Optional.of(bookmark2.value()));
                            handler.onComplete();
                        }));

        var tx1 = beginTransaction(session);
        await(tx1.commitAsync());
        assertEquals(Collections.singleton(bookmark1), session.lastBookmarks());

        var tx2 = beginTransaction(session);
        then(connection)
                .should(times(2))
                .beginTransaction(any(), any(), any(), any(), any(), any(), any(), any(), any());
        then(connection).should(times(3)).flush(any());
        await(tx2.commitAsync());

        assertEquals(Collections.singleton(bookmark2), session.lastBookmarks());
    }

    @Test
    void accessModeUsedToAcquireReadConnections() {
        setupSuccessfulBegin(connection);
        accessModeUsedToAcquireConnections(READ);
    }

    @Test
    void accessModeUsedToAcquireWriteConnections() {
        setupSuccessfulBegin(connection);
        accessModeUsedToAcquireConnections(WRITE);
    }

    private void accessModeUsedToAcquireConnections(AccessMode mode) {
        var session2 = newSession(connectionProvider, mode);
        beginTransaction(session2);
        var argument = ArgumentCaptor.forClass(org.neo4j.driver.internal.bolt.api.AccessMode.class);
        verify(connectionProvider).connect(any(), any(), any(), argument.capture(), any(), any(), any(), any(), any());
        assertEquals(
                switch (mode) {
                    case READ -> org.neo4j.driver.internal.bolt.api.AccessMode.READ;
                    case WRITE -> org.neo4j.driver.internal.bolt.api.AccessMode.WRITE;
                },
                argument.getValue());
    }

    @Test
    void testPassingNoBookmarkShouldRetainBookmark() {
        var bookmarks = Collections.singleton(InternalBookmark.parse("X"));
        var session = newSession(connectionProvider, bookmarks);
        setupSuccessfulBegin(connection);
        beginTransaction(session);
        assertThat(session.lastBookmarks(), equalTo(bookmarks));
    }

    @Test
    void shouldHaveEmptyLastBookmarksInitially() {
        assertTrue(session.lastBookmarks().isEmpty());
    }

    @Test
    void shouldDoNothingWhenClosingWithoutAcquiredConnection() {
        var error = new RuntimeException("Hi");
        Mockito.reset(connectionProvider);
        given(connectionProvider.connect(any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .willReturn(failedFuture(error));

        var e = assertThrows(Exception.class, () -> run(session, "RETURN 1"));
        assertEquals(error, e);

        close(session);
    }

    @Test
    void shouldRunAfterRunFailure() {
        var error = new RuntimeException("Hi");
        Mockito.reset(connectionProvider);
        given(connectionProvider.connect(any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .willReturn(failedFuture(error))
                .willAnswer((Answer<CompletionStage<BoltConnection>>) invocation -> {
                    var databaseName = (DatabaseName) invocation.getArguments()[1];
                    @SuppressWarnings("unchecked")
                    var databaseNameConsumer =
                            (Consumer<DatabaseName>) invocation.getArguments()[8];
                    databaseNameConsumer.accept(databaseName);
                    return completedFuture(connection);
                });

        var e = assertThrows(Exception.class, () -> run(session, "RETURN 1"));

        assertEquals(error, e);

        var query = "RETURN 2";
        setupSuccessfulAutocommitRunAndPull(connection);

        run(session, query);

        verify(connectionProvider, times(2)).connect(any(), any(), any(), any(), any(), any(), any(), any(), any());
        verifyAutocommitRunAndPull(connection, query);
    }

    @Test
    void shouldRunAfterBeginTxFailureOnBookmark() {
        var error = new RuntimeException("Hi");
        var connection1 = connectionMock(new BoltProtocolVersion(5, 0));
        given(connection1.beginTransaction(any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .willReturn(CompletableFuture.failedStage(error));
        given(connection1.close()).willReturn(CompletableFuture.completedStage(null));
        var connection2 = connectionMock(new BoltProtocolVersion(5, 0));
        given(connection2.close()).willReturn(CompletableFuture.completedStage(null));

        Mockito.reset(connectionProvider);
        given(connectionProvider.connect(any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .willAnswer((Answer<CompletionStage<BoltConnection>>) invocation -> {
                    var databaseName = (DatabaseName) invocation.getArguments()[1];
                    @SuppressWarnings("unchecked")
                    var databaseNameConsumer =
                            (Consumer<DatabaseName>) invocation.getArguments()[8];
                    databaseNameConsumer.accept(databaseName);
                    return completedFuture(connection1);
                })
                .willAnswer((Answer<CompletionStage<BoltConnection>>) invocation -> {
                    var databaseName = (DatabaseName) invocation.getArguments()[1];
                    @SuppressWarnings("unchecked")
                    var databaseNameConsumer =
                            (Consumer<DatabaseName>) invocation.getArguments()[8];
                    databaseNameConsumer.accept(databaseName);
                    return completedFuture(connection2);
                });

        var bookmarks = Collections.singleton(InternalBookmark.parse("neo4j:bookmark:v1:tx42"));
        var session = newSession(connectionProvider, bookmarks);

        var e = assertThrows(Exception.class, () -> beginTransaction(session));
        assertEquals(error, e);
        var query = "RETURN 2";
        setupSuccessfulAutocommitRunAndPull(connection2);

        run(session, query);

        verify(connectionProvider, times(2)).connect(any(), any(), any(), any(), any(), any(), any(), any(), any());
        then(connection1).should().beginTransaction(any(), any(), any(), any(), any(), any(), any(), any(), any());
        verifyAutocommitRunAndPull(connection2, "RETURN 2");
    }

    @Test
    void shouldBeginTxAfterBeginTxFailureOnBookmark() {
        var error = new RuntimeException("Hi");
        var connection1 = connectionMock(new BoltProtocolVersion(5, 0));
        given(connection1.beginTransaction(any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .willReturn(CompletableFuture.failedStage(error));
        var connection2 = connectionMock(new BoltProtocolVersion(5, 0));
        given(connection2.beginTransaction(any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .willReturn(CompletableFuture.completedStage(connection2));
        setupConnectionAnswers(connection2, List.of(handler -> {
            handler.onBeginSummary(mock(BeginSummary.class));
            handler.onComplete();
        }));

        Mockito.reset(connectionProvider);
        given(connectionProvider.connect(any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .willAnswer((Answer<CompletionStage<BoltConnection>>) invocation -> {
                    var databaseName = (DatabaseName) invocation.getArguments()[1];
                    @SuppressWarnings("unchecked")
                    var databaseNameConsumer =
                            (Consumer<DatabaseName>) invocation.getArguments()[8];
                    databaseNameConsumer.accept(databaseName);
                    return completedFuture(connection1);
                })
                .willAnswer((Answer<CompletionStage<BoltConnection>>) invocation -> {
                    var databaseName = (DatabaseName) invocation.getArguments()[1];
                    @SuppressWarnings("unchecked")
                    var databaseNameConsumer =
                            (Consumer<DatabaseName>) invocation.getArguments()[8];
                    databaseNameConsumer.accept(databaseName);
                    return completedFuture(connection2);
                });

        var bookmarks = Collections.singleton(InternalBookmark.parse("neo4j:bookmark:v1:tx42"));
        var session = newSession(connectionProvider, bookmarks);

        var e = assertThrows(Exception.class, () -> beginTransaction(session));
        assertEquals(error, e);

        beginTransaction(session);

        verify(connectionProvider, times(2)).connect(any(), any(), any(), any(), any(), any(), any(), any(), any());
        then(connection1).should().beginTransaction(any(), any(), any(), any(), any(), any(), any(), any(), any());
        then(connection2).should().beginTransaction(any(), any(), any(), any(), any(), any(), any(), any(), any());
    }

    @Test
    void shouldBeginTxAfterRunFailureToAcquireConnection() {
        var error = new RuntimeException("Hi");
        Mockito.reset(connectionProvider);
        given(connectionProvider.connect(any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .willReturn(failedFuture(error))
                .willAnswer((Answer<CompletionStage<BoltConnection>>) invocation -> {
                    var databaseName = (DatabaseName) invocation.getArguments()[1];
                    @SuppressWarnings("unchecked")
                    var databaseNameConsumer =
                            (Consumer<DatabaseName>) invocation.getArguments()[8];
                    databaseNameConsumer.accept(databaseName);
                    return completedFuture(connection);
                });
        setupSuccessfulBegin(connection);

        var e = assertThrows(Exception.class, () -> run(session, "RETURN 1"));
        assertEquals(error, e);

        beginTransaction(session);

        verify(connectionProvider, times(2)).connect(any(), any(), any(), any(), any(), any(), any(), any(), any());
        then(connection).should().beginTransaction(any(), any(), any(), any(), any(), any(), any(), any(), any());
    }

    @Test
    void shouldMarkTransactionAsTerminatedAndThenResetConnectionOnReset() {
        given(connection.beginTransaction(any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .willReturn(completedFuture(connection));
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
        var tx = beginTransaction(session);

        assertTrue(tx.isOpen());
        verify(connection, never()).reset();

        await(session.resetAsync());

        verify(connection).reset();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldSendTelemetryIfEnabledOnBegin(boolean telemetryDisabled) {
        // given
        var session = newSession(connectionProvider, WRITE, new FixedRetryLogic(0), Set.of(), telemetryDisabled);
        given(connection.telemetrySupported()).willReturn(true);
        given(connection.telemetry(any())).willReturn(CompletableFuture.completedStage(connection));
        setupSuccessfulBegin(connection);

        // when
        beginTransaction(session);

        // then
        if (telemetryDisabled) {
            then(connection).should(never()).telemetry(any());
        } else {
            then(connection).should().telemetry(eq(TelemetryApi.UNMANAGED_TRANSACTION));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldSendTelemetryIfEnabledOnRun(boolean telemetryDisabled) {
        // given
        var query = "RETURN 1";
        setupSuccessfulAutocommitRunAndPull(connection);
        var session = newSession(connectionProvider, WRITE, new FixedRetryLogic(0), Set.of(), telemetryDisabled);
        given(connection.telemetrySupported()).willReturn(true);
        given(connection.telemetry(any())).willReturn(CompletableFuture.completedStage(connection));

        // when
        run(session, query);

        // then
        if (telemetryDisabled) {
            then(connection).should(never()).telemetry(any());
        } else {
            then(connection).should().telemetry(eq(TelemetryApi.AUTO_COMMIT_TRANSACTION));
        }
    }

    private void setupSuccessfulBegin(BoltConnection connection) {
        given(connection.beginTransaction(any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .willReturn(completedFuture(connection));
        given(connection.flush(any())).willAnswer((Answer<CompletionStage<Void>>) invocation -> {
            var handler = (ResponseHandler) invocation.getArguments()[0];
            handler.onBeginSummary(mock(BeginSummary.class));
            handler.onComplete();
            return completedFuture(null);
        });
    }

    private static void run(NetworkSession session, String query) {
        await(session.runAsync(new Query(query), TransactionConfig.empty()));
    }

    private static UnmanagedTransaction beginTransaction(NetworkSession session) {
        var apiTelemetryWork = new ApiTelemetryWork(TelemetryApi.UNMANAGED_TRANSACTION);
        return await(session.beginTransactionAsync(TransactionConfig.empty(), apiTelemetryWork));
    }

    private static void close(NetworkSession session) {
        await(session.closeAsync());
    }
}
