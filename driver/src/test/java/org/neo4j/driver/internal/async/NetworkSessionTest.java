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
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.neo4j.driver.AccessMode.READ;
import static org.neo4j.driver.AccessMode.WRITE;
import static org.neo4j.driver.testutil.TestUtil.await;
import static org.neo4j.driver.testutil.TestUtil.connectionMock;
import static org.neo4j.driver.testutil.TestUtil.newSession;
import static org.neo4j.driver.testutil.TestUtil.setupSuccessfulRunAndPull;
import static org.neo4j.driver.testutil.TestUtil.verifyRollbackTx;
import static org.neo4j.driver.testutil.TestUtil.verifyRunAndPull;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Query;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.InternalBookmark;
import org.neo4j.driver.internal.bolt.api.BoltConnection;
import org.neo4j.driver.internal.bolt.api.BoltConnectionProvider;
import org.neo4j.driver.internal.bolt.api.BoltProtocolVersion;
import org.neo4j.driver.internal.bolt.api.ResponseHandler;
import org.neo4j.driver.internal.bolt.api.TelemetryApi;
import org.neo4j.driver.internal.telemetry.ApiTelemetryWork;

class NetworkSessionTest {
    private static final String DATABASE = "neo4j";
    private BoltConnection connection;
    private BoltConnectionProvider connectionProvider;
    private NetworkSession session;

    @BeforeEach
    void setUp() {
        connection = connectionMock(new BoltProtocolVersion(4, 0));
        connectionProvider = mock(BoltConnectionProvider.class);
        given(connectionProvider.connect(any(), any(), any(), any(), any(), any(), any(), any()))
                .willReturn(completedFuture(connection));
        session = newSession(connectionProvider);
    }

    @Test
    void shouldFlushOnRunAsync() {
        setupSuccessfulRunAndPull(connection);
        await(session.runAsync(new Query("RETURN 1"), TransactionConfig.empty()));

        verifyRunAndPull(connection, "RETURN 1");
    }

    //    @Test
    //    void shouldFlushOnRunRx() {
    //        setupSuccessfulRunRx(connection);
    //        await(session.runRx(new Query("RETURN 1"), TransactionConfig.empty(),
    // CompletableFuture.completedStage(null)));
    //
    //        verifyRunRx(connection, "RETURN 1");
    //    }

    @Test
    void shouldNotAllowNewTxWhileOneIsRunning() {
        // Given
        beginTransaction(session);

        // Expect
        assertThrows(ClientException.class, () -> beginTransaction(session));
    }

    @Test
    void shouldBeAbleToOpenTxAfterPreviousIsClosed() {
        // Given
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
        beginTransaction(session);

        // Expect
        assertThrows(ClientException.class, () -> run(session, "RETURN 1"));
    }

    @Test
    void shouldBeAbleToUseSessionAgainWhenTransactionIsClosed() {
        // Given
        stubBeginTransaction();
        beginTransaction(session);
        given(connection.rollback()).willReturn(CompletableFuture.completedFuture(connection));
        given(connection.close()).willReturn(completedFuture(null));
        //        await(.closeAsync());
        var query = "RETURN 1";
        setupSuccessfulRunAndPull(connection);

        // When
        run(session, query);

        // Then
        verifyRunAndPull(connection, query);
    }

    @Test
    void shouldNotCloseAlreadyClosedSession() {
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
        setupSuccessfulRunAndPull(connection);

        run(session, query);

        verify(connectionProvider).connect(any(), any(), any(), any(), any(), any(), any(), any());
    }

    @Test
    void releasesOpenConnectionUsedForRunWhenSessionIsClosed() {
        var query = "RETURN 1";
        setupSuccessfulRunAndPull(connection);

        run(session, query);

        close(session);

        //        var inOrder = inOrder(connection);
        //        inOrder.verify(connection).write(any(RunWithMetadataMessage.class), any());
        //        inOrder.verify(connection).writeAndFlush(any(PullMessage.class), any());
        //        inOrder.verify(connection, atLeastOnce()).release();
    }

    @Test
    void resetDoesNothingWhenNoTransactionAndNoConnection() {
        await(session.resetAsync());

        verify(connectionProvider, never()).connect(any(), any(), any(), any(), any(), any(), any(), any());
    }

    @Test
    void closeWithoutConnection() {
        var session = newSession(connectionProvider);

        close(session);

        verify(connectionProvider, never()).connect(any(), any(), any(), any(), any(), any(), any(), any());
    }

    @Test
    void acquiresNewConnectionForBeginTx() {
        stubBeginTransaction();
        var tx = beginTransaction(session);

        assertNotNull(tx);
        verify(connectionProvider).connect(any(), any(), any(), any(), any(), any(), any(), any());
    }

    //    @Test
    //    void updatesBookmarkWhenTxIsClosed() {
    //        var bookmarkAfterCommit = InternalBookmark.parse("TheBookmark");
    //
    //        var protocol = spy(BoltProtocolV4.INSTANCE);
    //        doReturn(completedFuture(new DatabaseBookmark(DATABASE, bookmarkAfterCommit)))
    //                .when(protocol)
    //                .commitTransaction(any(Connection.class));
    //
    //        when(connection.protocol()).thenReturn(protocol);
    //
    //        var tx = beginTransaction(session);
    //        assertThat(session.lastBookmarks(), instanceOf(Set.class));
    //        var bookmarks = session.lastBookmarks();
    //        assertTrue(bookmarks.isEmpty());
    //
    //        await(tx.commitAsync());
    //        assertEquals(Collections.singleton(bookmarkAfterCommit), session.lastBookmarks());
    //    }

    @Test
    void releasesConnectionWhenTxIsClosed() {
        var query = "RETURN 42";
        setupSuccessfulRunAndPull(connection);

        var tx = beginTransaction(session);
        await(tx.runAsync(new Query(query)));

        verify(connectionProvider).connect(any(), any(), any(), any(), any(), any(), any(), any());
        verifyRunAndPull(connection, query);

        await(tx.closeAsync());
        verify(connection).close();
    }

    @Test
    void bookmarkIsPropagatedFromSession() {
        var bookmarks = Collections.singleton(InternalBookmark.parse("Bookmarks"));
        var session = newSession(connectionProvider, bookmarks);

        var tx = beginTransaction(session);
        assertNotNull(tx);
        then(connection).should().beginTransaction(any(), any(), any(), any(), any(), any(), any(), any());
        then(connection).should().flush(any());
    }

    //    @Test
    //    void bookmarkIsPropagatedBetweenTransactions() {
    //        var bookmark1 = InternalBookmark.parse("Bookmark1");
    //        var bookmark2 = InternalBookmark.parse("Bookmark2");
    //
    //        var session = newSession(connectionProvider);
    //
    //        var protocol = spy(BoltProtocolV4.INSTANCE);
    //        doReturn(
    //                completedFuture(new DatabaseBookmark(DATABASE, bookmark1)),
    //                completedFuture(new DatabaseBookmark(DATABASE, bookmark2)))
    //                .when(protocol)
    //                .commitTransaction(any(Connection.class));
    //
    //        when(connection.protocol()).thenReturn(protocol);
    //
    //        var tx1 = beginTransaction(session);
    //        await(tx1.commitAsync());
    //        assertEquals(Collections.singleton(bookmark1), session.lastBookmarks());
    //
    //        var tx2 = beginTransaction(session);
    //        verifyBeginTx(connection, 2);
    //        await(tx2.commitAsync());
    //
    //        assertEquals(Collections.singleton(bookmark2), session.lastBookmarks());
    //    }

    @Test
    void accessModeUsedToAcquireReadConnections() {
        accessModeUsedToAcquireConnections(READ);
    }

    @Test
    void accessModeUsedToAcquireWriteConnections() {
        accessModeUsedToAcquireConnections(WRITE);
    }

    private void accessModeUsedToAcquireConnections(AccessMode mode) {
        var session2 = newSession(connectionProvider, mode);
        beginTransaction(session2);
        var argument = ArgumentCaptor.forClass(org.neo4j.driver.internal.bolt.api.AccessMode.class);
        verify(connectionProvider).connect(any(), any(), argument.capture(), any(), any(), any(), any(), any());
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
        beginTransaction(session);
        assertThat(session.lastBookmarks(), equalTo(bookmarks));
    }

    //    @Test
    //    void connectionShouldBeResetAfterSessionReset() {
    //        var query = "RETURN 1";
    //        setupSuccessfulRunAndPull(connection, query);
    //
    //        run(session, query);
    //
    //        var connectionInOrder = inOrder(connection);
    //        connectionInOrder.verify(connection, never()).reset(null);
    //        connectionInOrder.verify(connection).release();
    //
    //        await(session.resetAsync());
    //        connectionInOrder.verify(connection).reset(null);
    //        connectionInOrder.verify(connection, never()).release();
    //    }

    @Test
    void shouldHaveEmptyLastBookmarksInitially() {
        assertTrue(session.lastBookmarks().isEmpty());
    }

    //    @Test
    //    void shouldDoNothingWhenClosingWithoutAcquiredConnection() {
    //        var error = new RuntimeException("Hi");
    //        when(connectionProvider.acquireConnection(any(ConnectionContext.class))).thenReturn(failedFuture(error));
    //
    //        var e = assertThrows(Exception.class, () -> run(session, "RETURN 1"));
    //        assertEquals(error, e);
    //
    //        close(session);
    //    }
    //
    //    @Test
    //    void shouldRunAfterRunFailure() {
    //        var error = new RuntimeException("Hi");
    //        when(connectionProvider.acquireConnection(any(ConnectionContext.class)))
    //                .thenReturn(failedFuture(error))
    //                .thenAnswer(invocation -> {
    //                    var context = (ConnectionContext) invocation.getArgument(0);
    //                    context.databaseNameFuture().complete(DatabaseNameUtil.database(DATABASE));
    //                    return completedFuture(connection);
    //                });
    //
    //        var e = assertThrows(Exception.class, () -> run(session, "RETURN 1"));
    //
    //        assertEquals(error, e);
    //
    //        var query = "RETURN 2";
    //        setupSuccessfulRunAndPull(connection, query);
    //
    //        run(session, query);
    //
    //        verify(connectionProvider, times(2)).acquireConnection(any(ConnectionContext.class));
    //        verifyRunAndPull(connection, query);
    //    }
    //
    //    @Test
    //    void shouldRunAfterBeginTxFailureOnBookmark() {
    //        var error = new RuntimeException("Hi");
    //        var connection1 = connectionMock(BoltProtocolV4.INSTANCE);
    //        setupFailingBegin(connection1, error);
    //        var connection2 = connectionMock(BoltProtocolV4.INSTANCE);
    //
    //        when(connectionProvider.acquireConnection(any(ConnectionContext.class)))
    //                .thenAnswer(invocation -> {
    //                    var context = (ConnectionContext) invocation.getArgument(0);
    //                    context.databaseNameFuture().complete(DatabaseNameUtil.database(DATABASE));
    //                    return completedFuture(connection1);
    //                })
    //                .thenAnswer(invocation -> {
    //                    var context = (ConnectionContext) invocation.getArgument(0);
    //                    context.databaseNameFuture().complete(DatabaseNameUtil.database(DATABASE));
    //                    return completedFuture(connection2);
    //                });
    //
    //        var bookmarks = Collections.singleton(InternalBookmark.parse("neo4j:bookmark:v1:tx42"));
    //        var session = newSession(connectionProvider, bookmarks);
    //
    //        var e = assertThrows(Exception.class, () -> beginTransaction(session));
    //        assertEquals(error, e);
    //        var query = "RETURN 2";
    //        setupSuccessfulRunAndPull(connection2, query);
    //
    //        run(session, query);
    //
    //        verify(connectionProvider, times(2)).acquireConnection(any(ConnectionContext.class));
    //        verifyBeginTx(connection1);
    //        verifyRunAndPull(connection2, "RETURN 2");
    //    }
    //
    //    @Test
    //    void shouldBeginTxAfterBeginTxFailureOnBookmark() {
    //        var error = new RuntimeException("Hi");
    //        var connection1 = connectionMock(BoltProtocolV4.INSTANCE);
    //        setupFailingBegin(connection1, error);
    //        var connection2 = connectionMock(BoltProtocolV4.INSTANCE);
    //
    //        when(connectionProvider.acquireConnection(any(ConnectionContext.class)))
    //                .thenAnswer(invocation -> {
    //                    var context = (ConnectionContext) invocation.getArgument(0);
    //                    context.databaseNameFuture().complete(DatabaseNameUtil.database(DATABASE));
    //                    return completedFuture(connection1);
    //                })
    //                .thenAnswer(invocation -> {
    //                    var context = (ConnectionContext) invocation.getArgument(0);
    //                    context.databaseNameFuture().complete(DatabaseNameUtil.database(DATABASE));
    //                    return completedFuture(connection2);
    //                });
    //
    //        var bookmarks = Collections.singleton(InternalBookmark.parse("neo4j:bookmark:v1:tx42"));
    //        var session = newSession(connectionProvider, bookmarks);
    //
    //        var e = assertThrows(Exception.class, () -> beginTransaction(session));
    //        assertEquals(error, e);
    //
    //        beginTransaction(session);
    //
    //        verify(connectionProvider, times(2)).acquireConnection(any(ConnectionContext.class));
    //        verifyBeginTx(connection1);
    //        verifyBeginTx(connection2);
    //    }
    //
    //    @Test
    //    void shouldBeginTxAfterRunFailureToAcquireConnection() {
    //        var error = new RuntimeException("Hi");
    //        when(connectionProvider.acquireConnection(any(ConnectionContext.class)))
    //                .thenReturn(failedFuture(error))
    //                .thenAnswer(invocation -> {
    //                    var context = (ConnectionContext) invocation.getArgument(0);
    //                    context.databaseNameFuture().complete(DatabaseNameUtil.database(DATABASE));
    //                    return completedFuture(connection);
    //                });
    //
    //        var e = assertThrows(Exception.class, () -> run(session, "RETURN 1"));
    //        assertEquals(error, e);
    //
    //        beginTransaction(session);
    //
    //        verify(connectionProvider, times(2)).acquireConnection(any(ConnectionContext.class));
    //        verifyBeginTx(connection);
    //    }
    //
    //    @Test
    //    void shouldMarkTransactionAsTerminatedAndThenResetConnectionOnReset() {
    //        var tx = beginTransaction(session);
    //
    //        assertTrue(tx.isOpen());
    //        verify(connection, never()).reset(null);
    //
    //        await(session.resetAsync());
    //
    //        verify(connection).reset(any());
    //    }
    //
    //    @ParameterizedTest
    //    @ValueSource(booleans = {true, false})
    //    void shouldSendTelemetryIfEnabledOnBegin(boolean telemetryDisabled) {
    //        // given
    //        var session = newSession(connectionProvider, WRITE, new FixedRetryLogic(0), Set.of(), telemetryDisabled);
    //        given(connection.isTelemetryEnabled()).willReturn(true);
    //        var protocol = spy(BoltProtocolV54.INSTANCE);
    //        when(connection.protocol()).thenReturn(protocol);
    //
    //        // when
    //        beginTransaction(session);
    //
    //        // then
    //        if (telemetryDisabled) {
    //            then(protocol).should(never()).telemetry(any(), any());
    //        } else {
    //            then(protocol)
    //                    .should(times(1))
    //                    .telemetry(eq(connection), eq(TelemetryApi.UNMANAGED_TRANSACTION.getValue()));
    //        }
    //    }
    //
    //    @ParameterizedTest
    //    @ValueSource(booleans = {true, false})
    //    void shouldSendTelemetryIfEnabledOnRun(boolean telemetryDisabled) {
    //        // given
    //        var query = "RETURN 1";
    //        setupSuccessfulRunAndPull(connection, query);
    //        var apiTxWork = mock(ApiTelemetryWork.class);
    //        var session = newSession(connectionProvider, WRITE, new FixedRetryLogic(0), Set.of(), telemetryDisabled);
    //        given(connection.isTelemetryEnabled()).willReturn(true);
    //        var protocol = spy(BoltProtocolV54.INSTANCE);
    //        when(connection.protocol()).thenReturn(protocol);
    //
    //        // when
    //        run(session, query);
    //
    //        // then
    //        if (telemetryDisabled) {
    //            then(protocol).should(never()).telemetry(any(), any());
    //        } else {
    //            then(protocol)
    //                    .should(times(1))
    //                    .telemetry(eq(connection), eq(TelemetryApi.AUTO_COMMIT_TRANSACTION.getValue()));
    //        }
    //    }

    void stubBeginTransaction() {
        given(connection.beginTransaction(any(), any(), any(), any(), any(), any(), any(), any()))
                .willReturn(completedFuture(connection));
        given(connection.flush(any())).willAnswer((Answer<CompletionStage<Void>>) invocation -> {
            var handler = (ResponseHandler) invocation.getArguments()[0];
            handler.onBeginSummary(mock());
            return CompletableFuture.completedStage(null);
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
