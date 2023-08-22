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
package org.neo4j.driver.internal.messaging.v42;

import static java.time.Duration.ofSeconds;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.AccessMode.WRITE;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.internal.DatabaseNameUtil.database;
import static org.neo4j.driver.internal.DatabaseNameUtil.defaultDatabase;
import static org.neo4j.driver.internal.handlers.pulln.FetchSizeUtil.UNLIMITED_FETCH_SIZE;
import static org.neo4j.driver.testutil.TestUtil.await;
import static org.neo4j.driver.testutil.TestUtil.connectionMock;

import io.netty.channel.embedded.EmbeddedChannel;
import java.time.Clock;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Query;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.DatabaseBookmark;
import org.neo4j.driver.internal.DatabaseName;
import org.neo4j.driver.internal.InternalBookmark;
import org.neo4j.driver.internal.async.UnmanagedTransaction;
import org.neo4j.driver.internal.async.connection.ChannelAttributes;
import org.neo4j.driver.internal.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.async.pool.AuthContext;
import org.neo4j.driver.internal.cluster.RoutingContext;
import org.neo4j.driver.internal.cursor.AsyncResultCursor;
import org.neo4j.driver.internal.handlers.BeginTxResponseHandler;
import org.neo4j.driver.internal.handlers.CommitTxResponseHandler;
import org.neo4j.driver.internal.handlers.PullAllResponseHandler;
import org.neo4j.driver.internal.handlers.RollbackTxResponseHandler;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.request.BeginMessage;
import org.neo4j.driver.internal.messaging.request.CommitMessage;
import org.neo4j.driver.internal.messaging.request.GoodbyeMessage;
import org.neo4j.driver.internal.messaging.request.HelloMessage;
import org.neo4j.driver.internal.messaging.request.PullMessage;
import org.neo4j.driver.internal.messaging.request.RollbackMessage;
import org.neo4j.driver.internal.messaging.request.RunWithMetadataMessage;
import org.neo4j.driver.internal.messaging.v4.MessageFormatV4;
import org.neo4j.driver.internal.security.InternalAuthToken;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ResponseHandler;

public final class BoltProtocolV42Test {
    private static final String QUERY_TEXT = "RETURN $x";
    private static final Map<String, Value> PARAMS = singletonMap("x", value(42));
    private static final Query QUERY = new Query(QUERY_TEXT, value(PARAMS));

    private final BoltProtocol protocol = createProtocol();
    private final EmbeddedChannel channel = new EmbeddedChannel();
    private final InboundMessageDispatcher messageDispatcher = new InboundMessageDispatcher(channel, Logging.none());

    private final TransactionConfig txConfig = TransactionConfig.builder()
            .withTimeout(ofSeconds(12))
            .withMetadata(singletonMap("key", value(42)))
            .build();

    @SuppressWarnings("SameReturnValue")
    private BoltProtocol createProtocol() {
        return BoltProtocolV42.INSTANCE;
    }

    @BeforeEach
    void beforeEach() {
        ChannelAttributes.setMessageDispatcher(channel, messageDispatcher);
    }

    @AfterEach
    void afterEach() {
        channel.finishAndReleaseAll();
    }

    @Test
    void shouldCreateMessageFormat() {
        assertThat(protocol.createMessageFormat(), instanceOf(expectedMessageFormatType()));
    }

    @Test
    void shouldInitializeChannel() {
        var promise = channel.newPromise();
        var clock = mock(Clock.class);
        var time = 1L;
        when(clock.millis()).thenReturn(time);
        var authContext = mock(AuthContext.class);
        when(authContext.getAuthToken()).thenReturn(dummyAuthToken());
        ChannelAttributes.setAuthContext(channel, authContext);

        protocol.initializeChannel(
                "MyDriver/0.0.1", null, dummyAuthToken(), RoutingContext.EMPTY, promise, null, clock);

        assertThat(channel.outboundMessages(), hasSize(1));
        assertThat(channel.outboundMessages().poll(), instanceOf(HelloMessage.class));
        assertEquals(1, messageDispatcher.queuedHandlersCount());
        assertFalse(promise.isDone());

        Map<String, Value> metadata = new HashMap<>();
        metadata.put("server", value("Neo4j/4.2.0"));
        metadata.put("connection_id", value("bolt-42"));

        messageDispatcher.handleSuccessMessage(metadata);

        assertTrue(promise.isDone());
        assertTrue(promise.isSuccess());
        verify(clock).millis();
        verify(authContext).finishAuth(time);
    }

    @Test
    void shouldPrepareToCloseChannel() {
        protocol.prepareToCloseChannel(channel);

        assertThat(channel.outboundMessages(), hasSize(1));
        assertThat(channel.outboundMessages().poll(), instanceOf(GoodbyeMessage.class));
        assertEquals(1, messageDispatcher.queuedHandlersCount());
    }

    @Test
    void shouldFailToInitializeChannelWhenErrorIsReceived() {
        var promise = channel.newPromise();

        protocol.initializeChannel(
                "MyDriver/2.2.1", null, dummyAuthToken(), RoutingContext.EMPTY, promise, null, mock(Clock.class));

        assertThat(channel.outboundMessages(), hasSize(1));
        assertThat(channel.outboundMessages().poll(), instanceOf(HelloMessage.class));
        assertEquals(1, messageDispatcher.queuedHandlersCount());
        assertFalse(promise.isDone());

        messageDispatcher.handleFailureMessage("Neo.TransientError.General.DatabaseUnavailable", "Error!");

        assertTrue(promise.isDone());
        assertFalse(promise.isSuccess());
    }

    @Test
    void shouldBeginTransactionWithoutBookmark() {
        var connection = connectionMock(protocol);

        var stage = protocol.beginTransaction(
                connection, Collections.emptySet(), TransactionConfig.empty(), null, null, Logging.none(), true);

        verify(connection)
                .writeAndFlush(
                        eq(new BeginMessage(
                                Collections.emptySet(),
                                TransactionConfig.empty(),
                                defaultDatabase(),
                                WRITE,
                                null,
                                null,
                                null,
                                Logging.none())),
                        any(BeginTxResponseHandler.class));
        assertNull(await(stage));
    }

    @Test
    void shouldBeginTransactionWithBookmarks() {
        var connection = connectionMock(protocol);
        var bookmarks = Collections.singleton(InternalBookmark.parse("neo4j:bookmark:v1:tx100"));

        var stage = protocol.beginTransaction(
                connection, bookmarks, TransactionConfig.empty(), null, null, Logging.none(), true);

        verify(connection)
                .writeAndFlush(
                        eq(new BeginMessage(
                                bookmarks,
                                TransactionConfig.empty(),
                                defaultDatabase(),
                                WRITE,
                                null,
                                null,
                                null,
                                Logging.none())),
                        any(BeginTxResponseHandler.class));
        assertNull(await(stage));
    }

    @Test
    void shouldBeginTransactionWithConfig() {
        var connection = connectionMock(protocol);

        var stage = protocol.beginTransaction(
                connection, Collections.emptySet(), txConfig, null, null, Logging.none(), true);

        verify(connection)
                .writeAndFlush(
                        eq(new BeginMessage(
                                Collections.emptySet(),
                                txConfig,
                                defaultDatabase(),
                                WRITE,
                                null,
                                null,
                                null,
                                Logging.none())),
                        any(BeginTxResponseHandler.class));
        assertNull(await(stage));
    }

    @Test
    void shouldBeginTransactionWithBookmarksAndConfig() {
        var connection = connectionMock(protocol);
        var bookmarks = Collections.singleton(InternalBookmark.parse("neo4j:bookmark:v1:tx4242"));

        var stage = protocol.beginTransaction(connection, bookmarks, txConfig, null, null, Logging.none(), true);

        verify(connection)
                .writeAndFlush(
                        eq(new BeginMessage(
                                bookmarks, txConfig, defaultDatabase(), WRITE, null, null, null, Logging.none())),
                        any(BeginTxResponseHandler.class));
        assertNull(await(stage));
    }

    @Test
    void shouldCommitTransaction() {
        var bookmarkString = "neo4j:bookmark:v1:tx4242";

        var connection = connectionMock(protocol);
        when(connection.protocol()).thenReturn(protocol);
        doAnswer(invocation -> {
                    ResponseHandler commitHandler = invocation.getArgument(1);
                    commitHandler.onSuccess(singletonMap("bookmark", value(bookmarkString)));
                    return null;
                })
                .when(connection)
                .writeAndFlush(eq(CommitMessage.COMMIT), any());

        var stage = protocol.commitTransaction(connection);

        verify(connection).writeAndFlush(eq(CommitMessage.COMMIT), any(CommitTxResponseHandler.class));
        assertEquals(InternalBookmark.parse(bookmarkString), await(stage).bookmark());
    }

    @Test
    void shouldRollbackTransaction() {
        var connection = connectionMock(protocol);

        var stage = protocol.rollbackTransaction(connection);

        verify(connection).writeAndFlush(eq(RollbackMessage.ROLLBACK), any(RollbackTxResponseHandler.class));
        assertNull(await(stage));
    }

    @ParameterizedTest
    @EnumSource(AccessMode.class)
    void shouldRunInAutoCommitTransactionAndWaitForRunResponse(AccessMode mode) throws Exception {
        testRunAndWaitForRunResponse(true, TransactionConfig.empty(), mode);
    }

    @ParameterizedTest
    @EnumSource(AccessMode.class)
    void shouldRunInAutoCommitWithConfigTransactionAndWaitForRunResponse(AccessMode mode) throws Exception {
        testRunAndWaitForRunResponse(true, txConfig, mode);
    }

    @ParameterizedTest
    @EnumSource(AccessMode.class)
    void shouldRunInAutoCommitTransactionAndWaitForSuccessRunResponse(AccessMode mode) throws Exception {
        testSuccessfulRunInAutoCommitTxWithWaitingForResponse(Collections.emptySet(), TransactionConfig.empty(), mode);
    }

    @ParameterizedTest
    @EnumSource(AccessMode.class)
    void shouldRunInAutoCommitTransactionWithBookmarkAndConfigAndWaitForSuccessRunResponse(AccessMode mode)
            throws Exception {
        testSuccessfulRunInAutoCommitTxWithWaitingForResponse(
                Collections.singleton(InternalBookmark.parse("neo4j:bookmark:v1:tx65")), txConfig, mode);
    }

    @ParameterizedTest
    @EnumSource(AccessMode.class)
    void shouldRunInAutoCommitTransactionAndWaitForFailureRunResponse(AccessMode mode) {
        testFailedRunInAutoCommitTxWithWaitingForResponse(Collections.emptySet(), TransactionConfig.empty(), mode);
    }

    @ParameterizedTest
    @EnumSource(AccessMode.class)
    void shouldRunInAutoCommitTransactionWithBookmarkAndConfigAndWaitForFailureRunResponse(AccessMode mode) {
        testFailedRunInAutoCommitTxWithWaitingForResponse(
                Collections.singleton(InternalBookmark.parse("neo4j:bookmark:v1:tx163")), txConfig, mode);
    }

    @ParameterizedTest
    @EnumSource(AccessMode.class)
    void shouldRunInUnmanagedTransactionAndWaitForRunResponse(AccessMode mode) throws Exception {
        testRunAndWaitForRunResponse(false, TransactionConfig.empty(), mode);
    }

    @ParameterizedTest
    @EnumSource(AccessMode.class)
    void shouldRunInUnmanagedTransactionAndWaitForSuccessRunResponse(AccessMode mode) throws Exception {
        testRunInUnmanagedTransactionAndWaitForRunResponse(true, mode);
    }

    @ParameterizedTest
    @EnumSource(AccessMode.class)
    void shouldRunInUnmanagedTransactionAndWaitForFailureRunResponse(AccessMode mode) throws Exception {
        testRunInUnmanagedTransactionAndWaitForRunResponse(false, mode);
    }

    @Test
    void databaseNameInBeginTransaction() {
        testDatabaseNameSupport(false);
    }

    @Test
    void databaseNameForAutoCommitTransactions() {
        testDatabaseNameSupport(true);
    }

    @Test
    void shouldSupportDatabaseNameInBeginTransaction() {
        var txStage = protocol.beginTransaction(
                connectionMock("foo", protocol),
                Collections.emptySet(),
                TransactionConfig.empty(),
                null,
                null,
                Logging.none(),
                true);

        assertDoesNotThrow(() -> await(txStage));
    }

    @Test
    void shouldNotSupportDatabaseNameForAutoCommitTransactions() {
        assertDoesNotThrow(() -> protocol.runInAutoCommitTransaction(
                connectionMock("foo", protocol),
                new Query("RETURN 1"),
                Collections.emptySet(),
                (ignored) -> {},
                TransactionConfig.empty(),
                UNLIMITED_FETCH_SIZE,
                null,
                Logging.none()));
    }

    private Class<? extends MessageFormat> expectedMessageFormatType() {
        return MessageFormatV4.class;
    }

    private void testFailedRunInAutoCommitTxWithWaitingForResponse(
            Set<Bookmark> bookmarks, TransactionConfig config, AccessMode mode) {
        // Given
        var connection = connectionMock(mode, protocol);
        @SuppressWarnings("unchecked")
        Consumer<DatabaseBookmark> bookmarkConsumer = mock(Consumer.class);

        var cursorFuture = protocol.runInAutoCommitTransaction(
                        connection,
                        QUERY,
                        bookmarks,
                        bookmarkConsumer,
                        config,
                        UNLIMITED_FETCH_SIZE,
                        null,
                        Logging.none())
                .asyncResult()
                .toCompletableFuture();

        var runHandler = verifySessionRunInvoked(connection, bookmarks, config, mode, defaultDatabase());
        assertFalse(cursorFuture.isDone());

        // When I response to Run message with a failure
        Throwable error = new RuntimeException();
        runHandler.onFailure(error);

        // Then
        then(bookmarkConsumer).should(times(0)).accept(any());
        var actual =
                assertThrows(error.getClass(), () -> await(cursorFuture.get().mapSuccessfulRunCompletionAsync()));
        assertSame(error, actual);
    }

    private void testSuccessfulRunInAutoCommitTxWithWaitingForResponse(
            Set<Bookmark> bookmarks, TransactionConfig config, AccessMode mode) throws Exception {
        // Given
        var connection = connectionMock(mode, protocol);
        @SuppressWarnings("unchecked")
        Consumer<DatabaseBookmark> bookmarkConsumer = mock(Consumer.class);

        var cursorFuture = protocol.runInAutoCommitTransaction(
                        connection,
                        QUERY,
                        bookmarks,
                        bookmarkConsumer,
                        config,
                        UNLIMITED_FETCH_SIZE,
                        null,
                        Logging.none())
                .asyncResult()
                .toCompletableFuture();

        var runHandler = verifySessionRunInvoked(connection, bookmarks, config, mode, defaultDatabase());
        assertFalse(cursorFuture.isDone());

        // When I response to the run message
        runHandler.onSuccess(emptyMap());

        // Then
        then(bookmarkConsumer).should(times(0)).accept(any());
        assertTrue(cursorFuture.isDone());
        assertNotNull(cursorFuture.get());
    }

    private void testRunInUnmanagedTransactionAndWaitForRunResponse(boolean success, AccessMode mode) throws Exception {
        // Given
        var connection = connectionMock(mode, protocol);

        var cursorFuture = protocol.runInUnmanagedTransaction(
                        connection, QUERY, mock(UnmanagedTransaction.class), UNLIMITED_FETCH_SIZE)
                .asyncResult()
                .toCompletableFuture();

        var runHandler = verifyTxRunInvoked(connection);
        assertFalse(cursorFuture.isDone());
        Throwable error = new RuntimeException();

        if (success) {
            runHandler.onSuccess(emptyMap());
        } else {
            // When responded with a failure
            runHandler.onFailure(error);
        }

        // Then
        assertTrue(cursorFuture.isDone());
        if (success) {
            assertNotNull(await(cursorFuture.get().mapSuccessfulRunCompletionAsync()));
        } else {
            var actual = assertThrows(
                    error.getClass(), () -> await(cursorFuture.get().mapSuccessfulRunCompletionAsync()));
            assertSame(error, actual);
        }
    }

    private void testRunAndWaitForRunResponse(boolean autoCommitTx, TransactionConfig config, AccessMode mode)
            throws Exception {
        // Given
        var connection = connectionMock(mode, protocol);
        var initialBookmarks = Collections.singleton(InternalBookmark.parse("neo4j:bookmark:v1:tx987"));

        CompletionStage<AsyncResultCursor> cursorStage;
        if (autoCommitTx) {
            cursorStage = protocol.runInAutoCommitTransaction(
                            connection,
                            QUERY,
                            initialBookmarks,
                            (ignored) -> {},
                            config,
                            UNLIMITED_FETCH_SIZE,
                            null,
                            Logging.none())
                    .asyncResult();
        } else {
            cursorStage = protocol.runInUnmanagedTransaction(
                            connection, QUERY, mock(UnmanagedTransaction.class), UNLIMITED_FETCH_SIZE)
                    .asyncResult();
        }

        // When & Then
        var cursorFuture = cursorStage.toCompletableFuture();
        assertFalse(cursorFuture.isDone());

        var runResponseHandler = autoCommitTx
                ? verifySessionRunInvoked(connection, initialBookmarks, config, mode, defaultDatabase())
                : verifyTxRunInvoked(connection);
        runResponseHandler.onSuccess(emptyMap());

        assertTrue(cursorFuture.isDone());
        assertNotNull(cursorFuture.get());
    }

    private void testDatabaseNameSupport(boolean autoCommitTx) {
        var connection = connectionMock("foo", protocol);
        if (autoCommitTx) {
            var factory = protocol.runInAutoCommitTransaction(
                    connection,
                    QUERY,
                    Collections.emptySet(),
                    (ignored) -> {},
                    TransactionConfig.empty(),
                    UNLIMITED_FETCH_SIZE,
                    null,
                    Logging.none());
            var resultStage = factory.asyncResult();
            var runHandler = verifySessionRunInvoked(
                    connection, Collections.emptySet(), TransactionConfig.empty(), AccessMode.WRITE, database("foo"));
            runHandler.onSuccess(emptyMap());
            await(resultStage);
            verifySessionRunInvoked(
                    connection, Collections.emptySet(), TransactionConfig.empty(), AccessMode.WRITE, database("foo"));
        } else {
            var txStage = protocol.beginTransaction(
                    connection, Collections.emptySet(), TransactionConfig.empty(), null, null, Logging.none(), true);
            await(txStage);
            verifyBeginInvoked(connection, Collections.emptySet(), TransactionConfig.empty(), database("foo"));
        }
    }

    private ResponseHandler verifyTxRunInvoked(Connection connection) {
        return verifyRunInvoked(connection, RunWithMetadataMessage.unmanagedTxRunMessage(QUERY));
    }

    private ResponseHandler verifySessionRunInvoked(
            Connection connection,
            Set<Bookmark> bookmarks,
            TransactionConfig config,
            AccessMode mode,
            DatabaseName databaseName) {
        var runMessage = RunWithMetadataMessage.autoCommitTxRunMessage(
                QUERY, config, databaseName, mode, bookmarks, null, null, Logging.none());
        return verifyRunInvoked(connection, runMessage);
    }

    private ResponseHandler verifyRunInvoked(Connection connection, RunWithMetadataMessage runMessage) {
        var runHandlerCaptor = ArgumentCaptor.forClass(ResponseHandler.class);
        var pullHandlerCaptor = ArgumentCaptor.forClass(ResponseHandler.class);

        verify(connection).write(eq(runMessage), runHandlerCaptor.capture());
        verify(connection).writeAndFlush(any(PullMessage.class), pullHandlerCaptor.capture());

        assertThat(runHandlerCaptor.getValue(), instanceOf(RunResponseHandler.class));
        assertThat(pullHandlerCaptor.getValue(), instanceOf(PullAllResponseHandler.class));

        return runHandlerCaptor.getValue();
    }

    private void verifyBeginInvoked(
            Connection connection, Set<Bookmark> bookmarks, TransactionConfig config, DatabaseName databaseName) {
        var beginHandlerCaptor = ArgumentCaptor.forClass(ResponseHandler.class);
        var beginMessage =
                new BeginMessage(bookmarks, config, databaseName, AccessMode.WRITE, null, null, null, Logging.none());
        verify(connection).writeAndFlush(eq(beginMessage), beginHandlerCaptor.capture());
        assertThat(beginHandlerCaptor.getValue(), instanceOf(BeginTxResponseHandler.class));
    }

    private static InternalAuthToken dummyAuthToken() {
        return (InternalAuthToken) AuthTokens.basic("hello", "world");
    }
}
