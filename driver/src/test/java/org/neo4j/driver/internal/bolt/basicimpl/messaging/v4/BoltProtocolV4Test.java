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
package org.neo4j.driver.internal.bolt.basicimpl.messaging.v4;

import static java.time.Duration.ofSeconds;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.internal.bolt.api.DatabaseNameUtil.database;
import static org.neo4j.driver.internal.bolt.api.DatabaseNameUtil.defaultDatabase;
import static org.neo4j.driver.internal.bolt.basicimpl.messaging.request.RunWithMetadataMessage.autoCommitTxRunMessage;
import static org.neo4j.driver.internal.bolt.basicimpl.messaging.request.RunWithMetadataMessage.unmanagedTxRunMessage;

import io.netty.channel.embedded.EmbeddedChannel;
import java.time.Clock;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.stubbing.Answer;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.bolt.NoopLoggingProvider;
import org.neo4j.driver.internal.bolt.api.AccessMode;
import org.neo4j.driver.internal.bolt.api.GqlError;
import org.neo4j.driver.internal.bolt.api.RoutingContext;
import org.neo4j.driver.internal.bolt.api.summary.RouteSummary;
import org.neo4j.driver.internal.bolt.api.summary.RunSummary;
import org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes;
import org.neo4j.driver.internal.bolt.basicimpl.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.bolt.basicimpl.handlers.BeginTxResponseHandler;
import org.neo4j.driver.internal.bolt.basicimpl.handlers.CommitTxResponseHandler;
import org.neo4j.driver.internal.bolt.basicimpl.handlers.PullResponseHandlerImpl;
import org.neo4j.driver.internal.bolt.basicimpl.handlers.RollbackTxResponseHandler;
import org.neo4j.driver.internal.bolt.basicimpl.handlers.RunResponseHandler;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.BoltProtocol;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.MessageFormat;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.MessageHandler;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.PullMessageHandler;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.request.BeginMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.request.CommitMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.request.HelloMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.request.PullMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.request.RollbackMessage;
import org.neo4j.driver.internal.bolt.basicimpl.spi.Connection;

public final class BoltProtocolV4Test {
    private static final String query = "RETURN $x";
    private static final Map<String, Value> query_params = singletonMap("x", value(42));
    private static final long UNLIMITED_FETCH_SIZE = -1;
    private static final Duration txTimeout = ofSeconds(12);
    private static final Map<String, Value> txMetadata = singletonMap("x", value(42));

    private final BoltProtocol protocol = createProtocol();
    private final EmbeddedChannel channel = new EmbeddedChannel();
    private final InboundMessageDispatcher messageDispatcher =
            new InboundMessageDispatcher(channel, NoopLoggingProvider.INSTANCE);

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

        var latestAuthMillisFuture = new CompletableFuture<Long>();

        protocol.initializeChannel(
                "MyDriver/0.0.1",
                null,
                Collections.emptyMap(),
                RoutingContext.EMPTY,
                promise,
                null,
                clock,
                latestAuthMillisFuture);

        assertThat(channel.outboundMessages(), hasSize(1));
        assertThat(channel.outboundMessages().poll(), instanceOf(HelloMessage.class));
        assertEquals(1, messageDispatcher.queuedHandlersCount());
        assertFalse(promise.isDone());

        var metadata = Map.of(
                "server", value("Neo4j/3.5.0"),
                "connection_id", value("bolt-42"));

        messageDispatcher.handleSuccessMessage(metadata);

        assertTrue(promise.isDone());
        assertTrue(promise.isSuccess());
        verify(clock).millis();
        assertTrue(latestAuthMillisFuture.isDone());
        assertEquals(time, latestAuthMillisFuture.join());
    }

    @Test
    void shouldFailToInitializeChannelWhenErrorIsReceived() {
        var promise = channel.newPromise();

        protocol.initializeChannel(
                "MyDriver/2.2.1",
                null,
                Collections.emptyMap(),
                RoutingContext.EMPTY,
                promise,
                null,
                mock(Clock.class),
                new CompletableFuture<>());

        assertThat(channel.outboundMessages(), hasSize(1));
        assertThat(channel.outboundMessages().poll(), instanceOf(HelloMessage.class));
        assertEquals(1, messageDispatcher.queuedHandlersCount());
        assertFalse(promise.isDone());

        messageDispatcher.handleFailureMessage(
                new GqlError("Neo.TransientError.General.DatabaseUnavailable", "Error!"));

        assertTrue(promise.isDone());
        assertFalse(promise.isSuccess());
    }

    @Test
    void shouldSendRoute() {
        var connection = mock(Connection.class);
        given(connection.protocol()).willReturn(protocol);
        given(connection.write(any(), any()))
                .willAnswer((Answer<CompletionStage<Void>>) invocation -> {
                    var handler = (RunResponseHandler) invocation.getArgument(1);
                    handler.onSuccess(Map.of("fields", Values.value(List.of("ttl", "servers"))));
                    return CompletableFuture.completedStage(null);
                })
                .willAnswer((Answer<CompletionStage<Void>>) invocation -> {
                    var handler = (PullResponseHandlerImpl) invocation.getArgument(1);
                    handler.onRecord(new Value[] {
                        Values.value(1000),
                        Values.value(List.of(
                                Values.value(Map.of(
                                        "role",
                                        Values.value("ROUTE"),
                                        "addresses",
                                        Values.value(List.of(Values.value("192.168.0.1:7867"))))),
                                Values.value(Map.of(
                                        "role",
                                        Values.value("WRITE"),
                                        "addresses",
                                        Values.value(List.of(Values.value("192.168.0.1:7867"))))),
                                Values.value(Map.of(
                                        "role",
                                        Values.value("READ"),
                                        "addresses",
                                        Values.value(List.of(Values.value("192.168.0.1:7867")))))))
                    });
                    handler.onSuccess(emptyMap());
                    return CompletableFuture.completedStage(null);
                });
        var routeFuture = new CompletableFuture<RouteSummary>();
        var handler = new MessageHandler<RouteSummary>() {
            @Override
            public void onError(Throwable throwable) {
                routeFuture.completeExceptionally(throwable);
            }

            @Override
            public void onSummary(RouteSummary summary) {
                routeFuture.complete(summary);
            }
        };

        protocol.route(
                        connection,
                        Collections.emptyMap(),
                        Collections.emptySet(),
                        "neo4j",
                        null,
                        handler,
                        mock(Clock.class),
                        NoopLoggingProvider.INSTANCE)
                .toCompletableFuture()
                .join();

        var summary = routeFuture.join();
        assertEquals(summary.clusterComposition().expirationTimestamp(), TimeUnit.SECONDS.toMillis(1000));
        assertEquals(summary.clusterComposition().routers().size(), 1);
        assertEquals(summary.clusterComposition().writers().size(), 1);
        assertEquals(summary.clusterComposition().readers().size(), 1);
    }

    @Test
    void shouldBeginTransactionWithoutBookmark() {
        var connection = mock(Connection.class);
        given(connection.protocol()).willReturn(protocol);
        var expectedStage = CompletableFuture.<Void>completedStage(null);
        given(connection.write(any(), any())).willAnswer((Answer<CompletionStage<Void>>) invocation -> {
            var beginHandler = (BeginTxResponseHandler) invocation.getArgument(1);
            beginHandler.onSuccess(emptyMap());
            return expectedStage;
        });
        @SuppressWarnings("unchecked")
        var handler = (MessageHandler<Void>) mock(MessageHandler.class);

        var stage = protocol.beginTransaction(
                connection,
                defaultDatabase(),
                org.neo4j.driver.internal.bolt.api.AccessMode.WRITE,
                null,
                Collections.emptySet(),
                null,
                Collections.emptyMap(),
                null,
                null,
                handler,
                NoopLoggingProvider.INSTANCE);

        assertEquals(expectedStage, stage);
        var message = new BeginMessage(
                Collections.emptySet(),
                null,
                Collections.emptyMap(),
                defaultDatabase(),
                org.neo4j.driver.internal.bolt.api.AccessMode.WRITE,
                null,
                null,
                null,
                false,
                NoopLoggingProvider.INSTANCE);
        then(connection).should().write(eq(message), any(BeginTxResponseHandler.class));
        then(handler).should().onSummary(any());
    }

    @Test
    void shouldBeginTransactionWithBookmarks() {
        var connection = mock(Connection.class);
        given(connection.protocol()).willReturn(protocol);
        var expectedStage = CompletableFuture.<Void>completedStage(null);
        given(connection.write(any(), any())).willAnswer((Answer<CompletionStage<Void>>) invocation -> {
            var beginHandler = (BeginTxResponseHandler) invocation.getArgument(1);
            beginHandler.onSuccess(emptyMap());
            return expectedStage;
        });
        @SuppressWarnings("unchecked")
        var handler = (MessageHandler<Void>) mock(MessageHandler.class);
        var bookmarks = Collections.singleton("neo4j:bookmark:v1:tx100");

        var stage = protocol.beginTransaction(
                connection,
                defaultDatabase(),
                org.neo4j.driver.internal.bolt.api.AccessMode.WRITE,
                null,
                bookmarks,
                null,
                Collections.emptyMap(),
                null,
                null,
                handler,
                NoopLoggingProvider.INSTANCE);

        assertEquals(expectedStage, stage);
        var message = new BeginMessage(
                bookmarks,
                null,
                Collections.emptyMap(),
                defaultDatabase(),
                org.neo4j.driver.internal.bolt.api.AccessMode.WRITE,
                null,
                null,
                null,
                false,
                NoopLoggingProvider.INSTANCE);
        then(connection).should().write(eq(message), any(BeginTxResponseHandler.class));
        then(handler).should().onSummary(any());
    }

    @Test
    void shouldBeginTransactionWithConfig() {
        var connection = mock(Connection.class);
        given(connection.protocol()).willReturn(protocol);
        var expectedStage = CompletableFuture.<Void>completedStage(null);
        given(connection.write(any(), any())).willAnswer((Answer<CompletionStage<Void>>) invocation -> {
            var beginHandler = (BeginTxResponseHandler) invocation.getArgument(1);
            beginHandler.onSuccess(emptyMap());
            return expectedStage;
        });
        @SuppressWarnings("unchecked")
        var handler = (MessageHandler<Void>) mock(MessageHandler.class);

        var stage = protocol.beginTransaction(
                connection,
                defaultDatabase(),
                org.neo4j.driver.internal.bolt.api.AccessMode.WRITE,
                null,
                Collections.emptySet(),
                txTimeout,
                txMetadata,
                null,
                null,
                handler,
                NoopLoggingProvider.INSTANCE);

        assertEquals(expectedStage, stage);
        var message = new BeginMessage(
                Collections.emptySet(),
                txTimeout,
                txMetadata,
                defaultDatabase(),
                org.neo4j.driver.internal.bolt.api.AccessMode.WRITE,
                null,
                null,
                null,
                false,
                NoopLoggingProvider.INSTANCE);
        then(connection).should().write(eq(message), any(BeginTxResponseHandler.class));
        then(handler).should().onSummary(any());
    }

    @Test
    void shouldBeginTransactionWithBookmarksAndConfig() {
        var connection = mock(Connection.class);
        given(connection.protocol()).willReturn(protocol);
        var expectedStage = CompletableFuture.<Void>completedStage(null);
        given(connection.write(any(), any())).willAnswer((Answer<CompletionStage<Void>>) invocation -> {
            var beginHandler = (BeginTxResponseHandler) invocation.getArgument(1);
            beginHandler.onSuccess(emptyMap());
            return expectedStage;
        });
        @SuppressWarnings("unchecked")
        var handler = (MessageHandler<Void>) mock(MessageHandler.class);
        var bookmarks = Collections.singleton("neo4j:bookmark:v1:tx4242");

        var stage = protocol.beginTransaction(
                connection,
                defaultDatabase(),
                org.neo4j.driver.internal.bolt.api.AccessMode.WRITE,
                null,
                bookmarks,
                txTimeout,
                txMetadata,
                null,
                null,
                handler,
                NoopLoggingProvider.INSTANCE);

        assertEquals(expectedStage, stage);
        var message = new BeginMessage(
                bookmarks,
                txTimeout,
                txMetadata,
                defaultDatabase(),
                org.neo4j.driver.internal.bolt.api.AccessMode.WRITE,
                null,
                null,
                null,
                false,
                NoopLoggingProvider.INSTANCE);
        then(connection).should().write(eq(message), any(BeginTxResponseHandler.class));
        then(handler).should().onSummary(any());
    }

    @Test
    void shouldCommitTransaction() {
        var bookmarkString = "neo4j:bookmark:v1:tx4242";
        var connection = mock(Connection.class);
        given(connection.protocol()).willReturn(protocol);
        var expectedStage = CompletableFuture.<Void>completedStage(null);
        given(connection.write(any(), any())).willAnswer((Answer<CompletionStage<Void>>) invocation -> {
            var commitHandler = (CommitTxResponseHandler) invocation.getArgument(1);
            commitHandler.onSuccess(Map.of("bookmark", value(bookmarkString)));
            return expectedStage;
        });
        @SuppressWarnings("unchecked")
        var handler = (MessageHandler<String>) mock(MessageHandler.class);

        var stage = protocol.commitTransaction(connection, handler);

        assertEquals(expectedStage, stage);
        then(connection).should().write(eq(CommitMessage.COMMIT), any(CommitTxResponseHandler.class));
        then(handler).should().onSummary(bookmarkString);
    }

    @Test
    void shouldRollbackTransaction() {
        var connection = mock(Connection.class);
        given(connection.protocol()).willReturn(protocol);
        var expectedStage = CompletableFuture.<Void>completedStage(null);
        given(connection.write(any(), any())).willAnswer((Answer<CompletionStage<Void>>) invocation -> {
            var rollbackHandler = (RollbackTxResponseHandler) invocation.getArgument(1);
            rollbackHandler.onSuccess(Collections.emptyMap());
            return expectedStage;
        });
        @SuppressWarnings("unchecked")
        var handler = (MessageHandler<Void>) mock(MessageHandler.class);

        var stage = protocol.rollbackTransaction(connection, handler);

        assertEquals(expectedStage, stage);
        then(connection).should().write(eq(RollbackMessage.ROLLBACK), any(RollbackTxResponseHandler.class));
        then(handler).should().onSummary(any());
    }

    @ParameterizedTest
    @EnumSource(AccessMode.class)
    void shouldRunInAutoCommitTransactionAndWaitForRunResponse(AccessMode mode) {
        testRunAndWaitForRunResponse(true, null, Collections.emptyMap(), mode);
    }

    @ParameterizedTest
    @EnumSource(AccessMode.class)
    void shouldRunInAutoCommitWithConfigTransactionAndWaitForRunResponse(AccessMode mode) {
        testRunAndWaitForRunResponse(true, txTimeout, txMetadata, mode);
    }

    @ParameterizedTest
    @EnumSource(AccessMode.class)
    void shouldRunInAutoCommitTransactionAndWaitForSuccessRunResponse(AccessMode mode) {
        testSuccessfulRunInAutoCommitTxWithWaitingForResponse(
                Collections.emptySet(), null, Collections.emptyMap(), mode);
    }

    @ParameterizedTest
    @EnumSource(AccessMode.class)
    void shouldRunInAutoCommitTransactionWithBookmarkAndConfigAndWaitForSuccessRunResponse(AccessMode mode) {
        testSuccessfulRunInAutoCommitTxWithWaitingForResponse(
                Collections.singleton("neo4j:bookmark:v1:tx65"), txTimeout, txMetadata, mode);
    }

    @ParameterizedTest
    @EnumSource(AccessMode.class)
    void shouldRunInAutoCommitTransactionAndWaitForFailureRunResponse(AccessMode mode) {
        testFailedRunInAutoCommitTxWithWaitingForResponse(Collections.emptySet(), null, Collections.emptyMap(), mode);
    }

    @ParameterizedTest
    @EnumSource(AccessMode.class)
    void shouldRunInAutoCommitTransactionWithBookmarkAndConfigAndWaitForFailureRunResponse(AccessMode mode) {
        testFailedRunInAutoCommitTxWithWaitingForResponse(
                Collections.singleton("neo4j:bookmark:v1:tx163"), txTimeout, txMetadata, mode);
    }

    @ParameterizedTest
    @EnumSource(AccessMode.class)
    void shouldRunInUnmanagedTransactionAndWaitForRunResponse(AccessMode mode) {
        testRunAndWaitForRunResponse(false, null, Collections.emptyMap(), mode);
    }

    @Test
    void shouldRunInUnmanagedTransactionAndWaitForSuccessRunResponse() {
        testRunInUnmanagedTransactionAndWaitForRunResponse(true);
    }

    @Test
    void shouldRunInUnmanagedTransactionAndWaitForFailureRunResponse() {
        testRunInUnmanagedTransactionAndWaitForRunResponse(false);
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
        var connection = mock(Connection.class);
        var expectedStage = CompletableFuture.<Void>completedStage(null);
        given(connection.write(any(), any())).willReturn(expectedStage);
        var future = protocol.beginTransaction(
                connection,
                database("foo"),
                org.neo4j.driver.internal.bolt.api.AccessMode.WRITE,
                null,
                Collections.emptySet(),
                null,
                Collections.emptyMap(),
                null,
                null,
                mock(),
                NoopLoggingProvider.INSTANCE);

        assertEquals(expectedStage, future);
        then(connection).should().write(any(), any());
    }

    @Test
    void shouldNotSupportDatabaseNameForAutoCommitTransactions() {
        var connection = mock(Connection.class);
        var expectedStage = CompletableFuture.<Void>completedStage(null);
        given(connection.write(any(), any())).willReturn(expectedStage);
        var future = protocol.runAuto(
                connection,
                database("foo"),
                AccessMode.WRITE,
                null,
                query,
                query_params,
                Collections.emptySet(),
                txTimeout,
                txMetadata,
                null,
                mock(),
                NoopLoggingProvider.INSTANCE);

        assertEquals(expectedStage, future);
        then(connection).should().write(any(), any());
    }

    @Test
    void shouldReturnFailedStageWithNoConnectionInteractionsOnTelemetry() {
        var connection = mock(Connection.class);
        @SuppressWarnings("unchecked")
        var handler = (MessageHandler<Void>) mock(MessageHandler.class);

        var future = protocol.telemetry(connection, 1, handler).toCompletableFuture();

        assertTrue(future.isCompletedExceptionally());
        then(connection).shouldHaveNoInteractions();
    }

    @SuppressWarnings("SameReturnValue")
    private BoltProtocol createProtocol() {
        return BoltProtocolV4.INSTANCE;
    }

    private Class<? extends MessageFormat> expectedMessageFormatType() {
        return MessageFormatV4.class;
    }

    private void testFailedRunInAutoCommitTxWithWaitingForResponse(
            Set<String> bookmarks,
            Duration txTimeout,
            Map<String, Value> txMetadata,
            org.neo4j.driver.internal.bolt.api.AccessMode mode) {
        var connection = mock(Connection.class);
        given(connection.protocol()).willReturn(protocol);
        var expectedStage = CompletableFuture.<Void>completedStage(null);
        Throwable error = new RuntimeException();
        given(connection.write(any(), any())).willAnswer((Answer<CompletionStage<Void>>) invocation -> {
            var runHandler = (RunResponseHandler) invocation.getArgument(1);
            runHandler.onFailure(error);
            return expectedStage;
        });
        @SuppressWarnings("unchecked")
        var handler = (MessageHandler<RunSummary>) mock(MessageHandler.class);

        var stage = protocol.runAuto(
                connection,
                defaultDatabase(),
                mode,
                null,
                query,
                query_params,
                bookmarks,
                txTimeout,
                txMetadata,
                null,
                handler,
                NoopLoggingProvider.INSTANCE);
        assertEquals(expectedStage, stage);
        var message = autoCommitTxRunMessage(
                query,
                query_params,
                txTimeout,
                txMetadata,
                defaultDatabase(),
                mode,
                bookmarks,
                null,
                null,
                false,
                NoopLoggingProvider.INSTANCE);
        then(connection).should().write(eq(message), any(RunResponseHandler.class));
        then(handler).should().onError(error);
    }

    private void testSuccessfulRunInAutoCommitTxWithWaitingForResponse(
            Set<String> bookmarks,
            Duration txTimeout,
            Map<String, Value> txMetadata,
            org.neo4j.driver.internal.bolt.api.AccessMode mode) {
        var connection = mock(Connection.class);
        given(connection.protocol()).willReturn(protocol);
        var expectedRunStage = CompletableFuture.<Void>completedStage(null);
        var expectedPullStage = CompletableFuture.<Void>completedStage(null);
        var newBookmarkValue = "neo4j:bookmark:v1:tx98765";
        given(connection.write(any(), any()))
                .willAnswer((Answer<CompletionStage<Void>>) invocation -> {
                    var runHandler = (RunResponseHandler) invocation.getArgument(1);
                    runHandler.onSuccess(emptyMap());
                    return expectedRunStage;
                })
                .willAnswer((Answer<CompletionStage<Void>>) invocation -> {
                    var pullHandler = (PullResponseHandlerImpl) invocation.getArgument(1);
                    pullHandler.onSuccess(
                            Map.of("has_more", Values.value(false), "bookmark", Values.value(newBookmarkValue)));
                    return expectedPullStage;
                });
        @SuppressWarnings("unchecked")
        var runHandler = (MessageHandler<RunSummary>) mock(MessageHandler.class);
        var pullHandler = mock(PullMessageHandler.class);

        var runStage = protocol.runAuto(
                connection,
                defaultDatabase(),
                mode,
                null,
                query,
                query_params,
                bookmarks,
                txTimeout,
                txMetadata,
                null,
                runHandler,
                NoopLoggingProvider.INSTANCE);
        var pullStage = protocol.pull(connection, 0, UNLIMITED_FETCH_SIZE, pullHandler);

        assertEquals(expectedRunStage, runStage);
        assertEquals(expectedPullStage, pullStage);
        var runMessage = autoCommitTxRunMessage(
                query,
                query_params,
                txTimeout,
                txMetadata,
                defaultDatabase(),
                mode,
                bookmarks,
                null,
                null,
                false,
                NoopLoggingProvider.INSTANCE);
        then(connection).should().write(eq(runMessage), any(RunResponseHandler.class));
        var pullMessage = new PullMessage(UNLIMITED_FETCH_SIZE, 0L);
        then(connection).should().write(eq(pullMessage), any(PullResponseHandlerImpl.class));
        then(runHandler).should().onSummary(any());
        then(pullHandler)
                .should()
                .onSummary(new PullResponseHandlerImpl.PullSummaryImpl(
                        false, Map.of("has_more", Values.value(false), "bookmark", Values.value(newBookmarkValue))));
    }

    private void testRunInUnmanagedTransactionAndWaitForRunResponse(boolean success) {
        var connection = mock(Connection.class);
        given(connection.protocol()).willReturn(protocol);
        var expectedStage = CompletableFuture.<Void>completedStage(null);
        Throwable error = new RuntimeException();
        given(connection.write(any(), any())).willAnswer((Answer<CompletionStage<Void>>) invocation -> {
            var runHandler = (RunResponseHandler) invocation.getArgument(1);
            if (success) {
                runHandler.onSuccess(emptyMap());
            } else {
                runHandler.onFailure(error);
            }
            return expectedStage;
        });
        @SuppressWarnings("unchecked")
        var handler = (MessageHandler<RunSummary>) mock(MessageHandler.class);

        var stage = protocol.run(connection, query, query_params, handler);

        assertEquals(expectedStage, stage);
        var message = unmanagedTxRunMessage(query, query_params);
        then(connection).should().write(eq(message), any(RunResponseHandler.class));
        if (success) {
            then(handler).should().onSummary(any());
        } else {
            then(handler).should().onError(error);
        }
    }

    private void testRunAndWaitForRunResponse(
            boolean autoCommitTx,
            Duration txTimeout,
            Map<String, Value> txMetadata,
            org.neo4j.driver.internal.bolt.api.AccessMode mode) {
        var connection = mock(Connection.class);
        given(connection.protocol()).willReturn(protocol);
        var expectedStage = CompletableFuture.<Void>completedStage(null);
        given(connection.write(any(), any())).willAnswer((Answer<CompletionStage<Void>>) invocation -> {
            var runHandler = (RunResponseHandler) invocation.getArgument(1);
            runHandler.onSuccess(emptyMap());
            return expectedStage;
        });
        @SuppressWarnings("unchecked")
        var handler = (MessageHandler<RunSummary>) mock(MessageHandler.class);
        var initialBookmarks = Collections.singleton("neo4j:bookmark:v1:tx987");

        if (autoCommitTx) {
            var stage = protocol.runAuto(
                    connection,
                    defaultDatabase(),
                    mode,
                    null,
                    query,
                    query_params,
                    initialBookmarks,
                    txTimeout,
                    txMetadata,
                    null,
                    handler,
                    NoopLoggingProvider.INSTANCE);
            assertEquals(expectedStage, stage);
            var message = autoCommitTxRunMessage(
                    query,
                    query_params,
                    txTimeout,
                    txMetadata,
                    defaultDatabase(),
                    mode,
                    initialBookmarks,
                    null,
                    null,
                    false,
                    NoopLoggingProvider.INSTANCE);

            then(connection).should().write(eq(message), any(RunResponseHandler.class));
            then(handler).should().onSummary(any());
        } else {
            var stage = protocol.run(connection, query, query_params, handler);

            assertEquals(expectedStage, stage);
            var message = unmanagedTxRunMessage(query, query_params);
            then(connection).should().write(eq(message), any(RunResponseHandler.class));
            then(handler).should().onSummary(any());
        }
    }

    private void testDatabaseNameSupport(boolean autoCommitTx) {
        var connection = mock(Connection.class);
        given(connection.protocol()).willReturn(protocol);
        var expectedStage = CompletableFuture.<Void>completedStage(null);
        if (autoCommitTx) {
            given(connection.write(any(), any())).willAnswer((Answer<CompletionStage<Void>>) invocation -> {
                var runHandler = (RunResponseHandler) invocation.getArgument(1);
                runHandler.onSuccess(Collections.emptyMap());
                return expectedStage;
            });
            @SuppressWarnings("unchecked")
            var handler = (MessageHandler<RunSummary>) mock(MessageHandler.class);

            var stage = protocol.runAuto(
                    connection,
                    defaultDatabase(),
                    AccessMode.WRITE,
                    null,
                    query,
                    query_params,
                    Collections.emptySet(),
                    txTimeout,
                    txMetadata,
                    null,
                    handler,
                    NoopLoggingProvider.INSTANCE);
            assertEquals(expectedStage, stage);
            var message = autoCommitTxRunMessage(
                    query,
                    query_params,
                    txTimeout,
                    txMetadata,
                    defaultDatabase(),
                    AccessMode.WRITE,
                    Collections.emptySet(),
                    null,
                    null,
                    false,
                    NoopLoggingProvider.INSTANCE);
            then(connection).should().write(eq(message), any(RunResponseHandler.class));
            then(handler).should().onSummary(any());
        } else {
            given(connection.write(any(), any())).willAnswer((Answer<CompletionStage<Void>>) invocation -> {
                var beginHandler = (BeginTxResponseHandler) invocation.getArgument(1);
                beginHandler.onSuccess(emptyMap());
                return expectedStage;
            });
            @SuppressWarnings("unchecked")
            var handler = (MessageHandler<Void>) mock(MessageHandler.class);

            var stage = protocol.beginTransaction(
                    connection,
                    defaultDatabase(),
                    org.neo4j.driver.internal.bolt.api.AccessMode.WRITE,
                    null,
                    Collections.emptySet(),
                    null,
                    Collections.emptyMap(),
                    null,
                    null,
                    handler,
                    NoopLoggingProvider.INSTANCE);

            assertEquals(expectedStage, stage);
            var message = new BeginMessage(
                    Collections.emptySet(),
                    null,
                    Collections.emptyMap(),
                    defaultDatabase(),
                    org.neo4j.driver.internal.bolt.api.AccessMode.WRITE,
                    null,
                    null,
                    null,
                    false,
                    NoopLoggingProvider.INSTANCE);
            then(connection).should().write(eq(message), any(BeginTxResponseHandler.class));
            then(handler).should().onSummary(any());
        }
    }
}
