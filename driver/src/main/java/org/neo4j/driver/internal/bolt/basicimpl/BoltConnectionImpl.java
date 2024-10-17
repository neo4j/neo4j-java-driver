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
package org.neo4j.driver.internal.bolt.basicimpl;

import io.netty.channel.EventLoop;
import io.netty.handler.codec.CodecException;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.exceptions.AuthorizationExpiredException;
import org.neo4j.driver.exceptions.ConnectionReadTimeoutException;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.ProtocolException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.UnsupportedFeatureException;
import org.neo4j.driver.internal.bolt.api.AccessMode;
import org.neo4j.driver.internal.bolt.api.AuthData;
import org.neo4j.driver.internal.bolt.api.BoltConnection;
import org.neo4j.driver.internal.bolt.api.BoltConnectionState;
import org.neo4j.driver.internal.bolt.api.BoltProtocolVersion;
import org.neo4j.driver.internal.bolt.api.BoltServerAddress;
import org.neo4j.driver.internal.bolt.api.DatabaseName;
import org.neo4j.driver.internal.bolt.api.LoggingProvider;
import org.neo4j.driver.internal.bolt.api.NotificationConfig;
import org.neo4j.driver.internal.bolt.api.ResponseHandler;
import org.neo4j.driver.internal.bolt.api.RoutingContext;
import org.neo4j.driver.internal.bolt.api.TelemetryApi;
import org.neo4j.driver.internal.bolt.api.TransactionType;
import org.neo4j.driver.internal.bolt.api.exception.MessageIgnoredException;
import org.neo4j.driver.internal.bolt.api.summary.BeginSummary;
import org.neo4j.driver.internal.bolt.api.summary.CommitSummary;
import org.neo4j.driver.internal.bolt.api.summary.DiscardSummary;
import org.neo4j.driver.internal.bolt.api.summary.LogoffSummary;
import org.neo4j.driver.internal.bolt.api.summary.LogonSummary;
import org.neo4j.driver.internal.bolt.api.summary.PullSummary;
import org.neo4j.driver.internal.bolt.api.summary.ResetSummary;
import org.neo4j.driver.internal.bolt.api.summary.RollbackSummary;
import org.neo4j.driver.internal.bolt.api.summary.RouteSummary;
import org.neo4j.driver.internal.bolt.api.summary.RunSummary;
import org.neo4j.driver.internal.bolt.api.summary.TelemetrySummary;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.BoltProtocol;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.MessageHandler;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.PullMessageHandler;
import org.neo4j.driver.internal.bolt.basicimpl.spi.Connection;
import org.neo4j.driver.internal.bolt.basicimpl.util.FutureUtil;

public final class BoltConnectionImpl implements BoltConnection {
    private final LoggingProvider logging;
    private final System.Logger log;
    private final BoltProtocol protocol;
    private final Connection connection;
    private final EventLoop eventLoop;
    private final String serverAgent;
    private final BoltServerAddress serverAddress;
    private final BoltProtocolVersion protocolVersion;
    private final boolean telemetrySupported;
    private final AtomicReference<BoltConnectionState> stateRef = new AtomicReference<>(BoltConnectionState.OPEN);
    private final AtomicReference<CompletableFuture<AuthData>> authDataRef;
    private final Map<String, Value> routingContext;
    private final Queue<Function<ResponseHandler, CompletionStage<Void>>> messageWriters;
    private final Clock clock;

    public BoltConnectionImpl(
            BoltProtocol protocol,
            Connection connection,
            EventLoop eventLoop,
            Map<String, Value> authMap,
            CompletableFuture<Long> latestAuthMillisFuture,
            RoutingContext routingContext,
            Clock clock,
            LoggingProvider logging) {
        this.protocol = Objects.requireNonNull(protocol);
        this.connection = Objects.requireNonNull(connection);
        this.eventLoop = Objects.requireNonNull(eventLoop);
        this.serverAgent = Objects.requireNonNull(connection.serverAgent());
        this.serverAddress = Objects.requireNonNull(connection.serverAddress());
        this.protocolVersion = Objects.requireNonNull(connection.protocol().version());
        this.telemetrySupported = connection.isTelemetryEnabled();
        this.authDataRef = new AtomicReference<>(
                CompletableFuture.completedFuture(new AuthDataImpl(authMap, latestAuthMillisFuture.join())));
        this.routingContext = routingContext.toMap().entrySet().stream()
                .collect(Collectors.toUnmodifiableMap(
                        Map.Entry::getKey, entry -> Values.value(entry.getValue()), (a, b) -> b));
        this.messageWriters = new ArrayDeque<>();
        this.clock = Objects.requireNonNull(clock);
        this.logging = Objects.requireNonNull(logging);
        this.log = this.logging.getLog(getClass());
    }

    @Override
    public CompletionStage<BoltConnection> route(
            DatabaseName databaseName, String impersonatedUser, Set<String> bookmarks) {
        return executeInEventLoop(() -> messageWriters.add(handler -> protocol.route(
                        this.connection,
                        this.routingContext,
                        bookmarks,
                        databaseName.databaseName().orElse(null),
                        impersonatedUser,
                        new MessageHandler<>() {
                            @Override
                            public void onError(Throwable throwable) {
                                updateState(throwable);
                                handler.onError(throwable);
                            }

                            @Override
                            public void onSummary(RouteSummary summary) {
                                handler.onRouteSummary(summary);
                            }
                        },
                        clock,
                        logging)))
                .thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<BoltConnection> beginTransaction(
            DatabaseName databaseName,
            AccessMode accessMode,
            String impersonatedUser,
            Set<String> bookmarks,
            TransactionType transactionType,
            Duration txTimeout,
            Map<String, Value> txMetadata,
            String txType,
            NotificationConfig notificationConfig) {
        return executeInEventLoop(() -> messageWriters.add(handler -> protocol.beginTransaction(
                        this.connection,
                        databaseName,
                        accessMode,
                        impersonatedUser,
                        bookmarks,
                        txTimeout,
                        txMetadata,
                        txType,
                        notificationConfig,
                        new MessageHandler<>() {
                            @Override
                            public void onError(Throwable throwable) {
                                updateState(throwable);
                                handler.onError(throwable);
                            }

                            @Override
                            public void onSummary(Void summary) {
                                handler.onBeginSummary(BeginSummaryImpl.INSTANCE);
                            }
                        },
                        logging)))
                .thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<BoltConnection> runInAutoCommitTransaction(
            DatabaseName databaseName,
            AccessMode accessMode,
            String impersonatedUser,
            Set<String> bookmarks,
            String query,
            Map<String, Value> parameters,
            Duration txTimeout,
            Map<String, Value> txMetadata,
            NotificationConfig notificationConfig) {
        return executeInEventLoop(() -> messageWriters.add(handler -> protocol.runAuto(
                        connection,
                        databaseName,
                        accessMode,
                        impersonatedUser,
                        query,
                        parameters,
                        bookmarks,
                        txTimeout,
                        txMetadata,
                        notificationConfig,
                        new MessageHandler<>() {
                            @Override
                            public void onError(Throwable throwable) {
                                updateState(throwable);
                                handler.onError(throwable);
                            }

                            @Override
                            public void onSummary(RunSummary summary) {
                                handler.onRunSummary(summary);
                            }
                        },
                        logging)))
                .thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<BoltConnection> run(String query, Map<String, Value> parameters) {
        return executeInEventLoop(() -> messageWriters.add(
                        handler -> protocol.run(connection, query, parameters, new MessageHandler<>() {
                            @Override
                            public void onError(Throwable throwable) {
                                updateState(throwable);
                                handler.onError(throwable);
                            }

                            @Override
                            public void onSummary(RunSummary summary) {
                                handler.onRunSummary(summary);
                            }
                        })))
                .thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<BoltConnection> pull(long qid, long request) {
        return executeInEventLoop(() ->
                        messageWriters.add(handler -> protocol.pull(connection, qid, request, new PullMessageHandler() {
                            @Override
                            public void onRecord(Value[] fields) {
                                handler.onRecord(fields);
                            }

                            @Override
                            public void onError(Throwable throwable) {
                                updateState(throwable);
                                handler.onError(throwable);
                            }

                            @Override
                            public void onSummary(PullSummary success) {
                                handler.onPullSummary(success);
                            }
                        })))
                .thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<BoltConnection> discard(long qid, long number) {
        return executeInEventLoop(() -> messageWriters.add(
                        handler -> protocol.discard(this.connection, qid, number, new MessageHandler<>() {
                            @Override
                            public void onError(Throwable throwable) {
                                updateState(throwable);
                                handler.onError(throwable);
                            }

                            @Override
                            public void onSummary(DiscardSummary summary) {
                                handler.onDiscardSummary(summary);
                            }
                        })))
                .thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<BoltConnection> commit() {
        return executeInEventLoop(() ->
                        messageWriters.add(handler -> protocol.commitTransaction(connection, new MessageHandler<>() {
                            @Override
                            public void onError(Throwable throwable) {
                                updateState(throwable);
                                handler.onError(throwable);
                            }

                            @Override
                            public void onSummary(String bookmark) {
                                handler.onCommitSummary(() -> Optional.ofNullable(bookmark));
                            }
                        })))
                .thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<BoltConnection> rollback() {
        return executeInEventLoop(() ->
                        messageWriters.add(handler -> protocol.rollbackTransaction(connection, new MessageHandler<>() {
                            @Override
                            public void onError(Throwable throwable) {
                                updateState(throwable);
                                handler.onError(throwable);
                            }

                            @Override
                            public void onSummary(Void summary) {
                                handler.onRollbackSummary(RollbackSummaryImpl.INSTANCE);
                            }
                        })))
                .thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<BoltConnection> reset() {
        return executeInEventLoop(
                        () -> messageWriters.add(handler -> protocol.reset(connection, new MessageHandler<>() {
                            @Override
                            public void onError(Throwable throwable) {
                                updateState(throwable);
                                handler.onError(throwable);
                            }

                            @Override
                            public void onSummary(Void summary) {
                                stateRef.set(BoltConnectionState.OPEN);
                                handler.onResetSummary(null);
                            }
                        })))
                .thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<BoltConnection> logoff() {
        return executeInEventLoop(
                        () -> messageWriters.add(handler -> protocol.logoff(connection, new MessageHandler<>() {
                            @Override
                            public void onError(Throwable throwable) {
                                updateState(throwable);
                                handler.onError(throwable);
                            }

                            @Override
                            public void onSummary(Void summary) {
                                authDataRef.set(new CompletableFuture<>());
                                handler.onLogoffSummary(null);
                            }
                        })))
                .thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<BoltConnection> logon(Map<String, Value> authMap) {
        return executeInEventLoop(() -> messageWriters.add(
                        handler -> protocol.logon(connection, authMap, clock, new MessageHandler<>() {
                            @Override
                            public void onError(Throwable throwable) {
                                updateState(throwable);
                                handler.onError(throwable);
                            }

                            @Override
                            public void onSummary(Void summary) {
                                authDataRef.get().complete(new AuthDataImpl(authMap, clock.millis()));
                                handler.onLogonSummary(null);
                            }
                        })))
                .thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<BoltConnection> telemetry(TelemetryApi telemetryApi) {
        return executeInEventLoop(() -> {
                    if (!telemetrySupported()) {
                        throw new UnsupportedFeatureException("telemetry not supported");
                    } else {
                        messageWriters.add(handler ->
                                protocol.telemetry(connection, telemetryApi.getValue(), new MessageHandler<>() {
                                    @Override
                                    public void onError(Throwable throwable) {
                                        updateState(throwable);
                                        handler.onError(throwable);
                                    }

                                    @Override
                                    public void onSummary(Void summary) {
                                        handler.onTelemetrySummary(TelemetrySummaryImpl.INSTANCE);
                                    }
                                }));
                    }
                })
                .thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<BoltConnection> clear() {
        return executeInEventLoop(messageWriters::clear).thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<Void> flush(ResponseHandler handler) {
        var flushFuture = new CompletableFuture<Void>();
        return executeInEventLoop(() -> {
                    if (connection.isOpen()) {
                        var flushStage = CompletableFuture.<Void>completedStage(null);
                        var responseHandler = new ResponseHandleImpl(handler, messageWriters.size());
                        var messageWriterIterator = messageWriters.iterator();
                        while (messageWriterIterator.hasNext()) {
                            var messageWriter = messageWriterIterator.next();
                            messageWriterIterator.remove();
                            flushStage = flushStage.thenCompose(ignored -> messageWriter.apply(responseHandler));
                        }
                        flushStage.thenCompose(ignored -> connection.flush()).whenComplete((ignored, throwable) -> {
                            if (throwable != null) {
                                throwable = FutureUtil.completionExceptionCause(throwable);
                                if (throwable instanceof CodecException
                                        && throwable.getCause() instanceof IOException) {
                                    var serviceError = new ServiceUnavailableException(
                                            "Connection to the database failed", throwable.getCause());
                                    forceClose("Connection has been closed due to encoding error")
                                            .whenComplete((ignored1, ignored2) ->
                                                    flushFuture.completeExceptionally(serviceError));
                                } else {
                                    flushFuture.completeExceptionally(throwable);
                                }
                            } else {
                                flushFuture.complete(null);
                                log.log(System.Logger.Level.DEBUG, "flushed");
                            }
                        });
                    } else {
                        throw new ServiceUnavailableException("Connection is closed");
                    }
                })
                .thenCompose(ignored -> flushFuture);
    }

    @Override
    public CompletionStage<Void> forceClose(String reason) {
        if (stateRef.getAndSet(BoltConnectionState.CLOSED) != BoltConnectionState.CLOSED) {
            try {
                return connection.forceClose(reason).exceptionally(ignored -> null);
            } catch (Throwable throwable) {
                return CompletableFuture.completedStage(null);
            }
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    @Override
    public CompletionStage<Void> close() {
        CompletionStage<Void> close;
        try {
            close = switch (stateRef.getAndSet(BoltConnectionState.CLOSED)) {
                case OPEN -> connection.close();
                case ERROR -> connection.forceClose("Closing connection after error");
                case FAILURE -> connection.forceClose("Closing connection after failure");
                case CLOSED -> CompletableFuture.completedStage(null);};
        } catch (Throwable throwable) {
            close = CompletableFuture.completedStage(null);
        }
        return close.exceptionally(ignored -> null);
    }

    @Override
    public BoltConnectionState state() {
        var state = stateRef.get();
        if (state == BoltConnectionState.OPEN) {
            if (!connection.isOpen()) {
                state = BoltConnectionState.CLOSED;
            }
        }
        return state;
    }

    @Override
    public CompletionStage<AuthData> authData() {
        return authDataRef.get();
    }

    @Override
    public String serverAgent() {
        return serverAgent;
    }

    @Override
    public BoltServerAddress serverAddress() {
        return serverAddress;
    }

    @Override
    public BoltProtocolVersion protocolVersion() {
        return protocolVersion;
    }

    @Override
    public boolean telemetrySupported() {
        return telemetrySupported;
    }

    private CompletionStage<Void> executeInEventLoop(Runnable runnable) {
        var executeStage = new CompletableFuture<Void>();
        Runnable stageCompletingRunnable = () -> {
            try {
                runnable.run();
            } catch (Throwable throwable) {
                executeStage.completeExceptionally(throwable);
            }
            executeStage.complete(null);
        };
        if (eventLoop.inEventLoop()) {
            stageCompletingRunnable.run();
        } else {
            try {
                eventLoop.execute(stageCompletingRunnable);
            } catch (Throwable throwable) {
                executeStage.completeExceptionally(throwable);
            }
        }
        return executeStage;
    }

    private void updateState(Throwable throwable) {
        if (throwable instanceof ServiceUnavailableException) {
            if (throwable instanceof ConnectionReadTimeoutException) {
                stateRef.compareAndExchange(BoltConnectionState.OPEN, BoltConnectionState.ERROR);
            } else {
                stateRef.set(BoltConnectionState.CLOSED);
            }
        } else if (throwable instanceof Neo4jException) {
            if (throwable instanceof AuthorizationExpiredException) {
                stateRef.compareAndExchange(BoltConnectionState.OPEN, BoltConnectionState.ERROR);
            } else {
                stateRef.compareAndExchange(BoltConnectionState.OPEN, BoltConnectionState.FAILURE);
            }
        } else {
            stateRef.updateAndGet(state -> switch (state) {
                case OPEN, FAILURE, ERROR -> BoltConnectionState.ERROR;
                case CLOSED -> BoltConnectionState.CLOSED;
            });
        }
    }

    private record AuthDataImpl(Map<String, Value> authMap, long authAckMillis) implements AuthData {}

    private static class ResponseHandleImpl implements ResponseHandler {
        private final ResponseHandler delegate;
        private final CompletableFuture<Void> summariesFuture = new CompletableFuture<>();
        private int expectedSummaries;

        private ResponseHandleImpl(ResponseHandler delegate, int expectedSummaries) {
            this.delegate = Objects.requireNonNull(delegate);
            this.expectedSummaries = expectedSummaries;

            summariesFuture.whenComplete((ignored1, ignored2) -> onComplete());
        }

        @Override
        public void onError(Throwable throwable) {
            if (!(throwable instanceof MessageIgnoredException)) {
                runIgnoringError(() -> delegate.onError(throwable));
                if (!(throwable instanceof Neo4jException)
                        || throwable instanceof ServiceUnavailableException
                        || throwable instanceof ProtocolException) {
                    // assume unrecoverable error, ensure onComplete
                    expectedSummaries = 1;
                }
                handleSummary();
            } else {
                onIgnored();
            }
        }

        @Override
        public void onBeginSummary(BeginSummary summary) {
            runIgnoringError(() -> delegate.onBeginSummary(summary));
            handleSummary();
        }

        @Override
        public void onRunSummary(RunSummary summary) {
            runIgnoringError(() -> delegate.onRunSummary(summary));
            handleSummary();
        }

        @Override
        public void onRecord(Value[] fields) {
            runIgnoringError(() -> delegate.onRecord(fields));
        }

        @Override
        public void onPullSummary(PullSummary summary) {
            runIgnoringError(() -> delegate.onPullSummary(summary));
            handleSummary();
        }

        @Override
        public void onDiscardSummary(DiscardSummary summary) {
            runIgnoringError(() -> delegate.onDiscardSummary(summary));
            handleSummary();
        }

        @Override
        public void onCommitSummary(CommitSummary summary) {
            runIgnoringError(() -> delegate.onCommitSummary(summary));
            handleSummary();
        }

        @Override
        public void onRollbackSummary(RollbackSummary summary) {
            runIgnoringError(() -> delegate.onRollbackSummary(summary));
            handleSummary();
        }

        @Override
        public void onResetSummary(ResetSummary summary) {
            runIgnoringError(() -> delegate.onResetSummary(summary));
            handleSummary();
        }

        @Override
        public void onRouteSummary(RouteSummary summary) {
            runIgnoringError(() -> delegate.onRouteSummary(summary));
            handleSummary();
        }

        @Override
        public void onLogoffSummary(LogoffSummary summary) {
            runIgnoringError(() -> delegate.onLogoffSummary(summary));
            handleSummary();
        }

        @Override
        public void onLogonSummary(LogonSummary summary) {
            runIgnoringError(() -> delegate.onLogonSummary(summary));
            handleSummary();
        }

        @Override
        public void onTelemetrySummary(TelemetrySummary summary) {
            runIgnoringError(() -> delegate.onTelemetrySummary(summary));
            handleSummary();
        }

        @Override
        public void onIgnored() {
            runIgnoringError(delegate::onIgnored);
            handleSummary();
        }

        @Override
        public void onComplete() {
            runIgnoringError(delegate::onComplete);
        }

        private void handleSummary() {
            expectedSummaries--;
            if (expectedSummaries == 0) {
                summariesFuture.complete(null);
            }
        }

        private void runIgnoringError(Runnable runnable) {
            try {
                runnable.run();
            } catch (Throwable ignored) {
            }
        }
    }

    private static class BeginSummaryImpl implements BeginSummary {
        private static final BeginSummary INSTANCE = new BeginSummaryImpl();
    }

    private static class TelemetrySummaryImpl implements TelemetrySummary {
        private static final TelemetrySummary INSTANCE = new TelemetrySummaryImpl();
    }

    private static class RollbackSummaryImpl implements RollbackSummary {
        private static final RollbackSummary INSTANCE = new RollbackSummaryImpl();
    }
}
