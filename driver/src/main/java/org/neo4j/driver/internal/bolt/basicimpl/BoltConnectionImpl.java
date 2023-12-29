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

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.UnsupportedFeatureException;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.bolt.api.AccessMode;
import org.neo4j.driver.internal.bolt.api.BoltConnection;
import org.neo4j.driver.internal.bolt.api.BoltConnectionInfo;
import org.neo4j.driver.internal.bolt.api.BoltConnectionState;
import org.neo4j.driver.internal.bolt.api.BoltProtocolVersion;
import org.neo4j.driver.internal.bolt.api.NotificationConfig;
import org.neo4j.driver.internal.bolt.api.ResponseHandler;
import org.neo4j.driver.internal.bolt.api.TelemetryApi;
import org.neo4j.driver.internal.bolt.api.TransactionType;
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
import org.neo4j.driver.internal.bolt.routedimpl.cluster.RoutingContext;

public final class BoltConnectionImpl implements BoltConnection {
    private final Logging logging;

    private final BoltProtocol protocol;

    private final Connection connection;

    private final BoltConnectionInfo connectionInfo;

    private final AtomicReference<BoltConnectionState> stateRef = new AtomicReference<>(BoltConnectionState.OPEN);

    private final AtomicReference<CompletableFuture<Long>> latestAuthMillisRef = new AtomicReference<>();
    private final AtomicReference<Map<String, Value>> authMapRef = new AtomicReference<>();

    private final RoutingContext routingContext;
    private final Queue<Function<ResponseHandler, CompletionStage<Void>>> messageWriters;
    private final Clock clock;

    public BoltConnectionImpl(
            BoltProtocol protocol,
            Connection connection,
            Map<String, Value> authMap,
            CompletableFuture<Long> latestAuthMillisFuture,
            RoutingContext routingContext,
            Clock clock,
            Logging logging) {
        this.protocol = Objects.requireNonNull(protocol);
        this.connection = Objects.requireNonNull(connection);
        this.connectionInfo = new BoltConnectionInfoRecord(
                connection.serverAgent(),
                connection.serverAddress(),
                connection.protocol().version(),
                connection.isTelemetryEnabled());
        this.authMapRef.set(authMap);
        this.latestAuthMillisRef.set(Objects.requireNonNull(latestAuthMillisFuture));
        this.routingContext = Objects.requireNonNull(routingContext);
        this.messageWriters = new ArrayDeque<>();
        this.clock = Objects.requireNonNull(clock);
        this.logging = Objects.requireNonNull(logging);
    }

    @Override
    public CompletionStage<BoltConnection> route(Set<String> bookmarks, String databaseName, String impersonatedUser) {
        var connectionFuture = new CompletableFuture<BoltConnection>();
        try {
            var mappedContext = new HashMap<String, Value>();
            for (var entry : routingContext.toMap().entrySet()) {
                mappedContext.put(entry.getKey(), Values.value(entry.getValue()));
            }
            messageWriters.add(handler -> protocol.route(
                    this.connection,
                    mappedContext,
                    bookmarks,
                    databaseName,
                    impersonatedUser,
                    new MessageHandler<>() {
                        @Override
                        public void onError(Throwable throwable) {
                            if (throwable instanceof ServiceUnavailableException) {
                                stateRef.set(BoltConnectionState.CLOSED);
                            } else {
                                stateRef.set(BoltConnectionState.ERROR);
                            }
                            handler.onError(throwable);
                        }

                        @Override
                        public void onSummary(RouteSummary summary) {
                            handler.onRouteSummary(summary);
                        }
                    },
                    clock,
                    logging));
        } catch (Throwable throwable) {
            connectionFuture.completeExceptionally(throwable);
        }
        connectionFuture.complete(this);
        return connectionFuture;
    }

    @Override
    public CompletionStage<BoltConnection> beginTransaction(
            Set<String> bookmarks,
            TransactionType transactionType,
            Duration txTimeout,
            Map<String, Value> txMetadata,
            NotificationConfig notificationConfig) {
        var connectionFuture = new CompletableFuture<BoltConnection>();
        try {
            messageWriters.add(handler -> protocol.beginTransaction(
                    this.connection,
                    bookmarks,
                    txTimeout,
                    txMetadata,
                    null,
                    notificationConfig,
                    new MessageHandler<>() {
                        @Override
                        public void onError(Throwable throwable) {
                            if (throwable instanceof ServiceUnavailableException) {
                                stateRef.set(BoltConnectionState.CLOSED);
                            } else {
                                stateRef.set(BoltConnectionState.ERROR);
                            }
                            handler.onError(throwable);
                        }

                        @Override
                        public void onSummary(Void summary) {
                            handler.onBeginSummary(null);
                        }
                    },
                    logging));
        } catch (Throwable throwable) {
            connectionFuture.completeExceptionally(throwable);
        }
        connectionFuture.complete(this);
        return connectionFuture;
    }

    @Override
    public CompletionStage<BoltConnection> runInAutoCommitTransaction(
            String query,
            Map<String, Value> parameters,
            Set<String> bookmarks,
            Duration txTimeout,
            Map<String, Value> txMetadata,
            NotificationConfig notificationConfig) {
        var connectionFuture = new CompletableFuture<BoltConnection>();
        try {
            messageWriters.add(handler -> protocol.runAuto(
                    connection,
                    query,
                    parameters,
                    bookmarks,
                    txTimeout,
                    txMetadata,
                    notificationConfig,
                    new MessageHandler<RunSummary>() {
                        @Override
                        public void onError(Throwable throwable) {
                            if (throwable instanceof ServiceUnavailableException) {
                                stateRef.set(BoltConnectionState.CLOSED);
                            } else {
                                stateRef.set(BoltConnectionState.ERROR);
                            }
                            handler.onError(throwable);
                        }

                        @Override
                        public void onSummary(RunSummary summary) {
                            handler.onRunSummary(summary);
                        }
                    },
                    logging));
        } catch (Throwable throwable) {
            connectionFuture.completeExceptionally(throwable);
        }
        connectionFuture.complete(this);
        return connectionFuture;
    }

    @Override
    public CompletionStage<BoltConnection> run(String query, Map<String, Value> parameters) {
        var connectionFuture = new CompletableFuture<BoltConnection>();
        try {
            messageWriters.add(handler -> protocol.run(connection, query, parameters, new MessageHandler<>() {
                @Override
                public void onError(Throwable throwable) {
                    if (throwable instanceof ServiceUnavailableException) {
                        stateRef.set(BoltConnectionState.CLOSED);
                    } else {
                        stateRef.set(BoltConnectionState.ERROR);
                    }
                    handler.onError(throwable);
                }

                @Override
                public void onSummary(RunSummary summary) {
                    handler.onRunSummary(summary);
                }
            }));
        } catch (Throwable throwable) {
            connectionFuture.completeExceptionally(throwable);
        }
        connectionFuture.complete(this);
        return connectionFuture;
    }

    @Override
    public CompletionStage<BoltConnection> pull(long qid, long request) {
        var connectionFuture = new CompletableFuture<BoltConnection>();
        try {
            messageWriters.add(handler -> protocol.pull(connection, qid, request, new PullMessageHandler() {
                @Override
                public void onRecord(Value[] fields) {
                    handler.onRecord(fields);
                }

                @Override
                public void onError(Throwable throwable) {
                    if (throwable instanceof ServiceUnavailableException) {
                        stateRef.set(BoltConnectionState.CLOSED);
                    } else {
                        stateRef.set(BoltConnectionState.ERROR);
                    }
                    handler.onError(throwable);
                }

                @Override
                public void onSummary(PullSummary success) {
                    handler.onPullSummary(success);
                }
            }));
        } catch (Throwable throwable) {
            connectionFuture.completeExceptionally(throwable);
        }
        connectionFuture.complete(this);
        return connectionFuture;
    }

    @Override
    public CompletionStage<BoltConnection> discard(long qid, long number) {
        var connectionFuture = new CompletableFuture<BoltConnection>();
        try {
            messageWriters.add(handler -> protocol.discard(this.connection, qid, number, new MessageHandler<>() {
                @Override
                public void onError(Throwable throwable) {
                    if (throwable instanceof ServiceUnavailableException) {
                        stateRef.set(BoltConnectionState.CLOSED);
                    } else {
                        stateRef.set(BoltConnectionState.ERROR);
                    }
                    handler.onError(throwable);
                }

                @Override
                public void onSummary(DiscardSummary summary) {
                    handler.onDiscardSummary(summary);
                }
            }));
        } catch (Throwable throwable) {
            connectionFuture.completeExceptionally(throwable);
        }
        connectionFuture.complete(this);
        return connectionFuture;
    }

    @Override
    public CompletionStage<BoltConnection> commit() {
        var connectionFuture = new CompletableFuture<BoltConnection>();
        try {
            messageWriters.add(handler -> protocol.commitTransaction(connection, new MessageHandler<>() {
                @Override
                public void onError(Throwable throwable) {
                    if (throwable instanceof ServiceUnavailableException) {
                        stateRef.set(BoltConnectionState.CLOSED);
                    } else {
                        stateRef.set(BoltConnectionState.ERROR);
                    }
                    handler.onError(throwable);
                }

                @Override
                public void onSummary(String bookmark) {
                    handler.onCommitSummary(() -> Optional.ofNullable(bookmark));
                }
            }));
        } catch (Throwable throwable) {
            connectionFuture.completeExceptionally(throwable);
        }
        connectionFuture.complete(this);
        return connectionFuture;
    }

    @Override
    public CompletionStage<BoltConnection> rollback() {
        var connectionFuture = new CompletableFuture<BoltConnection>();
        try {
            messageWriters.add(handler -> protocol.rollbackTransaction(connection, new MessageHandler<>() {
                @Override
                public void onError(Throwable throwable) {
                    if (throwable instanceof ServiceUnavailableException) {
                        stateRef.set(BoltConnectionState.CLOSED);
                    } else {
                        stateRef.set(BoltConnectionState.ERROR);
                    }
                    handler.onError(throwable);
                }

                @Override
                public void onSummary(Void summary) {
                    handler.onRollbackSummary(null);
                }
            }));
        } catch (Throwable throwable) {
            connectionFuture.completeExceptionally(throwable);
        }
        connectionFuture.complete(this);
        return connectionFuture;
    }

    @Override
    public CompletionStage<BoltConnection> reset() {
        var connectionFuture = new CompletableFuture<BoltConnection>();
        try {
            messageWriters.add(handler -> protocol.reset(connection, new MessageHandler<>() {
                @Override
                public void onError(Throwable throwable) {
                    if (throwable instanceof ServiceUnavailableException) {
                        stateRef.set(BoltConnectionState.CLOSED);
                    } else {
                        stateRef.set(BoltConnectionState.ERROR);
                    }
                    handler.onError(throwable);
                }

                @Override
                public void onSummary(Void summary) {
                    stateRef.set(BoltConnectionState.OPEN);
                    handler.onResetSummary(null);
                }
            }));
        } catch (Throwable throwable) {
            connectionFuture.completeExceptionally(throwable);
        }
        connectionFuture.complete(this);
        return connectionFuture;
    }

    @Override
    public CompletionStage<BoltConnection> logoff() {
        var connectionFuture = new CompletableFuture<BoltConnection>();
        try {
            messageWriters.add(handler -> protocol.logoff(connection, new MessageHandler<>() {
                @Override
                public void onError(Throwable throwable) {
                    if (throwable instanceof ServiceUnavailableException) {
                        stateRef.set(BoltConnectionState.CLOSED);
                    } else {
                        stateRef.set(BoltConnectionState.ERROR);
                    }
                    handler.onError(throwable);
                }

                @Override
                public void onSummary(Void summary) {
                    handler.onLogoffSummary(null);
                }
            }));
        } catch (Throwable throwable) {
            connectionFuture.completeExceptionally(throwable);
        }
        connectionFuture.complete(this);
        return connectionFuture;
    }

    @Override
    public CompletionStage<BoltConnection> logon(Map<String, Value> authMap) {
        var connectionFuture = new CompletableFuture<BoltConnection>();
        try {
            messageWriters.add(handler -> protocol.logon(connection, authMap, clock, new MessageHandler<>() {
                @Override
                public void onError(Throwable throwable) {
                    if (throwable instanceof ServiceUnavailableException) {
                        stateRef.set(BoltConnectionState.CLOSED);
                    } else {
                        stateRef.set(BoltConnectionState.ERROR);
                    }
                    handler.onError(throwable);
                }

                @Override
                public void onSummary(Void summary) {
                    latestAuthMillisRef.set(CompletableFuture.completedFuture(clock.millis()));
                    authMapRef.set(authMap);
                    handler.onLogonSummary(null);
                }
            }));
        } catch (Throwable throwable) {
            connectionFuture.completeExceptionally(throwable);
        }
        connectionFuture.complete(this);
        return connectionFuture;
    }

    @Override
    public CompletionStage<BoltConnection> telemetry(TelemetryApi telemetryApi) {
        var connectionFuture = new CompletableFuture<BoltConnection>();
        try {
            if (!connectionInfo().telemetrySupported()) {
                connectionFuture.completeExceptionally(new UnsupportedFeatureException("telemetry not supported"));
            } else {
                messageWriters.add(
                        handler -> protocol.telemetry(connection, telemetryApi.getValue(), new MessageHandler<>() {
                            @Override
                            public void onError(Throwable throwable) {
                                if (throwable instanceof ServiceUnavailableException) {
                                    stateRef.set(BoltConnectionState.CLOSED);
                                } else {
                                    stateRef.set(BoltConnectionState.ERROR);
                                }
                                handler.onError(throwable);
                            }

                            @Override
                            public void onSummary(Void summary) {
                                handler.onTelemetrySummary(null);
                            }
                        }));
            }
        } catch (Throwable throwable) {
            connectionFuture.completeExceptionally(throwable);
        }
        connectionFuture.complete(this);
        return connectionFuture;
    }

    @Override
    public CompletionStage<Void> flush(ResponseHandler handler) {
        var flushStage = CompletableFuture.<Void>completedStage(null);
        var expectedSummaries = messageWriters.size();
        var iterator = messageWriters.iterator();
        var responseHandler = new ResponseHandler() {
            private final CompletableFuture<Throwable> errorFuture = new CompletableFuture<>();
            private final CompletableFuture<Void> completed = new CompletableFuture<>();
            private final AtomicInteger summariesHandled = new AtomicInteger();

            {
                completed.whenComplete((ignored1, ignored2) -> onComplete());
            }

            private void handleSummary() {
                if (summariesHandled.getAndIncrement() == expectedSummaries) {
                    completed.complete(null);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                if (errorFuture.completeExceptionally(throwable)) {
                    handler.onError(throwable);
                }
                handleSummary();
            }

            @Override
            public void onBeginSummary(BeginSummary summary) {
                handler.onBeginSummary(summary);
                handleSummary();
            }

            @Override
            public void onRunSummary(RunSummary summary) {
                handler.onRunSummary(summary);
                handleSummary();
            }

            @Override
            public void onRecord(Value[] fields) {
                handler.onRecord(fields);
            }

            @Override
            public void onPullSummary(PullSummary summary) {
                handler.onPullSummary(summary);
                handleSummary();
            }

            @Override
            public void onDiscardSummary(DiscardSummary summary) {
                handler.onDiscardSummary(summary);
                handleSummary();
            }

            @Override
            public void onCommitSummary(CommitSummary summary) {
                handler.onCommitSummary(summary);
                handleSummary();
            }

            @Override
            public void onRollbackSummary(RollbackSummary summary) {
                handler.onRollbackSummary(summary);
                handleSummary();
            }

            @Override
            public void onResetSummary(ResetSummary summary) {
                handler.onResetSummary(summary);
                handleSummary();
            }

            @Override
            public void onRouteSummary(RouteSummary summary) {
                handler.onRouteSummary(summary);
                handleSummary();
            }

            @Override
            public void onLogoffSummary(LogoffSummary summary) {
                handler.onLogoffSummary(summary);
                handleSummary();
            }

            @Override
            public void onLogonSummary(LogonSummary summary) {
                handler.onLogonSummary(summary);
                handleSummary();
            }

            @Override
            public void onTelemetrySummary(TelemetrySummary summary) {
                handler.onTelemetrySummary(summary);
                handleSummary();
            }

            @Override
            public void onComplete() {
                handler.onComplete();
            }
        };
        while (iterator.hasNext()) {
            var messageWriter = iterator.next();
            iterator.remove();
            flushStage = flushStage.thenCompose(ignored -> messageWriter.apply(responseHandler));
        }
        return flushStage.thenCompose(ignored -> connection.flush());
    }

    @Override
    public CompletionStage<Void> close() {
        var resultFuture = new CompletableFuture<Void>();
        try {
            return connection.close();
        } catch (Throwable throwable) {
            resultFuture.completeExceptionally(throwable);
            return resultFuture;
        }
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
    public BoltConnectionInfo connectionInfo() {
        return connectionInfo;
    }

    @Override
    public Map<String, Value> authMap() {
        return authMapRef.get();
    }

    @Override
    public CompletionStage<Long> latestAuthMillis() {
        return latestAuthMillisRef.get();
    }

    @Override
    public void setDatabase(String database) {
        this.connection.setDatabase(database);
    }

    @Override
    public void setAccessMode(AccessMode mode) {
        this.connection.setAccessMode(mode);
    }

    @Override
    public void setImpersonatedUser(String impersonatedUser) {
        this.connection.setImpersonatedUser(impersonatedUser);
    }

    private record BoltConnectionInfoRecord(
            String serverAgent,
            BoltServerAddress serverAddress,
            BoltProtocolVersion protocolVersion,
            boolean telemetrySupported)
            implements BoltConnectionInfo {}
}
