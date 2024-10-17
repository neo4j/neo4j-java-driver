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
package org.neo4j.driver.internal.bolt.basicimpl.messaging.v3;

import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes.messageDispatcher;
import static org.neo4j.driver.internal.bolt.basicimpl.messaging.request.CommitMessage.COMMIT;
import static org.neo4j.driver.internal.bolt.basicimpl.messaging.request.RollbackMessage.ROLLBACK;

import io.netty.channel.ChannelPromise;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.UnsupportedFeatureException;
import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.bolt.api.AccessMode;
import org.neo4j.driver.internal.bolt.api.BoltAgent;
import org.neo4j.driver.internal.bolt.api.BoltProtocolVersion;
import org.neo4j.driver.internal.bolt.api.BoltServerAddress;
import org.neo4j.driver.internal.bolt.api.ClusterComposition;
import org.neo4j.driver.internal.bolt.api.DatabaseName;
import org.neo4j.driver.internal.bolt.api.DatabaseNameUtil;
import org.neo4j.driver.internal.bolt.api.LoggingProvider;
import org.neo4j.driver.internal.bolt.api.NotificationConfig;
import org.neo4j.driver.internal.bolt.api.RoutingContext;
import org.neo4j.driver.internal.bolt.api.summary.DiscardSummary;
import org.neo4j.driver.internal.bolt.api.summary.PullSummary;
import org.neo4j.driver.internal.bolt.api.summary.RouteSummary;
import org.neo4j.driver.internal.bolt.api.summary.RunSummary;
import org.neo4j.driver.internal.bolt.basicimpl.handlers.BeginTxResponseHandler;
import org.neo4j.driver.internal.bolt.basicimpl.handlers.CommitTxResponseHandler;
import org.neo4j.driver.internal.bolt.basicimpl.handlers.DiscardResponseHandler;
import org.neo4j.driver.internal.bolt.basicimpl.handlers.HelloResponseHandler;
import org.neo4j.driver.internal.bolt.basicimpl.handlers.PullResponseHandlerImpl;
import org.neo4j.driver.internal.bolt.basicimpl.handlers.ResetResponseHandler;
import org.neo4j.driver.internal.bolt.basicimpl.handlers.RollbackTxResponseHandler;
import org.neo4j.driver.internal.bolt.basicimpl.handlers.RunResponseHandler;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.BoltProtocol;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.MessageFormat;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.MessageHandler;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.PullMessageHandler;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.request.BeginMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.request.DiscardMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.request.HelloMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.request.MultiDatabaseUtil;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.request.PullAllMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.request.ResetMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.request.RunWithMetadataMessage;
import org.neo4j.driver.internal.bolt.basicimpl.spi.Connection;
import org.neo4j.driver.internal.bolt.basicimpl.util.MetadataExtractor;
import org.neo4j.driver.types.MapAccessor;

public class BoltProtocolV3 implements BoltProtocol {
    public static final BoltProtocolVersion VERSION = new BoltProtocolVersion(3, 0);

    public static final BoltProtocol INSTANCE = new BoltProtocolV3();

    public static final MetadataExtractor METADATA_EXTRACTOR = new MetadataExtractor("t_first");

    private static final String ROUTING_CONTEXT = "context";
    private static final String GET_ROUTING_TABLE =
            "CALL dbms.cluster.routing.getRoutingTable($" + ROUTING_CONTEXT + ")";

    @Override
    public MessageFormat createMessageFormat() {
        return new MessageFormatV3();
    }

    @Override
    public void initializeChannel(
            String userAgent,
            BoltAgent boltAgent,
            Map<String, Value> authMap,
            RoutingContext routingContext,
            ChannelPromise channelInitializedPromise,
            NotificationConfig notificationConfig,
            Clock clock,
            CompletableFuture<Long> latestAuthMillisFuture) {
        var exception = verifyNotificationConfigSupported(notificationConfig);
        if (exception != null) {
            channelInitializedPromise.setFailure(exception);
            return;
        }
        var channel = channelInitializedPromise.channel();
        HelloMessage message;

        if (routingContext.isServerRoutingEnabled()) {
            message = new HelloMessage(
                    userAgent,
                    null,
                    authMap,
                    routingContext.toMap(),
                    includeDateTimeUtcPatchInHello(),
                    notificationConfig,
                    useLegacyNotifications());
        } else {
            message = new HelloMessage(
                    userAgent,
                    null,
                    authMap,
                    null,
                    includeDateTimeUtcPatchInHello(),
                    notificationConfig,
                    useLegacyNotifications());
        }

        var future = new CompletableFuture<String>();
        var handler = new HelloResponseHandler(future, channel, clock, latestAuthMillisFuture);
        messageDispatcher(channel).enqueue(handler);
        channel.writeAndFlush(message, channel.voidPromise());
        future.whenComplete((serverAgent, throwable) -> {
            if (throwable != null) {
                channelInitializedPromise.setFailure(throwable);
            } else {
                channelInitializedPromise.setSuccess();
            }
        });
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    public CompletionStage<Void> route(
            Connection connection,
            Map<String, Value> routingContext,
            Set<String> bookmarks,
            String databaseName,
            String impersonatedUser,
            MessageHandler<RouteSummary> handler,
            Clock clock,
            LoggingProvider logging) {
        var query = new Query(
                GET_ROUTING_TABLE, parameters(ROUTING_CONTEXT, routingContext).asMap(Values::value));

        var runMessage = RunWithMetadataMessage.autoCommitTxRunMessage(
                query.query(),
                query.parameters(),
                null,
                Collections.emptyMap(),
                DatabaseNameUtil.defaultDatabase(),
                AccessMode.WRITE,
                Collections.emptySet(),
                null,
                NotificationConfig.defaultConfig(),
                useLegacyNotifications(),
                logging);
        var runFuture = new CompletableFuture<RunSummary>();
        var runHandler = new RunResponseHandler(runFuture, METADATA_EXTRACTOR);
        var pullFuture = new CompletableFuture<PullSummary>();
        var records = new ArrayList<Record>();

        runFuture
                .thenCompose(ignored -> pullFuture)
                .thenApply(ignored -> {
                    var map = records.get(0);
                    var ttl = map.get("ttl").asLong();
                    var expirationTimestamp = clock.millis() + ttl * 1000;
                    if (ttl < 0 || ttl >= Long.MAX_VALUE / 1000L || expirationTimestamp < 0) {
                        expirationTimestamp = Long.MAX_VALUE;
                    }

                    Set<BoltServerAddress> readers = new LinkedHashSet<>();
                    Set<BoltServerAddress> writers = new LinkedHashSet<>();
                    Set<BoltServerAddress> routers = new LinkedHashSet<>();

                    for (var serversMap : map.get("servers").asList(MapAccessor::asMap)) {
                        var role = (Values.value(serversMap.get("role")).asString());
                        for (var server :
                                Values.value(serversMap.get("addresses")).asList()) {
                            var address =
                                    new BoltServerAddress(Values.value(server).asString());
                            switch (role) {
                                case "WRITE" -> writers.add(address);
                                case "READ" -> readers.add(address);
                                case "ROUTE" -> routers.add(address);
                            }
                        }
                    }
                    var db = map.get("db");
                    var name = db != null ? db.computeOrDefault(Value::asString, null) : null;

                    var clusterComposition =
                            new ClusterComposition(expirationTimestamp, readers, writers, routers, name);
                    return new RouteSummaryImpl(clusterComposition);
                })
                .whenComplete((summary, throwable) -> {
                    if (throwable != null) {
                        handler.onError(throwable);
                    } else {
                        handler.onSummary(summary);
                    }
                });

        return connection.write(runMessage, runHandler).thenCompose(ignored -> {
            var pullMessage = PullAllMessage.PULL_ALL;
            var pullHandler = new PullResponseHandlerImpl(new PullMessageHandler() {
                @Override
                public void onRecord(Value[] fields) {
                    var keys = runFuture.join().keys();
                    records.add(new InternalRecord(keys, fields));
                }

                @Override
                public void onError(Throwable throwable) {
                    pullFuture.completeExceptionally(throwable);
                }

                @Override
                public void onSummary(PullSummary success) {
                    pullFuture.complete(success);
                }
            });
            return connection.write(pullMessage, pullHandler);
        });
    }

    @Override
    public CompletionStage<Void> beginTransaction(
            Connection connection,
            DatabaseName databaseName,
            AccessMode accessMode,
            String impersonatedUser,
            Set<String> bookmarks,
            Duration txTimeout,
            Map<String, Value> txMetadata,
            String txType,
            NotificationConfig notificationConfig,
            MessageHandler<Void> handler,
            LoggingProvider logging) {
        var exception = verifyNotificationConfigSupported(notificationConfig);
        if (exception != null) {
            return CompletableFuture.failedStage(exception);
        }
        try {
            verifyDatabaseNameBeforeTransaction(databaseName);
        } catch (Exception error) {
            return CompletableFuture.failedFuture(error);
        }

        var beginTxFuture = new CompletableFuture<Void>();
        var beginMessage = new BeginMessage(
                bookmarks,
                txTimeout,
                txMetadata,
                databaseName,
                accessMode,
                impersonatedUser,
                txType,
                notificationConfig,
                useLegacyNotifications(),
                logging);
        beginTxFuture.whenComplete((ignored, throwable) -> {
            if (throwable != null) {
                handler.onError(throwable);
            } else {
                handler.onSummary(null);
            }
        });
        return connection.write(beginMessage, new BeginTxResponseHandler(beginTxFuture));
    }

    @Override
    public CompletionStage<Void> commitTransaction(Connection connection, MessageHandler<String> handler) {
        var commitFuture = new CompletableFuture<String>();
        commitFuture.whenComplete((bookmark, throwable) -> {
            if (throwable != null) {
                handler.onError(throwable);
            } else {
                handler.onSummary(bookmark);
            }
        });
        return connection.write(COMMIT, new CommitTxResponseHandler(commitFuture));
    }

    @Override
    public CompletionStage<Void> rollbackTransaction(Connection connection, MessageHandler<Void> handler) {
        var rollbackFuture = new CompletableFuture<Void>();
        rollbackFuture.whenComplete((ignored, throwable) -> {
            if (throwable != null) {
                handler.onError(throwable);
            } else {
                handler.onSummary(null);
            }
        });
        return connection.write(ROLLBACK, new RollbackTxResponseHandler(rollbackFuture));
    }

    @Override
    public CompletionStage<Void> reset(Connection connection, MessageHandler<Void> handler) {
        var resetFuture = new CompletableFuture<Void>();
        resetFuture.whenComplete((ignored, throwable) -> {
            if (throwable != null) {
                handler.onError(throwable);
            } else {
                handler.onSummary(null);
            }
        });
        var resetHandler = new ResetResponseHandler(resetFuture);
        return connection.write(ResetMessage.RESET, resetHandler);
    }

    @Override
    public CompletionStage<Void> telemetry(Connection connection, Integer api, MessageHandler<Void> handler) {
        return CompletableFuture.failedStage(new UnsupportedFeatureException("telemetry not supported"));
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    public CompletionStage<Void> runAuto(
            Connection connection,
            DatabaseName databaseName,
            AccessMode accessMode,
            String impersonatedUser,
            String query,
            Map<String, Value> parameters,
            Set<String> bookmarks,
            Duration txTimeout,
            Map<String, Value> txMetadata,
            NotificationConfig notificationConfig,
            MessageHandler<RunSummary> handler,
            LoggingProvider logging) {
        try {
            verifyDatabaseNameBeforeTransaction(databaseName);
        } catch (Exception error) {
            return CompletableFuture.failedFuture(error);
        }

        var runMessage = RunWithMetadataMessage.autoCommitTxRunMessage(
                query,
                parameters,
                txTimeout,
                txMetadata,
                databaseName,
                accessMode,
                bookmarks,
                impersonatedUser,
                notificationConfig,
                useLegacyNotifications(),
                logging);
        var runFuture = new CompletableFuture<RunSummary>();
        runFuture.whenComplete((summary, throwable) -> {
            if (throwable != null) {
                handler.onError(throwable);
            } else {
                handler.onSummary(summary);
            }
        });
        var runHandler = new RunResponseHandler(runFuture, METADATA_EXTRACTOR);
        return connection.write(runMessage, runHandler);
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    public CompletionStage<Void> run(
            Connection connection, String query, Map<String, Value> parameters, MessageHandler<RunSummary> handler) {
        var runMessage = RunWithMetadataMessage.unmanagedTxRunMessage(query, parameters);
        var runFuture = new CompletableFuture<RunSummary>();
        runFuture.whenComplete((summary, throwable) -> {
            if (throwable != null) {
                handler.onError(throwable);
            } else {
                handler.onSummary(summary);
            }
        });
        var runHandler = new RunResponseHandler(runFuture, METADATA_EXTRACTOR);
        return connection.write(runMessage, runHandler);
    }

    @Override
    public CompletionStage<Void> pull(Connection connection, long qid, long request, PullMessageHandler handler) {
        var pullMessage = PullAllMessage.PULL_ALL;
        var pullHandler = new PullResponseHandlerImpl(handler);
        return connection.write(pullMessage, pullHandler);
    }

    @Override
    public CompletionStage<Void> discard(
            Connection connection, long qid, long number, MessageHandler<DiscardSummary> handler) {
        var discardMessage = new DiscardMessage(number, qid);
        var discardFuture = new CompletableFuture<DiscardSummary>();
        discardFuture.whenComplete((ignored, throwable) -> {
            if (throwable != null) {
                handler.onError(throwable);
            } else {
                handler.onSummary(ignored);
            }
        });
        var discardHandler = new DiscardResponseHandler(discardFuture);
        return connection.write(discardMessage, discardHandler);
    }

    protected void verifyDatabaseNameBeforeTransaction(DatabaseName databaseName) {
        MultiDatabaseUtil.assertEmptyDatabaseName(databaseName, version());
    }

    @Override
    public BoltProtocolVersion version() {
        return VERSION;
    }

    protected boolean includeDateTimeUtcPatchInHello() {
        return false;
    }

    protected Neo4jException verifyNotificationConfigSupported(NotificationConfig notificationConfig) {
        Neo4jException exception = null;
        if (notificationConfig != null && !notificationConfig.equals(NotificationConfig.defaultConfig())) {
            exception = new UnsupportedFeatureException(String.format(
                    "Notification configuration is not supported on Bolt %s",
                    version().toString()));
        }
        return exception;
    }

    protected boolean useLegacyNotifications() {
        return true;
    }

    private record RouteSummaryImpl(ClusterComposition clusterComposition) implements RouteSummary {}

    public record Query(String query, Map<String, Value> parameters) {}
}
