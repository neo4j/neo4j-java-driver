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
package org.neo4j.driver.internal.bolt.basicimpl.messaging;

import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes.protocolVersion;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.UnsupportedFeatureException;
import org.neo4j.driver.internal.bolt.api.AccessMode;
import org.neo4j.driver.internal.bolt.api.BoltAgent;
import org.neo4j.driver.internal.bolt.api.BoltProtocolVersion;
import org.neo4j.driver.internal.bolt.api.DatabaseName;
import org.neo4j.driver.internal.bolt.api.GqlStatusError;
import org.neo4j.driver.internal.bolt.api.LoggingProvider;
import org.neo4j.driver.internal.bolt.api.NotificationConfig;
import org.neo4j.driver.internal.bolt.api.RoutingContext;
import org.neo4j.driver.internal.bolt.api.summary.DiscardSummary;
import org.neo4j.driver.internal.bolt.api.summary.RouteSummary;
import org.neo4j.driver.internal.bolt.api.summary.RunSummary;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v3.BoltProtocolV3;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v4.BoltProtocolV4;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v41.BoltProtocolV41;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v42.BoltProtocolV42;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v43.BoltProtocolV43;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v44.BoltProtocolV44;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v5.BoltProtocolV5;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v51.BoltProtocolV51;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v52.BoltProtocolV52;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v53.BoltProtocolV53;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v54.BoltProtocolV54;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v55.BoltProtocolV55;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v56.BoltProtocolV56;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v57.BoltProtocolV57;
import org.neo4j.driver.internal.bolt.basicimpl.spi.Connection;

public interface BoltProtocol {
    MessageFormat createMessageFormat();

    void initializeChannel(
            String userAgent,
            BoltAgent boltAgent,
            Map<String, Value> authMap,
            RoutingContext routingContext,
            ChannelPromise channelInitializedPromise,
            NotificationConfig notificationConfig,
            Clock clock,
            CompletableFuture<Long> latestAuthMillisFuture);

    CompletionStage<Void> route(
            Connection connection,
            Map<String, Value> routingContext,
            Set<String> bookmarks,
            String databaseName,
            String impersonatedUser,
            MessageHandler<RouteSummary> handler,
            Clock clock,
            LoggingProvider logging);

    CompletionStage<Void> beginTransaction(
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
            LoggingProvider logging);

    CompletionStage<Void> commitTransaction(Connection connection, MessageHandler<String> handler);

    CompletionStage<Void> rollbackTransaction(Connection connection, MessageHandler<Void> handler);

    CompletionStage<Void> telemetry(Connection connection, Integer api, MessageHandler<Void> handler);

    CompletionStage<Void> runAuto(
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
            LoggingProvider logging);

    CompletionStage<Void> run(
            Connection connection, String query, Map<String, Value> parameters, MessageHandler<RunSummary> handler);

    CompletionStage<Void> pull(Connection connection, long qid, long request, PullMessageHandler handler);

    CompletionStage<Void> discard(Connection connection, long qid, long number, MessageHandler<DiscardSummary> handler);

    CompletionStage<Void> reset(Connection connection, MessageHandler<Void> handler);

    default CompletionStage<Void> logoff(Connection connection, MessageHandler<Void> handler) {
        return CompletableFuture.failedStage(new UnsupportedFeatureException("logoff not supported"));
    }

    default CompletionStage<Void> logon(
            Connection connection, Map<String, Value> authMap, Clock clock, MessageHandler<Void> handler) {
        return CompletableFuture.failedStage(new UnsupportedFeatureException("logon not supported"));
    }

    /**
     * Returns the protocol version. It can be used for version specific error messages.
     * @return the protocol version.
     */
    BoltProtocolVersion version();

    static BoltProtocol forChannel(Channel channel) {
        return forVersion(protocolVersion(channel));
    }

    static BoltProtocol forVersion(BoltProtocolVersion version) {
        if (BoltProtocolV3.VERSION.equals(version)) {
            return BoltProtocolV3.INSTANCE;
        } else if (BoltProtocolV4.VERSION.equals(version)) {
            return BoltProtocolV4.INSTANCE;
        } else if (BoltProtocolV41.VERSION.equals(version)) {
            return BoltProtocolV41.INSTANCE;
        } else if (BoltProtocolV42.VERSION.equals(version)) {
            return BoltProtocolV42.INSTANCE;
        } else if (BoltProtocolV43.VERSION.equals(version)) {
            return BoltProtocolV43.INSTANCE;
        } else if (BoltProtocolV44.VERSION.equals(version)) {
            return BoltProtocolV44.INSTANCE;
        } else if (BoltProtocolV5.VERSION.equals(version)) {
            return BoltProtocolV5.INSTANCE;
        } else if (BoltProtocolV51.VERSION.equals(version)) {
            return BoltProtocolV51.INSTANCE;
        } else if (BoltProtocolV52.VERSION.equals(version)) {
            return BoltProtocolV52.INSTANCE;
        } else if (BoltProtocolV53.VERSION.equals(version)) {
            return BoltProtocolV53.INSTANCE;
        } else if (BoltProtocolV54.VERSION.equals(version)) {
            return BoltProtocolV54.INSTANCE;
        } else if (BoltProtocolV55.VERSION.equals(version)) {
            return BoltProtocolV55.INSTANCE;
        } else if (BoltProtocolV56.VERSION.equals(version)) {
            return BoltProtocolV56.INSTANCE;
        } else if (BoltProtocolV57.VERSION.equals(version)) {
            return BoltProtocolV57.INSTANCE;
        }
        var message = "Unknown protocol version: " + version;
        throw new ClientException(
                GqlStatusError.UNKNOWN.getStatus(),
                GqlStatusError.UNKNOWN.getStatusDescription(message),
                "N/A",
                message,
                GqlStatusError.DIAGNOSTIC_RECORD,
                null);
    }
}
