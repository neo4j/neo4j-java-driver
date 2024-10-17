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
package org.neo4j.driver.internal.bolt.basicimpl.messaging.v51;

import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes.messageDispatcher;

import io.netty.channel.ChannelPromise;
import java.time.Clock;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.bolt.api.BoltAgent;
import org.neo4j.driver.internal.bolt.api.BoltProtocolVersion;
import org.neo4j.driver.internal.bolt.api.NotificationConfig;
import org.neo4j.driver.internal.bolt.api.RoutingContext;
import org.neo4j.driver.internal.bolt.basicimpl.handlers.HelloV51ResponseHandler;
import org.neo4j.driver.internal.bolt.basicimpl.handlers.LogoffResponseHandler;
import org.neo4j.driver.internal.bolt.basicimpl.handlers.LogonResponseHandler;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.BoltProtocol;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.MessageFormat;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.MessageHandler;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.request.HelloMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.request.LogoffMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.request.LogonMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v5.BoltProtocolV5;
import org.neo4j.driver.internal.bolt.basicimpl.spi.Connection;

public class BoltProtocolV51 extends BoltProtocolV5 {
    public static final BoltProtocolVersion VERSION = new BoltProtocolVersion(5, 1);
    public static final BoltProtocol INSTANCE = new BoltProtocolV51();

    @SuppressWarnings("DuplicatedCode")
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
                    Collections.emptyMap(),
                    routingContext.toMap(),
                    false,
                    notificationConfig,
                    useLegacyNotifications());
        } else {
            message = new HelloMessage(
                    userAgent, null, Collections.emptyMap(), null, false, notificationConfig, useLegacyNotifications());
        }

        var helloFuture = new CompletableFuture<String>();
        messageDispatcher(channel).enqueue(new HelloV51ResponseHandler(channel, helloFuture));
        channel.write(message, channel.voidPromise());

        var logonFuture = new CompletableFuture<Void>();
        var logon = new LogonMessage(authMap);
        messageDispatcher(channel)
                .enqueue(new LogonResponseHandler(logonFuture, channel, clock, latestAuthMillisFuture));
        channel.writeAndFlush(logon, channel.voidPromise());

        helloFuture.thenCompose(ignored -> logonFuture).whenComplete((ignored, throwable) -> {
            if (throwable != null) {
                channelInitializedPromise.setFailure(throwable);
            } else {
                channelInitializedPromise.setSuccess();
            }
        });
    }

    @Override
    public CompletionStage<Void> logoff(Connection connection, MessageHandler<Void> handler) {
        var logoffMessage = LogoffMessage.INSTANCE;
        var logoffFuture = new CompletableFuture<Void>();
        logoffFuture.whenComplete((ignored, throwable) -> {
            if (throwable != null) {
                handler.onError(throwable);
            } else {
                handler.onSummary(null);
            }
        });
        var logoffHandler = new LogoffResponseHandler(logoffFuture);
        return connection.write(logoffMessage, logoffHandler);
    }

    @Override
    public CompletionStage<Void> logon(
            Connection connection, Map<String, Value> authMap, Clock clock, MessageHandler<Void> handler) {
        var logonMessage = new LogonMessage(authMap);
        var logonFuture = new CompletableFuture<Long>();
        logonFuture.whenComplete((ignored, throwable) -> {
            if (throwable != null) {
                handler.onError(throwable);
            } else {
                handler.onSummary(null);
            }
        });
        var logonHandler = new LogonResponseHandler(logonFuture, null, clock, logonFuture);
        return connection.write(logonMessage, logonHandler);
    }

    @Override
    public BoltProtocolVersion version() {
        return VERSION;
    }

    @Override
    public MessageFormat createMessageFormat() {
        return new MessageFormatV51();
    }
}
