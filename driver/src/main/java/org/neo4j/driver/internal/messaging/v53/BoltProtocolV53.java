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
package org.neo4j.driver.internal.messaging.v53;

import static org.neo4j.driver.internal.async.connection.ChannelAttributes.messageDispatcher;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.setHelloStage;

import io.netty.channel.ChannelPromise;
import java.time.Clock;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.NotificationConfig;
import org.neo4j.driver.internal.BoltAgent;
import org.neo4j.driver.internal.cluster.RoutingContext;
import org.neo4j.driver.internal.handlers.HelloV51ResponseHandler;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.messaging.BoltProtocolVersion;
import org.neo4j.driver.internal.messaging.request.HelloMessage;
import org.neo4j.driver.internal.messaging.v52.BoltProtocolV52;

public class BoltProtocolV53 extends BoltProtocolV52 {
    public static final BoltProtocolVersion VERSION = new BoltProtocolVersion(5, 3);
    public static final BoltProtocol INSTANCE = new BoltProtocolV53();

    @Override
    public void initializeChannel(
            String userAgent,
            BoltAgent boltAgent,
            AuthToken authToken,
            RoutingContext routingContext,
            ChannelPromise channelInitializedPromise,
            NotificationConfig notificationConfig,
            Clock clock) {
        var exception = verifyNotificationConfigSupported(notificationConfig);
        if (exception != null) {
            channelInitializedPromise.setFailure(exception);
            return;
        }
        var channel = channelInitializedPromise.channel();
        HelloMessage message;

        if (routingContext.isServerRoutingEnabled()) {
            message = new HelloMessage(
                    userAgent, boltAgent, Collections.emptyMap(), routingContext.toMap(), false, notificationConfig);
        } else {
            message = new HelloMessage(userAgent, boltAgent, Collections.emptyMap(), null, false, notificationConfig);
        }

        var helloFuture = new CompletableFuture<Void>();
        setHelloStage(channel, helloFuture);
        messageDispatcher(channel).enqueue(new HelloV51ResponseHandler(channel, helloFuture));
        channel.write(message, channel.voidPromise());
        channelInitializedPromise.setSuccess();
    }

    @Override
    public BoltProtocolVersion version() {
        return VERSION;
    }
}
