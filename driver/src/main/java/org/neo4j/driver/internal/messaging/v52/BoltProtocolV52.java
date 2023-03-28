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
package org.neo4j.driver.internal.messaging.v52;

import static org.neo4j.driver.internal.async.connection.ChannelAttributes.messageDispatcher;

import io.netty.channel.ChannelPromise;
import java.util.Collections;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.NotificationConfig;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.internal.cluster.RoutingContext;
import org.neo4j.driver.internal.handlers.HelloResponseHandler;
import org.neo4j.driver.internal.handlers.LogonResponseHandler;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.messaging.BoltProtocolVersion;
import org.neo4j.driver.internal.messaging.request.HelloMessage;
import org.neo4j.driver.internal.messaging.request.LogonMessage;
import org.neo4j.driver.internal.messaging.v51.BoltProtocolV51;
import org.neo4j.driver.internal.security.InternalAuthToken;

public class BoltProtocolV52 extends BoltProtocolV51 {
    public static final BoltProtocolVersion VERSION = new BoltProtocolVersion(5, 2);
    public static final BoltProtocol INSTANCE = new BoltProtocolV52();

    @Override
    public void initializeChannel(
            String userAgent,
            AuthToken authToken,
            RoutingContext routingContext,
            ChannelPromise channelInitializedPromise,
            NotificationConfig notificationConfig) {
        var channel = channelInitializedPromise.channel();
        HelloMessage message;

        if (routingContext.isServerRoutingEnabled()) {
            message = new HelloMessage(
                    userAgent, Collections.emptyMap(), routingContext.toMap(), false, notificationConfig);
        } else {
            message = new HelloMessage(userAgent, Collections.emptyMap(), null, false, notificationConfig);
        }

        messageDispatcher(channel).enqueue(new HelloResponseHandler(channel.voidPromise()));
        messageDispatcher(channel).enqueue(new LogonResponseHandler(channelInitializedPromise));
        channel.write(message, channel.voidPromise());
        channel.writeAndFlush(new LogonMessage(((InternalAuthToken) authToken).toMap()));
    }

    @Override
    protected Neo4jException verifyNotificationConfigSupported(NotificationConfig notificationConfig) {
        return null;
    }

    @Override
    public BoltProtocolVersion version() {
        return VERSION;
    }
}
