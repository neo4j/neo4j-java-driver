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
package org.neo4j.driver.internal.async.connection;

import static java.util.Objects.requireNonNull;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.authContext;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelPromise;
import java.time.Clock;
import org.neo4j.driver.NotificationConfig;
import org.neo4j.driver.internal.BoltAgent;
import org.neo4j.driver.internal.cluster.RoutingContext;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.messaging.v51.BoltProtocolV51;

public class HandshakeCompletedListener implements ChannelFutureListener {
    private final String userAgent;
    private final BoltAgent boltAgent;
    private final RoutingContext routingContext;
    private final ChannelPromise connectionInitializedPromise;
    private final NotificationConfig notificationConfig;
    private final Clock clock;

    public HandshakeCompletedListener(
            String userAgent,
            BoltAgent boltAgent,
            RoutingContext routingContext,
            ChannelPromise connectionInitializedPromise,
            NotificationConfig notificationConfig,
            Clock clock) {
        requireNonNull(clock, "clock must not be null");
        this.userAgent = requireNonNull(userAgent);
        this.boltAgent = requireNonNull(boltAgent);
        this.routingContext = routingContext;
        this.connectionInitializedPromise = requireNonNull(connectionInitializedPromise);
        this.notificationConfig = notificationConfig;
        this.clock = clock;
    }

    @Override
    public void operationComplete(ChannelFuture future) {
        if (future.isSuccess()) {
            var protocol = BoltProtocol.forChannel(future.channel());
            // pre Bolt 5.1
            if (BoltProtocolV51.VERSION.compareTo(protocol.version()) > 0) {
                var channel = connectionInitializedPromise.channel();
                var authContext = authContext(channel);
                authContext
                        .getAuthTokenManager()
                        .getToken()
                        .whenCompleteAsync(
                                (authToken, throwable) -> {
                                    if (throwable != null) {
                                        connectionInitializedPromise.setFailure(throwable);
                                    } else {
                                        authContext.initiateAuth(authToken);
                                        authContext.setValidToken(authToken);
                                        protocol.initializeChannel(
                                                userAgent,
                                                boltAgent,
                                                authToken,
                                                routingContext,
                                                connectionInitializedPromise,
                                                notificationConfig,
                                                clock);
                                    }
                                },
                                channel.eventLoop());
            } else {
                protocol.initializeChannel(
                        userAgent,
                        boltAgent,
                        null,
                        routingContext,
                        connectionInitializedPromise,
                        notificationConfig,
                        clock);
            }
        } else {
            connectionInitializedPromise.setFailure(future.cause());
        }
    }
}
