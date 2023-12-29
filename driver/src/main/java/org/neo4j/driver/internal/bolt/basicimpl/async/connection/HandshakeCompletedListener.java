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
package org.neo4j.driver.internal.bolt.basicimpl.async.connection;

import static java.util.Objects.requireNonNull;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelPromise;
import java.time.Clock;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.bolt.api.BoltAgent;
import org.neo4j.driver.internal.bolt.api.NotificationConfig;
import org.neo4j.driver.internal.bolt.api.RoutingContext;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.BoltProtocol;

public class HandshakeCompletedListener implements ChannelFutureListener {
    private final Map<String, Value> authMap;
    private final String userAgent;
    private final BoltAgent boltAgent;
    private final RoutingContext routingContext;
    private final ChannelPromise connectionInitializedPromise;
    private final NotificationConfig notificationConfig;
    private final Clock clock;
    private final CompletableFuture<Long> latestAuthMillisFuture;

    public HandshakeCompletedListener(
            Map<String, Value> authMap,
            String userAgent,
            BoltAgent boltAgent,
            RoutingContext routingContext,
            ChannelPromise connectionInitializedPromise,
            NotificationConfig notificationConfig,
            Clock clock,
            CompletableFuture<Long> latestAuthMillisFuture) {
        requireNonNull(clock, "clock must not be null");
        this.authMap = authMap;
        this.userAgent = requireNonNull(userAgent);
        this.boltAgent = requireNonNull(boltAgent);
        this.routingContext = routingContext;
        this.connectionInitializedPromise = requireNonNull(connectionInitializedPromise);
        this.notificationConfig = notificationConfig;
        this.clock = clock;
        this.latestAuthMillisFuture = latestAuthMillisFuture;
    }

    @Override
    public void operationComplete(ChannelFuture future) {
        if (future.isSuccess()) {
            var protocol = BoltProtocol.forChannel(future.channel());
            protocol.initializeChannel(
                    userAgent,
                    boltAgent,
                    authMap,
                    routingContext,
                    connectionInitializedPromise,
                    notificationConfig,
                    clock,
                    latestAuthMillisFuture);
        } else {
            connectionInitializedPromise.setFailure(future.cause());
        }
    }
}
