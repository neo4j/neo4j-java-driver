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
package org.neo4j.driver.internal.async.pool;

import static java.util.Objects.requireNonNull;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.authContext;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.messageDispatcher;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.protocolVersion;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.setPoolId;
import static org.neo4j.driver.internal.util.Futures.asCompletionStage;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.channel.pool.FixedChannelPool;
import java.time.Clock;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.exceptions.UnsupportedFeatureException;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.async.connection.ChannelConnector;
import org.neo4j.driver.internal.handlers.LogoffResponseHandler;
import org.neo4j.driver.internal.handlers.LogonResponseHandler;
import org.neo4j.driver.internal.messaging.request.LogoffMessage;
import org.neo4j.driver.internal.messaging.request.LogonMessage;
import org.neo4j.driver.internal.metrics.ListenerEvent;
import org.neo4j.driver.internal.security.InternalAuthToken;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.internal.util.SessionAuthUtil;

public class NettyChannelPool implements ExtendedChannelPool {
    /**
     * Unlimited amount of parties are allowed to request channels from the pool.
     */
    private static final int MAX_PENDING_ACQUIRES = Integer.MAX_VALUE;
    /**
     * Do not check channels when they are returned to the pool.
     */
    private static final boolean RELEASE_HEALTH_CHECK = false;

    private final FixedChannelPool delegate;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final String id;
    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
    private final NettyChannelHealthChecker healthChecker;
    private final Clock clock;

    NettyChannelPool(
            BoltServerAddress address,
            ChannelConnector connector,
            Bootstrap bootstrap,
            NettyChannelTracker handler,
            NettyChannelHealthChecker healthCheck,
            long acquireTimeoutMillis,
            int maxConnections,
            Clock clock) {
        requireNonNull(address);
        requireNonNull(connector);
        requireNonNull(handler);
        requireNonNull(clock);
        this.id = poolId(address);
        this.healthChecker = healthCheck;
        this.clock = clock;
        this.delegate =
                new FixedChannelPool(
                        bootstrap,
                        handler,
                        healthCheck,
                        FixedChannelPool.AcquireTimeoutAction.FAIL,
                        acquireTimeoutMillis,
                        maxConnections,
                        MAX_PENDING_ACQUIRES,
                        RELEASE_HEALTH_CHECK) {
                    @Override
                    protected ChannelFuture connectChannel(Bootstrap bootstrap) {
                        ListenerEvent<?> creatingEvent = handler.channelCreating(id);
                        ChannelFuture connectedChannelFuture = connector.connect(address, bootstrap);
                        Channel channel = connectedChannelFuture.channel();
                        // This ensures that handler.channelCreated is called before SimpleChannelPool calls
                        // handler.channelAcquired
                        ChannelPromise trackedChannelFuture = channel.newPromise();
                        connectedChannelFuture.addListener(future -> {
                            if (future.isSuccess()) {
                                // notify pool handler about a successful connection
                                setPoolId(channel, id);
                                handler.channelCreated(channel, creatingEvent);
                                trackedChannelFuture.setSuccess();
                            } else {
                                handler.channelFailedToCreate(id);
                                trackedChannelFuture.setFailure(future.cause());
                            }
                        });
                        return trackedChannelFuture;
                    }
                };
    }

    @Override
    public CompletionStage<Void> close() {
        if (closed.compareAndSet(false, true)) {
            asCompletionStage(delegate.closeAsync(), closeFuture);
        }
        return closeFuture;
    }

    @Override
    public NettyChannelHealthChecker healthChecker() {
        return healthChecker;
    }

    @Override
    public CompletionStage<Channel> acquire(AuthToken overrideAuthToken) {
        return asCompletionStage(delegate.acquire()).thenCompose(channel -> pipelineAuth(channel, overrideAuthToken));
    }

    private CompletionStage<Channel> pipelineAuth(Channel channel, AuthToken overrideAuthToken) {
        CompletionStage<Channel> pipelinedAuthStage;
        var authContext = authContext(channel);
        if (overrideAuthToken != null) {
            // check protocol version
            var protocolVersion = protocolVersion(channel);
            if (!SessionAuthUtil.supportsSessionAuth(protocolVersion)) {
                pipelinedAuthStage = Futures.failedFuture(new UnsupportedFeatureException(String.format(
                        "Detected Bolt %s connection that does not support the auth token override feature, please make sure to have all servers communicating over Bolt 5.1 or above to use the feature",
                        protocolVersion)));
            } else {
                // auth or re-auth only if necessary
                if (!overrideAuthToken.equals(authContext.getAuthToken())) {
                    if (authContext.getAuthTimestamp() != null) {
                        messageDispatcher(channel).enqueue(new LogoffResponseHandler());
                        channel.write(LogoffMessage.INSTANCE);
                    }
                    messageDispatcher(channel).enqueue(new LogonResponseHandler(channel.voidPromise(), clock));
                    authContext.initiateAuth(overrideAuthToken);
                    channel.write(new LogonMessage(((InternalAuthToken) overrideAuthToken).toMap()));
                }
                pipelinedAuthStage = CompletableFuture.completedStage(channel);
            }
        } else {
            pipelinedAuthStage = authContext
                    .getAuthTokenManager()
                    .getToken()
                    .thenApplyAsync(
                            latestAuthToken -> {
                                if (authContext.getAuthTimestamp() != null) {
                                    if (!authContext.getAuthToken().equals(latestAuthToken)
                                            || authContext.isPendingLogoff()) {
                                        messageDispatcher(channel).enqueue(new LogoffResponseHandler());
                                        channel.write(LogoffMessage.INSTANCE);
                                        messageDispatcher(channel)
                                                .enqueue(new LogonResponseHandler(channel.voidPromise(), clock));
                                        authContext.initiateAuth(latestAuthToken);
                                        channel.write(new LogonMessage(((InternalAuthToken) latestAuthToken).toMap()));
                                    }
                                } else {
                                    messageDispatcher(channel)
                                            .enqueue(new LogonResponseHandler(channel.voidPromise(), clock));
                                    authContext.initiateAuth(latestAuthToken);
                                    channel.write(new LogonMessage(((InternalAuthToken) latestAuthToken).toMap()));
                                }
                                return channel;
                            },
                            channel.eventLoop());
        }
        return pipelinedAuthStage;
    }

    @Override
    public CompletionStage<Void> release(Channel channel) {
        return asCompletionStage(delegate.release(channel));
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public String id() {
        return this.id;
    }

    private String poolId(BoltServerAddress serverAddress) {
        return String.format("%s:%d-%d", serverAddress.host(), serverAddress.port(), this.hashCode());
    }
}
