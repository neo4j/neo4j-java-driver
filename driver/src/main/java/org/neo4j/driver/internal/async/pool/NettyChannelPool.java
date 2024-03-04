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
package org.neo4j.driver.internal.async.pool;

import static java.util.Objects.requireNonNull;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.authContext;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.helloStage;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.messageDispatcher;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.protocolVersion;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.setPoolId;
import static org.neo4j.driver.internal.util.Futures.asCompletionStage;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.pool.FixedChannelPool;
import java.time.Clock;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
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
                        var creatingEvent = handler.channelCreating(id);
                        return connector.connect(address, bootstrap, connectedChannelFuture -> {
                            var channel = connectedChannelFuture.channel();
                            // This ensures that handler.channelCreated is called before SimpleChannelPool calls
                            // handler.channelAcquired
                            var trackedChannelFuture = channel.newPromise();
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
                        });
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
        return asCompletionStage(delegate.acquire()).thenCompose(channel -> auth(channel, overrideAuthToken));
    }

    private CompletionStage<Channel> auth(Channel channel, AuthToken overrideAuthToken) {
        CompletionStage<Channel> authStage;
        var authContext = authContext(channel);
        if (overrideAuthToken != null) {
            // check protocol version
            var protocolVersion = protocolVersion(channel);
            if (!SessionAuthUtil.supportsSessionAuth(protocolVersion)) {
                authStage = Futures.failedFuture(new UnsupportedFeatureException(String.format(
                        "Detected Bolt %s connection that does not support the auth token override feature, please make sure to have all servers communicating over Bolt 5.1 or above to use the feature",
                        protocolVersion)));
            } else {
                // auth or re-auth only if necessary
                if (!overrideAuthToken.equals(authContext.getAuthToken())) {
                    CompletableFuture<Void> logoffFuture;
                    if (authContext.getAuthTimestamp() != null) {
                        logoffFuture = new CompletableFuture<>();
                        messageDispatcher(channel).enqueue(new LogoffResponseHandler(logoffFuture));
                        channel.write(LogoffMessage.INSTANCE);
                    } else {
                        logoffFuture = null;
                    }
                    var logonFuture = new CompletableFuture<Void>();
                    messageDispatcher(channel).enqueue(new LogonResponseHandler(logonFuture, channel, clock));
                    authContext.initiateAuth(overrideAuthToken, false);
                    authContext.setValidToken(null);
                    channel.write(new LogonMessage(((InternalAuthToken) overrideAuthToken).toMap()));
                    if (logoffFuture == null) {
                        authStage = helloStage(channel)
                                .thenCompose(ignored -> logonFuture)
                                .thenApply(ignored -> channel);
                        channel.flush();
                    } else {
                        // do not await for re-login
                        authStage = CompletableFuture.completedStage(channel);
                    }
                } else {
                    authStage = CompletableFuture.completedStage(channel);
                }
            }
        } else {
            var validToken = authContext.getValidToken();
            authContext.setValidToken(null);
            var stage = validToken != null
                    ? CompletableFuture.completedStage(validToken)
                    : authContext.getAuthTokenManager().getToken();
            authStage = stage.thenComposeAsync(
                    latestAuthToken -> {
                        CompletionStage<Channel> result;
                        if (authContext.getAuthTimestamp() != null) {
                            if (!authContext.getAuthToken().equals(latestAuthToken) || authContext.isPendingLogoff()) {
                                var logoffFuture = new CompletableFuture<Void>();
                                messageDispatcher(channel).enqueue(new LogoffResponseHandler(logoffFuture));
                                channel.write(LogoffMessage.INSTANCE);
                                var logonFuture = new CompletableFuture<Void>();
                                messageDispatcher(channel)
                                        .enqueue(new LogonResponseHandler(logonFuture, channel, clock));
                                authContext.initiateAuth(latestAuthToken);
                                channel.write(new LogonMessage(((InternalAuthToken) latestAuthToken).toMap()));
                            }
                            // do not await for re-login
                            result = CompletableFuture.completedStage(channel);
                        } else {
                            var logonFuture = new CompletableFuture<Void>();
                            messageDispatcher(channel).enqueue(new LogonResponseHandler(logonFuture, channel, clock));
                            result = helloStage(channel)
                                    .thenCompose(ignored -> logonFuture)
                                    .thenApply(ignored -> channel);
                            authContext.initiateAuth(latestAuthToken);
                            channel.writeAndFlush(new LogonMessage(((InternalAuthToken) latestAuthToken).toMap()));
                        }
                        return result;
                    },
                    channel.eventLoop());
        }
        return authStage.handle((ignored, throwable) -> {
            if (throwable != null) {
                channel.close();
                release(channel);
                if (throwable instanceof RuntimeException runtimeException) {
                    throw runtimeException;
                } else {
                    throw new CompletionException(throwable);
                }
            } else {
                return channel;
            }
        });
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
