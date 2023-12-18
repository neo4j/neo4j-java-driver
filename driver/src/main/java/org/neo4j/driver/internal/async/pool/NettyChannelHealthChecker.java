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

import static org.neo4j.driver.internal.async.connection.ChannelAttributes.authContext;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.creationTimestamp;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.lastUsedTimestamp;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.messageDispatcher;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.protocolVersion;

import io.netty.channel.Channel;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.PromiseNotifier;
import java.time.Clock;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.internal.async.connection.AuthorizationStateListener;
import org.neo4j.driver.internal.async.connection.ChannelAttributes;
import org.neo4j.driver.internal.async.inbound.ConnectionReadTimeoutHandler;
import org.neo4j.driver.internal.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.handlers.PingResponseHandler;
import org.neo4j.driver.internal.messaging.request.ResetMessage;
import org.neo4j.driver.internal.messaging.v51.BoltProtocolV51;

public class NettyChannelHealthChecker implements ChannelHealthChecker, AuthorizationStateListener {
    private final PoolSettings poolSettings;
    private final Clock clock;
    private final Logging logging;
    private final Logger log;
    private final AtomicLong minAuthTimestamp;

    public NettyChannelHealthChecker(PoolSettings poolSettings, Clock clock, Logging logging) {
        this.poolSettings = poolSettings;
        this.clock = clock;
        this.logging = logging;
        this.log = logging.getLog(getClass());
        this.minAuthTimestamp = new AtomicLong(-1);
    }

    @Override
    public Future<Boolean> isHealthy(Channel channel) {
        if (isTooOld(channel)) {
            return channel.eventLoop().newSucceededFuture(Boolean.FALSE);
        }
        Promise<Boolean> result = channel.eventLoop().newPromise();
        ACTIVE.isHealthy(channel).addListener(future -> {
            if (future.isCancelled()) {
                result.setSuccess(Boolean.FALSE);
            } else if (!future.isSuccess()) {
                var throwable = future.cause();
                if (throwable != null) {
                    result.setFailure(throwable);
                } else {
                    result.setSuccess(Boolean.FALSE);
                }
            } else {
                if (!(Boolean) future.get()) {
                    result.setSuccess(Boolean.FALSE);
                } else {
                    authContext(channel)
                            .getAuthTokenManager()
                            .getToken()
                            .whenCompleteAsync(
                                    (authToken, throwable) -> {
                                        if (throwable != null || authToken == null) {
                                            result.setSuccess(Boolean.FALSE);
                                        } else {
                                            var authContext = authContext(channel);
                                            if (authContext.getAuthTimestamp() != null) {
                                                authContext.setValidToken(authToken);
                                                var equal = authToken.equals(authContext.getAuthToken());
                                                if (isAuthExpiredByFailure(channel) || !equal) {
                                                    // Bolt versions prior to 5.1 do not support auth renewal
                                                    if (BoltProtocolV51.VERSION.compareTo(protocolVersion(channel))
                                                            > 0) {
                                                        result.setSuccess(Boolean.FALSE);
                                                    } else {
                                                        authContext.markPendingLogoff();
                                                        var downstreamCheck = hasBeenIdleForTooLong(channel)
                                                                ? ping(channel)
                                                                : channel.eventLoop()
                                                                        .newSucceededFuture(Boolean.TRUE);
                                                        downstreamCheck.addListener(new PromiseNotifier<>(result));
                                                    }
                                                } else {
                                                    var downstreamCheck = hasBeenIdleForTooLong(channel)
                                                            ? ping(channel)
                                                            : channel.eventLoop()
                                                                    .newSucceededFuture(Boolean.TRUE);
                                                    downstreamCheck.addListener(new PromiseNotifier<>(result));
                                                }
                                            } else {
                                                result.setSuccess(Boolean.FALSE);
                                            }
                                        }
                                    },
                                    channel.eventLoop());
                }
            }
        });
        return result;
    }

    private boolean isAuthExpiredByFailure(Channel channel) {
        var authTimestamp = authContext(channel).getAuthTimestamp();
        return authTimestamp != null && authTimestamp <= minAuthTimestamp.get();
    }

    @Override
    public void onExpired() {
        var now = clock.millis();
        minAuthTimestamp.getAndUpdate(prev -> Math.max(prev, now));
    }

    private boolean isTooOld(Channel channel) {
        if (poolSettings.maxConnectionLifetimeEnabled()) {
            var creationTimestampMillis = creationTimestamp(channel);
            var currentTimestampMillis = clock.millis();

            var ageMillis = currentTimestampMillis - creationTimestampMillis;
            var maxAgeMillis = poolSettings.maxConnectionLifetime();

            var tooOld = ageMillis > maxAgeMillis;
            if (tooOld) {
                log.trace(
                        "Failed acquire channel %s from the pool because it is too old: %s > %s",
                        channel, ageMillis, maxAgeMillis);
            }
            return tooOld;
        }
        return false;
    }

    private boolean hasBeenIdleForTooLong(Channel channel) {
        if (poolSettings.idleTimeBeforeConnectionTestEnabled()) {
            var lastUsedTimestamp = lastUsedTimestamp(channel);
            if (lastUsedTimestamp != null) {
                var idleTime = clock.millis() - lastUsedTimestamp;
                var idleTooLong = idleTime > poolSettings.idleTimeBeforeConnectionTest();

                if (idleTooLong) {
                    log.trace("Channel %s has been idle for %s and needs a ping", channel, idleTime);
                }

                return idleTooLong;
            }
        }
        return false;
    }

    private Future<Boolean> ping(Channel channel) {
        Promise<Boolean> result = channel.eventLoop().newPromise();
        var messageDispatcher = messageDispatcher(channel);
        messageDispatcher.enqueue(new PingResponseHandler(result, channel, logging));
        attachConnectionReadTimeoutHandler(channel, messageDispatcher);
        channel.writeAndFlush(ResetMessage.RESET, channel.voidPromise());
        return result;
    }

    private void attachConnectionReadTimeoutHandler(Channel channel, InboundMessageDispatcher messageDispatcher) {
        ChannelAttributes.connectionReadTimeout(channel).ifPresent(connectionReadTimeout -> {
            var connectionReadTimeoutHandler =
                    new ConnectionReadTimeoutHandler(connectionReadTimeout, TimeUnit.SECONDS);
            channel.pipeline().addFirst(connectionReadTimeoutHandler);
            log.debug("Added ConnectionReadTimeoutHandler");
            messageDispatcher.setBeforeLastHandlerHook((messageType) -> {
                channel.pipeline().remove(connectionReadTimeoutHandler);
                messageDispatcher.setBeforeLastHandlerHook(null);
                log.debug("Removed ConnectionReadTimeoutHandler");
            });
        });
    }
}
