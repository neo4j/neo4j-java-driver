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
import java.util.concurrent.atomic.AtomicLong;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.exceptions.AuthorizationExpiredException;
import org.neo4j.driver.internal.async.connection.AuthorizationStateListener;
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
                                    var equal = authToken.equals(authContext.getAuthToken());
                                    if (isAuthExpiredByFailure(channel) || !equal) {
                                        // Bolt versions prior to 5.1 do not support auth renewal
                                        if (BoltProtocolV51.VERSION.compareTo(protocolVersion(channel)) > 0) {
                                            result.setSuccess(Boolean.FALSE);
                                        } else {
                                            authContext.markPendingLogoff();
                                            var downstreamCheck = hasBeenIdleForTooLong(channel)
                                                    ? ping(channel)
                                                    : ACTIVE.isHealthy(channel);
                                            downstreamCheck.addListener(new PromiseNotifier<>(result));
                                        }
                                    } else {
                                        var downstreamCheck = hasBeenIdleForTooLong(channel)
                                                ? ping(channel)
                                                : ACTIVE.isHealthy(channel);
                                        downstreamCheck.addListener(new PromiseNotifier<>(result));
                                    }
                                } else {
                                    result.setSuccess(Boolean.FALSE);
                                }
                            }
                        },
                        channel.eventLoop());
        return result;
    }

    private boolean isAuthExpiredByFailure(Channel channel) {
        var authTimestamp = authContext(channel).getAuthTimestamp();
        return authTimestamp != null && authTimestamp <= minAuthTimestamp.get();
    }

    @Override
    public void onExpired(AuthorizationExpiredException e, Channel channel) {
        var now = clock.millis();
        minAuthTimestamp.getAndUpdate(prev -> Math.max(prev, now));
    }

    private boolean isTooOld(Channel channel) {
        if (poolSettings.maxConnectionLifetimeEnabled()) {
            long creationTimestampMillis = creationTimestamp(channel);
            long currentTimestampMillis = clock.millis();

            long ageMillis = currentTimestampMillis - creationTimestampMillis;
            long maxAgeMillis = poolSettings.maxConnectionLifetime();

            boolean tooOld = ageMillis > maxAgeMillis;
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
            Long lastUsedTimestamp = lastUsedTimestamp(channel);
            if (lastUsedTimestamp != null) {
                long idleTime = clock.millis() - lastUsedTimestamp;
                boolean idleTooLong = idleTime > poolSettings.idleTimeBeforeConnectionTest();

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
        messageDispatcher(channel).enqueue(new PingResponseHandler(result, channel, logging));
        channel.writeAndFlush(ResetMessage.RESET, channel.voidPromise());
        return result;
    }
}
