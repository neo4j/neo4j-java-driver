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

import static org.neo4j.driver.internal.async.connection.ChannelAttributes.creationTimestamp;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.lastUsedTimestamp;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.messageDispatcher;

import io.netty.channel.Channel;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.exceptions.AuthorizationExpiredException;
import org.neo4j.driver.internal.async.connection.AuthorizationStateListener;
import org.neo4j.driver.internal.handlers.PingResponseHandler;
import org.neo4j.driver.internal.messaging.request.ResetMessage;
import org.neo4j.driver.internal.util.Clock;

public class NettyChannelHealthChecker implements ChannelHealthChecker, AuthorizationStateListener {
    private final PoolSettings poolSettings;
    private final Clock clock;
    private final Logging logging;
    private final Logger log;
    private final AtomicReference<Optional<Long>> minCreationTimestampMillisOpt;

    public NettyChannelHealthChecker(PoolSettings poolSettings, Clock clock, Logging logging) {
        this.poolSettings = poolSettings;
        this.clock = clock;
        this.logging = logging;
        this.log = logging.getLog(getClass());
        this.minCreationTimestampMillisOpt = new AtomicReference<>(Optional.empty());
    }

    @Override
    public Future<Boolean> isHealthy(Channel channel) {
        if (isTooOld(channel)) {
            return channel.eventLoop().newSucceededFuture(Boolean.FALSE);
        }
        if (hasBeenIdleForTooLong(channel)) {
            return ping(channel);
        }
        return ACTIVE.isHealthy(channel);
    }

    @Override
    public void onExpired(AuthorizationExpiredException e, Channel channel) {
        long ts = creationTimestamp(channel);
        // Override current value ONLY if the new one is greater
        minCreationTimestampMillisOpt.getAndUpdate(
                prev -> Optional.of(prev.filter(prevTs -> ts <= prevTs).orElse(ts)));
    }

    private boolean isTooOld(Channel channel) {
        long creationTimestampMillis = creationTimestamp(channel);
        Optional<Long> minCreationTimestampMillisOpt = this.minCreationTimestampMillisOpt.get();

        if (minCreationTimestampMillisOpt.isPresent()
                && creationTimestampMillis <= minCreationTimestampMillisOpt.get()) {
            log.trace(
                    "The channel %s is marked for closure as its creation timestamp is older than or equal to the acceptable minimum timestamp: %s <= %s",
                    channel, creationTimestampMillis, minCreationTimestampMillisOpt.get());
            return true;
        } else if (poolSettings.maxConnectionLifetimeEnabled()) {
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
