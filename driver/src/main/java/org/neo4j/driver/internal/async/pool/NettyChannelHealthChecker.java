/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import io.netty.channel.Channel;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;

import org.neo4j.driver.internal.handlers.PingResponseHandler;
import org.neo4j.driver.internal.messaging.ResetMessage;
import org.neo4j.driver.internal.net.pooling.PoolSettings;
import org.neo4j.driver.internal.util.Clock;

import static java.util.Objects.requireNonNull;
import static org.neo4j.driver.internal.async.ChannelAttributes.creationTimestamp;
import static org.neo4j.driver.internal.async.ChannelAttributes.lastUsedTimestamp;
import static org.neo4j.driver.internal.async.ChannelAttributes.messageDispatcher;

public class NettyChannelHealthChecker implements ChannelHealthChecker
{
    private final PoolSettings poolSettings;
    private final Clock clock;

    public NettyChannelHealthChecker( PoolSettings poolSettings, Clock clock )
    {
        this.poolSettings = requireNonNull( poolSettings );
        this.clock = requireNonNull( clock );
    }

    @Override
    public Future<Boolean> isHealthy( Channel channel )
    {
        if ( isTooOld( channel ) )
        {
            return channel.eventLoop().newSucceededFuture( Boolean.FALSE );
        }
        if ( hasBeenIdleForTooLong( channel ) )
        {
            return ping( channel );
        }
        return ACTIVE.isHealthy( channel );
    }

    private boolean isTooOld( Channel channel )
    {
        if ( poolSettings.maxConnectionLifetimeEnabled() )
        {
            long creationTimestampMillis = creationTimestamp( channel );
            long currentTimestampMillis = clock.millis();
            long ageMillis = currentTimestampMillis - creationTimestampMillis;

            return ageMillis > poolSettings.maxConnectionLifetime();
        }
        return false;
    }

    private boolean hasBeenIdleForTooLong( Channel channel )
    {
        if ( poolSettings.idleTimeBeforeConnectionTestEnabled() )
        {
            Long lastUsedTimestamp = lastUsedTimestamp( channel );
            if ( lastUsedTimestamp != null )
            {
                long idleTime = clock.millis() - lastUsedTimestamp;
                return idleTime > poolSettings.idleTimeBeforeConnectionTest();
            }
        }
        return false;
    }

    private Future<Boolean> ping( Channel channel )
    {
        Promise<Boolean> result = channel.eventLoop().newPromise();
        messageDispatcher( channel ).queue( new PingResponseHandler( result ) );
        channel.writeAndFlush( ResetMessage.RESET );
        return result;
    }
}
