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
package org.neo4j.driver.internal.async;

import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.util.concurrent.Future;

import org.neo4j.driver.internal.net.pooling.PoolSettings;
import org.neo4j.driver.internal.util.Clock;

import static org.neo4j.driver.internal.async.ChannelAttributes.creationTimestamp;

public class NettyChannelHealthChecker implements ChannelHealthChecker
{
    private final PoolSettings poolSettings;
    private final Clock clock;

    public NettyChannelHealthChecker( PoolSettings poolSettings, Clock clock )
    {
        this.poolSettings = poolSettings;
        this.clock = clock;
    }

    @Override
    public Future<Boolean> isHealthy( Channel channel )
    {
        if ( isTooOld( channel ) )
        {
            EventLoop loop = channel.eventLoop();
            return loop.newSucceededFuture( Boolean.FALSE );
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
}
