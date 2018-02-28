/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import io.netty.channel.pool.ChannelPoolHandler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.metrics.ListenerEvent.ConnectionListenerEvent;
import org.neo4j.driver.internal.metrics.MetricsListener;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Logging;

import static org.neo4j.driver.internal.async.ChannelAttributes.serverAddress;

public class NettyChannelTracker implements ChannelPoolHandler
{
    private final Map<BoltServerAddress,AtomicInteger> addressToInUseChannelCount = new ConcurrentHashMap<>();
    private final Map<BoltServerAddress,AtomicInteger> addressToIdleChannelCount = new ConcurrentHashMap<>();
    private final Logger log;
    private final MetricsListener metricsListener;

    public NettyChannelTracker( MetricsListener metricsListener, Logging logging )
    {
        this.metricsListener = metricsListener;
        this.log = logging.getLog( getClass().getSimpleName() );
    }

    @Override
    public void channelReleased( Channel channel )
    {
        log.debug( "Channel %s released back to the pool", channel );
        decrementInUse( channel );
        incrementIdle( channel );
    }

    @Override
    public void channelAcquired( Channel channel )
    {
        log.debug( "Channel %s acquired from the pool", channel );
        incrementInUse( channel );
        decrementIdle( channel );
    }

    @Override
    public void channelCreated( Channel channel )
    {
        throw new IllegalStateException( "A fatal error happened in this driver as this method should never be called. " +
                "Contact the driver developer." );
    }

    public void channelCreated( Channel channel, ConnectionListenerEvent creatingEvent )
    {
        log.debug( "Channel %s created", channel );
        incrementInUse( channel );
        metricsListener.afterCreated( serverAddress( channel ), creatingEvent );
    }

    public ConnectionListenerEvent channelCreating( BoltServerAddress address )
    {
        ConnectionListenerEvent creatingEvent = metricsListener.createConnectionListenerEvent();
        metricsListener.beforeCreating( address, creatingEvent );
        return creatingEvent;
    }

    public void channelFailedToCreate( BoltServerAddress address )
    {
        metricsListener.afterFailedToCreate( address );
    }

    public void channelClosed( Channel channel )
    {
        decrementIdle( channel );
        metricsListener.afterClosed( serverAddress( channel ) );
    }

    public int inUseChannelCount( BoltServerAddress address )
    {
        AtomicInteger count = addressToInUseChannelCount.get( address );
        return count == null ? 0 : count.get();
    }

    public int idleChannelCount( BoltServerAddress address )
    {
        AtomicInteger count = addressToIdleChannelCount.get( address );
        return count == null ? 0 : count.get();
    }

    private void incrementInUse( Channel channel )
    {
        increment( channel, addressToInUseChannelCount );
    }

    private void decrementInUse( Channel channel )
    {
        decrement( channel, addressToInUseChannelCount );
    }

    private void incrementIdle( Channel channel )
    {
        increment( channel, addressToIdleChannelCount );
    }

    private void decrementIdle( Channel channel )
    {
        decrement( channel, addressToIdleChannelCount );
    }

    private void increment( Channel channel, Map<BoltServerAddress,AtomicInteger> countMap )
    {
        BoltServerAddress address = serverAddress( channel );
        AtomicInteger count = countMap.computeIfAbsent( address, k -> new AtomicInteger() );
        count.incrementAndGet();
    }

    private void decrement( Channel channel, Map<BoltServerAddress,AtomicInteger> countMap )
    {
        BoltServerAddress address = serverAddress( channel );
        AtomicInteger count = countMap.get( address );
        if ( count == null )
        {
            throw new IllegalStateException( "No count exist for address '" + address + "'" );
        }
        count.decrementAndGet();
    }
}
