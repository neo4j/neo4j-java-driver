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

import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.util.concurrent.EventExecutor;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.metrics.ListenerEvent;
import org.neo4j.driver.internal.metrics.MetricsListener;

import static org.neo4j.driver.internal.async.connection.ChannelAttributes.poolId;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.serverAddress;

public class NettyChannelTracker implements ChannelPoolHandler
{
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock read = lock.readLock();
    private final Lock write = lock.writeLock();
    private final Map<BoltServerAddress,Integer> addressToInUseChannelCount = new HashMap<>();
    private final Map<BoltServerAddress,Integer> addressToIdleChannelCount = new HashMap<>();
    private final Logger log;
    private final MetricsListener metricsListener;
    private final ChannelFutureListener closeListener = future -> channelClosed( future.channel() );
    private final ChannelGroup allChannels;

    public NettyChannelTracker( MetricsListener metricsListener, EventExecutor eventExecutor, Logging logging )
    {
        this( metricsListener, new DefaultChannelGroup( "all-connections", eventExecutor ), logging );
    }

    public NettyChannelTracker( MetricsListener metricsListener, ChannelGroup channels, Logging logging )
    {
        this.metricsListener = metricsListener;
        this.log = logging.getLog( getClass().getSimpleName() );
        this.allChannels = channels;
    }

    private void doInWriteLock( Runnable work )
    {
        try
        {
            write.lock();
            work.run();
        }
        finally
        {
            write.unlock();
        }
    }

    private <T> T retrieveInReadLock( Supplier<T> work )
    {
        try
        {
            read.lock();
            return work.get();
        }
        finally
        {
            read.unlock();
        }
    }

    @Override
    public void channelReleased( Channel channel )
    {
        doInWriteLock( () ->
        {
            decrementInUse( channel );
            incrementIdle( channel );
            channel.closeFuture().addListener( closeListener );
        } );

        log.debug( "Channel [0x%s] released back to the pool", channel.id() );
    }

    @Override
    public void channelAcquired( Channel channel )
    {
        doInWriteLock( () ->
        {
            incrementInUse( channel );
            decrementIdle( channel );
            channel.closeFuture().removeListener( closeListener );
        } );

        log.debug( "Channel [0x%s] acquired from the pool. Local address: %s, remote address: %s", channel.id(), channel.localAddress(),
                channel.remoteAddress() );
    }

    @Override
    public void channelCreated( Channel channel )
    {
        throw new IllegalStateException( "Untraceable channel created." );
    }

    public void channelCreated( Channel channel, ListenerEvent creatingEvent )
    {
        // when it is created, we count it as idle as it has not been acquired out of the pool
        doInWriteLock( () -> incrementIdle( channel ) );

        metricsListener.afterCreated( poolId( channel ), creatingEvent );
        allChannels.add( channel );
        log.debug( "Channel [0x%s] created. Local address: %s, remote address: %s", channel.id(), channel.localAddress(), channel.remoteAddress() );
    }

    public ListenerEvent channelCreating( String poolId )
    {
        ListenerEvent creatingEvent = metricsListener.createListenerEvent();
        metricsListener.beforeCreating( poolId, creatingEvent );
        return creatingEvent;
    }

    public void channelFailedToCreate( String poolId )
    {
        metricsListener.afterFailedToCreate( poolId );
    }

    public void channelClosed( Channel channel )
    {
        doInWriteLock( () -> decrementIdle( channel ) );
        metricsListener.afterClosed( poolId( channel ) );
    }

    public int inUseChannelCount( BoltServerAddress address )
    {
        return retrieveInReadLock( () -> addressToInUseChannelCount.getOrDefault( address, 0 ) );
    }

    public int idleChannelCount( BoltServerAddress address )
    {
        return retrieveInReadLock( () -> addressToIdleChannelCount.getOrDefault( address, 0 ) );
    }

    public void prepareToCloseChannels()
    {
        for ( Channel channel : allChannels )
        {
            BoltProtocol protocol = BoltProtocol.forChannel( channel );
            try
            {
                protocol.prepareToCloseChannel( channel );
            }
            catch ( Throwable e )
            {
                // only logging it
                log.debug( "Failed to prepare to close Channel %s due to error %s. " +
                                "It is safe to ignore this error as the channel will be closed despite if it is successfully prepared to close or not.", channel,
                        e.getMessage() );
            }
        }
    }

    private void incrementInUse( Channel channel )
    {
        increment( channel, addressToInUseChannelCount );
    }

    private void decrementInUse( Channel channel )
    {
        BoltServerAddress address = serverAddress( channel );
        if ( !addressToInUseChannelCount.containsKey( address ) )
        {
            throw new IllegalStateException( "No count exists for address '" + address + "' in the 'in use' count" );
        }
        Integer count = addressToInUseChannelCount.get( address );
        addressToInUseChannelCount.put( address, count - 1 );
    }

    private void incrementIdle( Channel channel )
    {
        increment( channel, addressToIdleChannelCount );
    }

    private void decrementIdle( Channel channel )
    {
        BoltServerAddress address = serverAddress( channel );
        if ( !addressToIdleChannelCount.containsKey( address ) )
        {
            throw new IllegalStateException( "No count exists for address '" + address + "' in the 'idle' count" );
        }
        Integer count = addressToIdleChannelCount.get( address );
        addressToIdleChannelCount.put( address, count - 1 );
    }

    private void increment( Channel channel, Map<BoltServerAddress,Integer> countMap )
    {
        BoltServerAddress address = serverAddress( channel );
        Integer count = countMap.computeIfAbsent( address, k -> 0 );
        countMap.put( address, count + 1 );
    }
}
