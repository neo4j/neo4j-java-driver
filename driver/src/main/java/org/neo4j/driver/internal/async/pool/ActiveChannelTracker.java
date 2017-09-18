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
import io.netty.channel.pool.ChannelPoolHandler;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.util.ConcurrentSet;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Logging;

import static org.neo4j.driver.internal.async.ChannelAttributes.address;

public class ActiveChannelTracker implements ChannelPoolHandler
{
    private final ConcurrentMap<BoltServerAddress,ConcurrentSet<Channel>> addressToActiveChannelCount;
    private final Logger log;

    public ActiveChannelTracker( Logging logging )
    {
        this.addressToActiveChannelCount = new ConcurrentHashMap<>();
        this.log = logging.getLog( getClass().getSimpleName() );
    }

    @Override
    public void channelReleased( Channel channel )
    {
        log.debug( "Channel %s released back to the pool", channel );
        channelInactive( channel );
    }

    @Override
    public void channelAcquired( Channel channel )
    {
        log.debug( "Channel %s acquired from the pool", channel );
        channelActive( channel );
    }

    @Override
    public void channelCreated( Channel channel )
    {
        log.debug( "Channel %s created", channel );
        channelActive( channel );
    }

    public int activeChannelCount( BoltServerAddress address )
    {
        ConcurrentSet<Channel> activeChannels = addressToActiveChannelCount.get( address );
        return activeChannels == null ? 0 : activeChannels.size();
    }

    public void purge( BoltServerAddress address )
    {
        ConcurrentSet<Channel> activeChannels = addressToActiveChannelCount.remove( address );
        if ( activeChannels != null )
        {
            for ( Channel channel : activeChannels )
            {
                channel.close();
            }
        }
    }

    private void channelActive( Channel channel )
    {
        BoltServerAddress address = address( channel );
        ConcurrentSet<Channel> activeChannels = addressToActiveChannelCount.get( address );
        if ( activeChannels == null )
        {
            ConcurrentSet<Channel> newActiveChannels = new ConcurrentSet<>();
            ConcurrentSet<Channel> existingActiveChannels = addressToActiveChannelCount.putIfAbsent( address,
                    newActiveChannels );
            if ( existingActiveChannels == null )
            {
                activeChannels = newActiveChannels;
            }
            else
            {
                activeChannels = existingActiveChannels;
            }
        }

        activeChannels.add( channel );
    }

    private void channelInactive( Channel channel )
    {
        BoltServerAddress address = address( channel );
        ConcurrentSet<Channel> activeChannels = addressToActiveChannelCount.get( address );
        if ( activeChannels == null )
        {
            throw new IllegalStateException( "No channels exist for address '" + address + "'" );
        }
        activeChannels.remove( channel );
    }
}
