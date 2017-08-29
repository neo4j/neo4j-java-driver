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
package org.neo4j.driver.internal.netty;

import io.netty.channel.Channel;
import io.netty.channel.pool.ChannelPoolHandler;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.driver.internal.net.BoltServerAddress;

import static org.neo4j.driver.internal.netty.ChannelAttributes.address;

public class ActiveChannelTracker implements ChannelPoolHandler
{
    private static final AtomicInteger ZERO = new AtomicInteger();

    private final ConcurrentMap<BoltServerAddress,AtomicInteger> addressToActiveChannelCount =
            new ConcurrentHashMap<>();

    @Override
    public void channelReleased( Channel channel ) throws Exception
    {
        decrementActiveChannelCount( channel );
    }

    @Override
    public void channelAcquired( Channel channel ) throws Exception
    {
        incrementActiveChannelCount( channel );
    }

    @Override
    public void channelCreated( Channel channel ) throws Exception
    {
        incrementActiveChannelCount( channel );
    }

    public int activeChannelCount( BoltServerAddress address )
    {
        AtomicInteger count = addressToActiveChannelCount.get( address );
        return count == null ? 0 : count.get();
    }

    private void incrementActiveChannelCount( Channel channel )
    {
        BoltServerAddress address = address( channel );
        AtomicInteger activeChannelCount = addressToActiveChannelCount.get( address );
        if ( activeChannelCount == null )
        {
            AtomicInteger newCount = new AtomicInteger();
            AtomicInteger existingCount = addressToActiveChannelCount.putIfAbsent( address, newCount );
            if ( existingCount == null )
            {
                activeChannelCount = newCount;
            }
            else
            {
                activeChannelCount = existingCount;
            }
        }

        activeChannelCount.incrementAndGet();
    }

    private void decrementActiveChannelCount( Channel channel )
    {
        BoltServerAddress address = address( channel );
        AtomicInteger activeChannelCount = addressToActiveChannelCount.get( address );
        if ( activeChannelCount == null )
        {
            throw new IllegalStateException(
                    "Unable to decrement channel count for address " + address + " because it does not exist" );
        }
        int updatedActiveChannelCount = activeChannelCount.decrementAndGet();
        if ( updatedActiveChannelCount == 0 )
        {
            addressToActiveChannelCount.remove( address, ZERO );
        }
    }
}
