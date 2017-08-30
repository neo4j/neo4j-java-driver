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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.util.concurrent.Future;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.net.pooling.PoolSettings;
import org.neo4j.driver.internal.util.Clock;

// todo: add logging
public class AsyncConnectionPoolImpl implements AsyncConnectionPool
{
    private final AsyncConnector connector;
    private final Bootstrap bootstrap;
    private final ActiveChannelTracker activeChannelTracker;
    private final NettyChannelHealthChecker channelHealthChecker;

    // todo: these two should come from PoolSettings
    private final long acquireTimeoutMillis;
    private final int maxConnections;

    private final ConcurrentMap<BoltServerAddress,NettyChannelPool> pools = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    public AsyncConnectionPoolImpl( AsyncConnector connector, Bootstrap bootstrap,
            ActiveChannelTracker activeChannelTracker, PoolSettings settings, Clock clock )
    {
        this.connector = connector;
        this.bootstrap = bootstrap;
        this.activeChannelTracker = activeChannelTracker;
        this.channelHealthChecker = new NettyChannelHealthChecker( settings, clock );
        // todo: fix next two settings
        this.acquireTimeoutMillis = TimeUnit.SECONDS.toMillis( 60 );
        this.maxConnections = 100;
    }

    @Override
    public AsyncConnection acquire( BoltServerAddress address )
    {
        assertNotClosed();
        NettyChannelPool pool = getOrCreatePool( address );
        Future<Channel> connectionFuture = pool.acquire();
        assertNotClosed( address, pool );
        return new NettyConnection( connectionFuture, pool );
    }

    @Override
    public void purge( BoltServerAddress address )
    {
        NettyChannelPool pool = getPool( address );
        if ( pool != null )
        {
            pool.close();
        }
    }

    @Override
    public boolean hasAddress( BoltServerAddress address )
    {
        return pools.containsKey( address );
    }

    @Override
    public int activeConnections( BoltServerAddress address )
    {
        return activeChannelTracker.activeChannelCount( address );
    }

    @Override
    public void close() throws IOException
    {
        if ( closed.compareAndSet( false, true ) )
        {
            try
            {
                for ( NettyChannelPool pool : pools.values() )
                {
                    pool.close();
                }

                pools.clear();
            }
            finally
            {
                // todo: do we need to sync here?
                bootstrap.config().group().shutdownGracefully();
            }
        }
    }

    private NettyChannelPool getOrCreatePool( BoltServerAddress address )
    {
        NettyChannelPool pool = getPool( address );
        if ( pool == null )
        {
            pool = newPool( address );

            if ( pools.putIfAbsent( address, pool ) != null )
            {
                // We lost a race to create the pool, dispose of the one we created, and recurse
                pool.close();
                return getOrCreatePool( address );
            }
        }
        return pool;
    }

    private NettyChannelPool getPool( BoltServerAddress address )
    {
        return pools.get( address );
    }

    private NettyChannelPool newPool( BoltServerAddress address )
    {
        return new NettyChannelPool( address, connector, bootstrap, activeChannelTracker, channelHealthChecker,
                acquireTimeoutMillis, maxConnections );
    }

    private void assertNotClosed()
    {
        if ( closed.get() )
        {
            throw new IllegalStateException( "Pool closed" );
        }
    }

    private void assertNotClosed( BoltServerAddress address, NettyChannelPool pool )
    {
        if ( closed.get() )
        {
            pool.close();
            pools.remove( address );
            assertNotClosed();
        }
    }
}
