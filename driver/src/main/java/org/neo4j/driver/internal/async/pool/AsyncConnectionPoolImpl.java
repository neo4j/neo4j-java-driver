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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.Future;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.driver.internal.async.AsyncConnection;
import org.neo4j.driver.internal.async.AsyncConnector;
import org.neo4j.driver.internal.async.Futures;
import org.neo4j.driver.internal.async.InternalFuture;
import org.neo4j.driver.internal.async.NettyConnection;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.net.pooling.PoolSettings;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.util.Function;

public class AsyncConnectionPoolImpl implements AsyncConnectionPool
{
    private final AsyncConnector connector;
    private final Bootstrap bootstrap;
    private final ActiveChannelTracker activeChannelTracker;
    private final NettyChannelHealthChecker channelHealthChecker;
    private final PoolSettings settings;
    private final Logger log;

    private final ConcurrentMap<BoltServerAddress,NettyChannelPool> pools = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    public AsyncConnectionPoolImpl( AsyncConnector connector, Bootstrap bootstrap,
            ActiveChannelTracker activeChannelTracker, PoolSettings settings, Logging logging, Clock clock )
    {
        this.connector = connector;
        this.bootstrap = bootstrap;
        this.activeChannelTracker = activeChannelTracker;
        this.channelHealthChecker = new NettyChannelHealthChecker( settings, clock );
        this.settings = settings;
        this.log = logging.getLog( getClass().getSimpleName() );
    }

    @Override
    public InternalFuture<AsyncConnection> acquire( final BoltServerAddress address )
    {
        log.debug( "Acquiring connection from pool for address: %s", address );

        assertNotClosed();
        final NettyChannelPool pool = getOrCreatePool( address );
        final Future<Channel> connectionFuture = pool.acquire();

        return Futures.thenApply( connectionFuture, bootstrap, new Function<Channel,AsyncConnection>()
        {
            @Override
            public AsyncConnection apply( Channel channel )
            {
                assertNotClosed( address, channel, pool );
                return new NettyConnection( channel, pool );
            }
        } );
    }

    @Override
    public void purge( BoltServerAddress address )
    {
        log.info( "Purging connections for address: %s", address );

        // prune active connections
        activeChannelTracker.prune( address );

        // prune idle connections in the pool and pool itself
        NettyChannelPool pool = pools.remove( address );
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
    public Future<?> closeAsync()
    {
        if ( closed.compareAndSet( false, true ) )
        {
            log.info( "Closing the connection pool" );
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
                eventLoopGroup().shutdownGracefully();
            }
        }
        return eventLoopGroup().terminationFuture();
    }

    private NettyChannelPool getOrCreatePool( BoltServerAddress address )
    {
        NettyChannelPool pool = pools.get( address );
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

    private NettyChannelPool newPool( BoltServerAddress address )
    {
        return new NettyChannelPool( address, connector, bootstrap, activeChannelTracker, channelHealthChecker,
                settings.connectionAcquisitionTimeout(), settings.maxConnectionPoolSize() );
    }

    private EventLoopGroup eventLoopGroup()
    {
        return bootstrap.config().group();
    }

    private void assertNotClosed()
    {
        if ( closed.get() )
        {
            throw new IllegalStateException( "Pool closed" );
        }
    }

    private void assertNotClosed( BoltServerAddress address, Channel channel, NettyChannelPool pool )
    {
        if ( closed.get() )
        {
            pool.release( channel );
            pool.close();
            pools.remove( address );
            assertNotClosed();
        }
    }
}
