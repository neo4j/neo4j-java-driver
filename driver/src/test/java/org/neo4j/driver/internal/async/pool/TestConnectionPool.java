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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.driver.Logging;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.async.connection.ChannelConnector;
import org.neo4j.driver.internal.metrics.ListenerEvent;
import org.neo4j.driver.internal.metrics.MetricsListener;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.Clock;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.setPoolId;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.setServerAddress;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;

public class TestConnectionPool extends ConnectionPoolImpl
{
    final Map<BoltServerAddress,ExtendedChannelPool> channelPoolsByAddress = new HashMap<>();
    private final NettyChannelTracker nettyChannelTracker;

    public TestConnectionPool( Bootstrap bootstrap, NettyChannelTracker nettyChannelTracker, PoolSettings settings,
            MetricsListener metricsListener, Logging logging, Clock clock, boolean ownsEventLoopGroup )
    {
        super( mock( ChannelConnector.class ), bootstrap, nettyChannelTracker, settings, metricsListener, logging, clock, ownsEventLoopGroup,
                newConnectionFactory() );
        this.nettyChannelTracker = nettyChannelTracker;
    }

    ExtendedChannelPool getPool( BoltServerAddress address )
    {
        return channelPoolsByAddress.get( address );
    }

    @Override
    ExtendedChannelPool newPool( BoltServerAddress address )
    {
        ExtendedChannelPool channelPool = new ExtendedChannelPool()
        {
            private final AtomicBoolean isClosed = new AtomicBoolean( false );
            @Override
            public CompletionStage<Channel> acquire()
            {
                EmbeddedChannel channel = new EmbeddedChannel();
                setServerAddress( channel, address );
                setPoolId( channel, id() );

                ListenerEvent event = nettyChannelTracker.channelCreating( id() );
                nettyChannelTracker.channelCreated( channel, event );
                nettyChannelTracker.channelAcquired( channel );

                return completedFuture( channel );
            }

            @Override
            public CompletionStage<Void> release( Channel channel )
            {
                nettyChannelTracker.channelReleased( channel );
                nettyChannelTracker.channelClosed( channel );
                return completedWithNull();
            }

            @Override
            public boolean isClosed()
            {
                return isClosed.get();
            }

            @Override
            public String id()
            {
                return "Pool-" + this.hashCode();
            }

            @Override
            public CompletionStage<Void> close()
            {
                isClosed.set( true );
                return completedWithNull();
            }
        };
        channelPoolsByAddress.put( address, channelPool );
        return channelPool;
    }

    private static ConnectionFactory newConnectionFactory()
    {
        return ( channel, pool ) -> {
            Connection conn = mock( Connection.class );
            when( conn.release() ).thenAnswer( invocation -> pool.release( channel ) );
            return conn;
        };
    }
}
