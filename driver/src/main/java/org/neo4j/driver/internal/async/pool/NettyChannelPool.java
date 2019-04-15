/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import io.netty.channel.ChannelFuture;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.FixedChannelPool;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.async.connection.ChannelConnector;
import org.neo4j.driver.internal.metrics.ListenerEvent;

import static java.util.Objects.requireNonNull;

public class NettyChannelPool extends FixedChannelPool
{
    /**
     * Unlimited amount of parties are allowed to request channels from the pool.
     */
    private static final int MAX_PENDING_ACQUIRES = Integer.MAX_VALUE;
    /**
     * Do not check channels when they are returned to the pool.
     */
    private static final boolean RELEASE_HEALTH_CHECK = false;

    private final BoltServerAddress address;
    private final ChannelConnector connector;
    private final NettyChannelTracker handler;

    public NettyChannelPool( BoltServerAddress address, ChannelConnector connector, Bootstrap bootstrap, NettyChannelTracker handler,
            ChannelHealthChecker healthCheck, long acquireTimeoutMillis, int maxConnections )
    {
        super( bootstrap, handler, healthCheck, AcquireTimeoutAction.FAIL, acquireTimeoutMillis, maxConnections,
                MAX_PENDING_ACQUIRES, RELEASE_HEALTH_CHECK );

        this.address = requireNonNull( address );
        this.connector = requireNonNull( connector );
        this.handler = requireNonNull( handler );
    }

    @Override
    protected ChannelFuture connectChannel( Bootstrap bootstrap )
    {
        ListenerEvent creatingEvent = handler.channelCreating( address );
        ChannelFuture channelFuture = connector.connect( address, bootstrap );
        channelFuture.addListener( future ->
        {
            if ( future.isSuccess() )
            {
                // notify pool handler about a successful connection
                Channel channel = channelFuture.channel();
                handler.channelCreated( channel, creatingEvent );
            }
            else
            {
                handler.channelFailedToCreate( address );
            }
        } );
        return channelFuture;
    }
}
