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
import io.netty.channel.ChannelFuture;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.FixedChannelPool;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.async.connection.ChannelConnector;
import org.neo4j.driver.internal.metrics.ListenerEvent;

import static java.util.Objects.requireNonNull;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.setPoolId;
import static org.neo4j.driver.internal.util.Futures.asCompletionStage;

public class NettyChannelPool implements ExtendedChannelPool
{
    /**
     * Unlimited amount of parties are allowed to request channels from the pool.
     */
    private static final int MAX_PENDING_ACQUIRES = Integer.MAX_VALUE;
    /**
     * Do not check channels when they are returned to the pool.
     */
    private static final boolean RELEASE_HEALTH_CHECK = false;

    private final FixedChannelPool delegate;
    private final AtomicBoolean closed = new AtomicBoolean( false );
    private final String id;
    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();

    NettyChannelPool( BoltServerAddress address, ChannelConnector connector, Bootstrap bootstrap, NettyChannelTracker handler,
            ChannelHealthChecker healthCheck, long acquireTimeoutMillis, int maxConnections )
    {
        requireNonNull( address );
        requireNonNull( connector );
        requireNonNull( handler );
        this.id = poolId( address );
        this.delegate = new FixedChannelPool( bootstrap, handler, healthCheck, FixedChannelPool.AcquireTimeoutAction.FAIL, acquireTimeoutMillis, maxConnections,
                MAX_PENDING_ACQUIRES, RELEASE_HEALTH_CHECK )
        {
            @Override
            protected ChannelFuture connectChannel( Bootstrap bootstrap )
            {
                ListenerEvent creatingEvent = handler.channelCreating( id );
                ChannelFuture channelFuture = connector.connect( address, bootstrap );
                channelFuture.addListener( future -> {
                    if ( future.isSuccess() )
                    {
                        // notify pool handler about a successful connection
                        Channel channel = channelFuture.channel();
                        setPoolId( channel, id );
                        handler.channelCreated( channel, creatingEvent );
                    }
                    else
                    {
                        handler.channelFailedToCreate( id );
                    }
                } );
                return channelFuture;
            }
        };
    }

    @Override
    public CompletionStage<Void> close()
    {
        if ( closed.compareAndSet( false, true ) )
        {
            asCompletionStage( delegate.closeAsync(), closeFuture );
        }
        return closeFuture;
    }

    @Override
    public CompletionStage<Channel> acquire()
    {
        return asCompletionStage( delegate.acquire() );
    }

    @Override
    public CompletionStage<Void> release( Channel channel )
    {
        return asCompletionStage( delegate.release( channel ) );
    }

    @Override
    public boolean isClosed()
    {
        return closed.get();
    }

    @Override
    public String id()
    {
        return this.id;
    }

    private String poolId( BoltServerAddress serverAddress )
    {
        return String.format( "%s:%d-%d", serverAddress.host(), serverAddress.port(), this.hashCode() );
    }
}
