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

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.PullAllMessage;
import org.neo4j.driver.internal.messaging.ResetMessage;
import org.neo4j.driver.internal.messaging.RunMessage;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.v1.Value;

import static org.neo4j.driver.internal.async.ChannelAttributes.responseHandlersHolder;

// todo: keep state flags to prohibit interaction with released connections
public class NettyConnection implements AsyncConnection
{
    private final Channel channel;
    private final ResponseHandlersHolder responseHandlersHolder;
    private final NettyChannelPool channelPool;

    private final AtomicBoolean autoReadEnabled = new AtomicBoolean( true );

    private final NettyConnectionState state = new NettyConnectionState();

    public NettyConnection( Channel channel, NettyChannelPool channelPool )
    {
        this.channel = channel;
        this.responseHandlersHolder = responseHandlersHolder( channel );
        this.channelPool = channelPool;
    }

    @Override
    public boolean tryMarkInUse()
    {
        return state.markInUse();
    }

    @Override
    public void enableAutoRead()
    {
        if ( autoReadEnabled.compareAndSet( false, true ) )
        {
            System.out.println( "=== enableAutoRead" );
            setAutoRead( true );
        }
    }

    @Override
    public void disableAutoRead()
    {
        if ( autoReadEnabled.compareAndSet( true, false ) )
        {
            System.out.println( "=== disableAutoRead" );
            setAutoRead( false );
        }
    }

    @Override
    public void run( String statement, Map<String,Value> parameters, ResponseHandler handler )
    {
        write( new RunMessage( statement, parameters ), handler, false );
    }

    @Override
    public void pullAll( ResponseHandler handler )
    {
        write( PullAllMessage.PULL_ALL, handler, false );
    }

    @Override
    public void flush()
    {
        channel.flush();
    }

    @Override
    public <T> EventLoopAwarePromise<T> newPromise()
    {
        return new EventLoopAwarePromise<>( channel.eventLoop() );
    }

    @Override
    public void release()
    {
        if ( state.release() )
        {
            write( ResetMessage.RESET, new ReleaseChannelHandler( channel, channelPool ), true );
        }
    }

    @Override
    public EventLoopAwareFuture<Void> forceRelease()
    {
        EventLoopAwarePromise<Void> releasePromise = newPromise();

        if ( state.forceRelease() )
        {
            write( ResetMessage.RESET, new ReleaseChannelHandler( channel, channelPool, releasePromise ), true );
        }
        else
        {
            releasePromise.setSuccess( null );
        }

        return releasePromise;
    }

    private void write( Message message, ResponseHandler handler, boolean flush )
    {
        responseHandlersHolder.queue( handler );
        if ( flush )
        {
            channel.writeAndFlush( message );
        }
        else
        {
            channel.write( message );
        }
    }

    private void setAutoRead( boolean value )
    {
        channel.config().setAutoRead( value );
    }
}
