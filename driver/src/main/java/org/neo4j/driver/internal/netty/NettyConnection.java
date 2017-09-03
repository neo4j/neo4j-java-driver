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
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.concurrent.Promise;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.driver.internal.messaging.DiscardAllMessage;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.PullAllMessage;
import org.neo4j.driver.internal.messaging.ResetMessage;
import org.neo4j.driver.internal.messaging.RunMessage;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.v1.Value;

import static java.util.Objects.requireNonNull;

// todo: keep state flags to prohibit interaction with released connections
public class NettyConnection implements AsyncConnection
{
    private final Future<Channel> channelFuture;
    private final NettyChannelPool channelPool;

    private Channel channel;
    private InboundMessageDispatcher inboundMessageDispatcher;
    private final AtomicBoolean autoReadEnabled = new AtomicBoolean( true );

    private final AtomicInteger usageCounter = new AtomicInteger( 1 );

    public NettyConnection( Future<Channel> channelFuture, NettyChannelPool channelPool )
    {
        this.channelFuture = channelFuture;
        this.channelPool = channelPool;
    }

    @Override
    public boolean tryMarkInUse()
    {
        int counter = usageCounter.incrementAndGet();
        if ( counter <= 0 )
        {
            // connection is now being released and should not be used further
            return false;
        }
        return true;
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
        write( new RunMessage( statement, parameters ), handler );
    }

    @Override
    public void pullAll( ResponseHandler handler )
    {
        write( PullAllMessage.PULL_ALL, handler );
    }

    @Override
    public void discardAll( ResponseHandler handler )
    {
        write( DiscardAllMessage.DISCARD_ALL, handler );
    }

    @Override
    public void reset( ResponseHandler handler )
    {
        write( ResetMessage.RESET, handler );
    }

    @Override
    public void flush()
    {
        if ( tryExtractChannel() )
        {
            channel.flush();
        }
        else
        {
            channelFuture.addListener( FlushListener.INSTANCE );
        }
    }

    @Override
    public <T> Promise<T> newPromise()
    {
        if ( tryExtractChannel() )
        {
            return channel.eventLoop().newPromise();
        }
        else
        {
            return GlobalEventExecutor.INSTANCE.newPromise();
        }
    }

    @Override
    public void release()
    {
        int counter = usageCounter.decrementAndGet();
        if ( counter == 0 )
        {
            // no usages, channel is eligible for release
            // try to mark this channel as being released
            boolean canRelease = usageCounter.compareAndSet( 0, Integer.MIN_VALUE );
            if ( canRelease )
            {
                reset( new ReleaseChannelHandler( channelFuture, channelPool ) );
                flush();
            }
            // else we lost a race with some thread that incremented the usage counter
            // it will continue using this connection and try to release it when done
        }
    }

    private void write( Message message, ResponseHandler handler )
    {
        if ( tryExtractChannel() )
        {
            inboundMessageDispatcher.addHandler( handler );
            channel.write( message );
        }
        else
        {
            channelFuture.addListener( new WriteListener( message, handler ) );
        }
    }

    private void setAutoRead( boolean value )
    {
        if ( tryExtractChannel() )
        {
            channel.config().setAutoRead( value );
        }
        else
        {
            channelFuture.addListener( AutoReadListener.forValue( value ) );
        }
    }

    private boolean tryExtractChannel()
    {
        if ( channel != null )
        {
            return true;
        }
        else if ( channelFuture.isSuccess() )
        {
            channel = requireNonNull( channelFuture.getNow() );
            inboundMessageDispatcher = requireNonNull( channel.pipeline().get( InboundMessageDispatcher.class ) );
            return true;
        }
        else
        {
            return false;
        }
    }
}
