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
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.concurrent.Promise;

import java.util.Map;

import org.neo4j.driver.internal.messaging.DiscardAllMessage;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.PullAllMessage;
import org.neo4j.driver.internal.messaging.RunMessage;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.v1.Value;

// todo: keep state flags to prohibit interaction with released connections
public class NettyConnection implements AsyncConnection
{
    private final BoltServerAddress address;
    private final Future<Channel> channelFuture;
    private final NettyChannelPool channelPool;

    public NettyConnection( BoltServerAddress address, Future<Channel> channelFuture, NettyChannelPool channelPool )
    {
        this.address = address;
        this.channelFuture = channelFuture;
        this.channelPool = channelPool;
    }

    @Override
    public void run( String statement, Map<String,Value> parameters, ResponseHandler handler )
    {
        send( new RunMessage( statement, parameters ), handler );
    }

    @Override
    public void pullAll( ResponseHandler handler )
    {
        send( PullAllMessage.PULL_ALL, handler );
    }

    @Override
    public void discardAll( ResponseHandler handler )
    {
        send( DiscardAllMessage.DISCARD_ALL, handler );
    }

    @Override
    public void reset( ResponseHandler handler )
    {

    }

    @Override
    public void resetAsync( ResponseHandler handler )
    {

    }

    @Override
    public void flush()
    {
        Channel channel = getChannelNow();
        if ( channel != null )
        {
            channel.flush();
        }
        else
        {
            channelFuture.addListener( new GenericFutureListener<Future<Channel>>()
            {
                @Override
                public void operationComplete( Future<Channel> future ) throws Exception
                {
                    if ( future.isSuccess() )
                    {
                        future.getNow().flush();
                    }
                }
            } );
        }
    }

    @Override
    public <T> Promise<T> newPromise()
    {
        Channel channel = getChannelNow();
        if ( channel != null )
        {
            return new DefaultPromise<>( channel.eventLoop() );
        }
        else
        {
            return new DefaultPromise<>( GlobalEventExecutor.INSTANCE );
        }
    }

    @Override
    public void execute( final Runnable command )
    {
        Channel channel = getChannelNow();
        if ( channel != null )
        {
            channel.eventLoop().execute( command );
        }
        else
        {
            channelFuture.addListener( new GenericFutureListener<Future<Channel>>()
            {
                @Override
                public void operationComplete( Future<Channel> future ) throws Exception
                {
                    Channel channel = channelFuture.getNow();
                    if ( channel != null )
                    {
                        channel.eventLoop().execute( command );
                    }
                }
            } );
        }
    }

    @Override
    public Future<Channel> channelFuture()
    {
        return channelFuture;
    }

    @Override
    public boolean isOpen()
    {
        Channel channel = getChannelNow();
        if ( channel != null )
        {
            return channel.isActive();
        }
        return false;
    }

    @Override
    public void release()
    {
        Channel channel = getChannelNow();
        if ( channel != null )
        {
            channelPool.release( channel );
        }
        else
        {
            channelFuture.addListener( new GenericFutureListener<Future<Channel>>()
            {
                @Override
                public void operationComplete( Future<Channel> future ) throws Exception
                {
                    Channel channel = channelFuture.getNow();
                    if ( channel != null )
                    {
                        channelPool.release( channel );
                    }
                }
            } );
        }
    }

    @Override
    public BoltServerAddress boltServerAddress()
    {
        return address;
    }

    private void send( final Message message, final ResponseHandler handler )
    {
        Channel channel = getChannelNow();
        if ( channel != null )
        {
            ChannelWriter.write( channel, message, handler, false );
        }
        else
        {
            channelFuture.addListener( new GenericFutureListener<Future<Channel>>()
            {
                @Override
                public void operationComplete( Future<Channel> future ) throws Exception
                {
                    if ( future.isSuccess() )
                    {
                        ChannelWriter.write( future.getNow(), message, handler, false );
                    }
                    else
                    {
                        handler.onFailure( future.cause() );
                    }
                }
            } );
        }
    }

    private Channel getChannelNow()
    {
        if ( channelFuture.isSuccess() )
        {
            return channelFuture.getNow();
        }
        return null;
    }
}
