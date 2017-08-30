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
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.concurrent.Promise;

import java.util.Map;

import org.neo4j.driver.internal.messaging.DiscardAllMessage;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.PullAllMessage;
import org.neo4j.driver.internal.messaging.ResetMessage;
import org.neo4j.driver.internal.messaging.RunMessage;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.v1.Value;

// todo: keep state flags to prohibit interaction with released connections
public class NettyConnection implements AsyncConnection
{
    private final Future<Channel> channelFuture;
    private final NettyChannelPool channelPool;

    public NettyConnection( Future<Channel> channelFuture, NettyChannelPool channelPool )
    {
        this.channelFuture = channelFuture;
        this.channelPool = channelPool;
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
        channelFuture.addListener( FlushListener.INSTANCE );
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
    public void execute( Runnable command )
    {
        channelFuture.addListener( new ExecuteCommandListener( command ) );
    }

    @Override
    public void release()
    {
        write( ResetMessage.RESET, new ReleaseChannelHandler( channelFuture, channelPool ) );
        flush();
    }

    private void write( Message message, ResponseHandler handler )
    {
        channelFuture.addListener( new WriteListener( message, handler ) );
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
