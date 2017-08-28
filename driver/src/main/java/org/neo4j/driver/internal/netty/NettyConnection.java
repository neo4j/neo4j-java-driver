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
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Promise;

import java.util.Map;

import org.neo4j.driver.internal.messaging.DiscardAllMessage;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.PullAllMessage;
import org.neo4j.driver.internal.messaging.RunMessage;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.v1.Value;

public class NettyConnection implements AsyncConnection
{
    private final BoltServerAddress address;
    private final ChannelFuture channelFuture;

    public NettyConnection( BoltServerAddress address, ChannelFuture channelFuture )
    {
        this.address = address;
        this.channelFuture = channelFuture;
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
        // todo: it is possible to check future for completion and use channel directly
        channelFuture.addListener( new ChannelFutureListener()
        {
            @Override
            public void operationComplete( ChannelFuture future ) throws Exception
            {
                if ( future.isSuccess() )
                {
                    future.channel().flush();
                }
            }
        } );
    }

    @Override
    public <T> Promise<T> newPromise()
    {
        return new DefaultPromise<>( channel().eventLoop() );
    }

    @Override
    public void execute( Runnable command )
    {
        channel().eventLoop().execute( command );
    }

    @Override
    public boolean isOpen()
    {
        return channel().isActive();
    }

    @Override
    public void close()
    {
        // todo: is it ok to block like this???
        channel().close().syncUninterruptibly();
    }

    @Override
    public BoltServerAddress boltServerAddress()
    {
        return address;
    }

    private Channel channel()
    {
        return channelFuture.channel();
    }

    private void send( final Message message, final ResponseHandler handler )
    {
        // todo: it is possible to check future for completion and use channel directly
        channelFuture.addListener( new ChannelFutureListener()
        {
            @Override
            public void operationComplete( ChannelFuture future ) throws Exception
            {
                if ( future.isSuccess() )
                {
                    ChannelWriter.write( channel(), message, handler, false );
                }
                else
                {
                    handler.onFailure( future.cause() );
                }
            }
        } );
    }
}
