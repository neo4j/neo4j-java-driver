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
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;

import org.neo4j.driver.internal.messaging.Message;

public class NettyConnection
{
    private final ChannelFuture channelFuture;

    public NettyConnection( ChannelFuture channelFuture )
    {
        this.channelFuture = channelFuture;
    }

    public ChannelPromise newPromise()
    {
        return channelFuture.channel().newPromise();
    }

    public void send( Message message, ResponseHandler handler )
    {
        send( message, handler, false );
    }

    public void sendAndFlush( Message message, ResponseHandler handler )
    {
        send( message, handler, true );
    }

    private void send( final Message message, final ResponseHandler handler, final boolean flush )
    {
        channelFuture.addListener( new ChannelFutureListener()
        {
            @Override
            public void operationComplete( ChannelFuture future ) throws Exception
            {
                if ( future.isSuccess() )
                {
                    Channel channel = channelFuture.channel();
                    ChannelPipeline pipeline = channel.pipeline();

                    InboundMessageHandler messageHandler = pipeline.get( InboundMessageHandler.class );
                    messageHandler.addHandler( handler );

                    if ( flush )
                    {
                        channel.writeAndFlush( message );
                    }
                    else
                    {
                        channel.write( message );
                    }
                }
                else
                {
                    // todo: not sure how to handle this error
                }
            }
        } );
    }
}
