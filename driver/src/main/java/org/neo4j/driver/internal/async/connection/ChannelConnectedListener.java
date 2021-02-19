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
package org.neo4j.driver.internal.async.connection;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.logging.ChannelActivityLogger;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.exceptions.ServiceUnavailableException;

import static java.lang.String.format;
import static org.neo4j.driver.internal.async.connection.BoltProtocolUtil.handshakeBuf;
import static org.neo4j.driver.internal.async.connection.BoltProtocolUtil.handshakeString;

public class ChannelConnectedListener implements ChannelFutureListener
{
    private final BoltServerAddress address;
    private final ChannelPipelineBuilder pipelineBuilder;
    private final ChannelPromise handshakeCompletedPromise;
    private final Logging logging;

    public ChannelConnectedListener( BoltServerAddress address, ChannelPipelineBuilder pipelineBuilder,
            ChannelPromise handshakeCompletedPromise, Logging logging )
    {
        this.address = address;
        this.pipelineBuilder = pipelineBuilder;
        this.handshakeCompletedPromise = handshakeCompletedPromise;
        this.logging = logging;
    }

    @Override
    public void operationComplete( ChannelFuture future )
    {
        Channel channel = future.channel();
        Logger log = new ChannelActivityLogger( channel, logging, getClass() );

        if ( future.isSuccess() )
        {
            log.trace( "Channel %s connected, initiating bolt handshake", channel );

            ChannelPipeline pipeline = channel.pipeline();
            pipeline.addLast( new HandshakeHandler( pipelineBuilder, handshakeCompletedPromise, logging ) );
            log.debug( "C: [Bolt Handshake] %s", handshakeString() );
            channel.writeAndFlush( handshakeBuf(), channel.voidPromise() );
        }
        else
        {
            handshakeCompletedPromise.setFailure( databaseUnavailableError( address, future.cause() ) );
        }
    }

    private static Throwable databaseUnavailableError( BoltServerAddress address, Throwable cause )
    {
        return new ServiceUnavailableException( format(
                "Unable to connect to %s, ensure the database is running and that there " +
                "is a working network connection to it.", address ), cause );
    }
}
