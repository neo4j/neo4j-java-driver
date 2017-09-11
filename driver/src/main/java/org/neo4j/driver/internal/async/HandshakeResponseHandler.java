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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.ReplayingDecoder;

import java.util.List;

import org.neo4j.driver.internal.async.inbound.ChunkDecoder;
import org.neo4j.driver.internal.async.inbound.InboundMessageHandler;
import org.neo4j.driver.internal.async.inbound.MessageDecoder;
import org.neo4j.driver.internal.async.outbound.OutboundMessageHandler;
import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.PackStreamMessageFormatV1;
import org.neo4j.driver.v1.exceptions.ClientException;

import static java.util.Objects.requireNonNull;
import static org.neo4j.driver.internal.async.ProtocolUtil.HTTP;
import static org.neo4j.driver.internal.async.ProtocolUtil.NO_PROTOCOL_VERSION;
import static org.neo4j.driver.internal.async.ProtocolUtil.PROTOCOL_VERSION_1;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;

public class HandshakeResponseHandler extends ReplayingDecoder<Void>
{
    private final ChannelPromise handshakeCompletedPromise;

    public HandshakeResponseHandler( ChannelPromise handshakeCompletedPromise )
    {
        this.handshakeCompletedPromise = requireNonNull( handshakeCompletedPromise );
    }

    @Override
    public void exceptionCaught( ChannelHandlerContext ctx, Throwable cause ) throws Exception
    {
        fail( ctx, cause );
    }

    // todo: add logging like in SocketClient
    @Override
    protected void decode( ChannelHandlerContext ctx, ByteBuf in, List<Object> out ) throws Exception
    {
        int serverSuggestedVersion = in.readInt();

        switch ( serverSuggestedVersion )
        {
        case PROTOCOL_VERSION_1:

            MessageFormat format = new PackStreamMessageFormatV1();
            ChannelPipeline pipeline = ctx.pipeline();

            pipeline.remove( this );

            // inbound handlers
            pipeline.addLast( new ChunkDecoder() );
            pipeline.addLast( new MessageDecoder() );
            pipeline.addLast( new InboundMessageHandler( format, DEV_NULL_LOGGING ) );

            // outbound handlers
            pipeline.addLast( OutboundMessageHandler.NAME, new OutboundMessageHandler( format, DEV_NULL_LOGGING ) );

            handshakeCompletedPromise.setSuccess();

            break;
        case NO_PROTOCOL_VERSION:
            fail( ctx, protocolNoSupportedByServerError() );
            break;
        case HTTP:
            fail( ctx, httpEndpointError() );
            break;
        default:
            fail( ctx, protocolNoSupportedByDriverError( serverSuggestedVersion ) );
            break;
        }
    }

    private void fail( ChannelHandlerContext ctx, Throwable error )
    {
        ctx.close();
        handshakeCompletedPromise.setFailure( error );
    }

    private static Throwable protocolNoSupportedByServerError()
    {
        return new ClientException( "The server does not support any of the protocol versions supported by " +
                                    "this driver. Ensure that you are using driver and server versions that " +
                                    "are compatible with one another." );
    }

    private static Throwable httpEndpointError()
    {
        return new ClientException(
                "Server responded HTTP. Make sure you are not trying to connect to the http endpoint " +
                "(HTTP defaults to port 7474 whereas BOLT defaults to port 7687)" );
    }

    private static Throwable protocolNoSupportedByDriverError( int suggestedProtocolVersion )
    {
        return new ClientException(
                "Protocol error, server suggested unexpected protocol version: " + suggestedProtocolVersion );
    }
}
