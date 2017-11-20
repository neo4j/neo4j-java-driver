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
package org.neo4j.driver.internal.async.inbound;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

import org.neo4j.driver.internal.logging.DelegatingLogger;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Logging;

import static io.netty.buffer.ByteBufUtil.prettyHexDump;

public class MessageDecoder extends ByteToMessageDecoder
{
    private final Logging logging;

    private boolean readMessageBoundary;
    private Logger log;

    public MessageDecoder( Logging logging )
    {
        this.logging = logging;
    }

    @Override
    public void handlerAdded( ChannelHandlerContext ctx )
    {
        log = new DelegatingLogger( ctx.channel().toString(), logging, getClass() );
    }

    @Override
    protected void handlerRemoved0( ChannelHandlerContext ctx ) throws Exception
    {
        readMessageBoundary = false;
        log = null;
    }

    @Override
    public void channelRead( ChannelHandlerContext ctx, Object msg ) throws Exception
    {
        if ( msg instanceof ByteBuf )
        {
            ByteBuf chunkBuf = (ByteBuf) msg;

            // on every read check if input buffer is empty or not
            // if it is empty then it's a message boundary and full message is in the buffer
            readMessageBoundary = chunkBuf.readableBytes() == 0;

            if ( log.isTraceEnabled() )
            {
                if ( readMessageBoundary )
                {
                    log.trace( "Received message boundary" );
                }
                else
                {
                    log.trace( "Received message chunk:\n%s\n", prettyHexDump( chunkBuf ) );
                }
            }
        }
        super.channelRead( ctx, msg );
    }

    @Override
    protected void decode( ChannelHandlerContext ctx, ByteBuf in, List<Object> out )
    {
        if ( readMessageBoundary )
        {
            // now we have a complete message in the input buffer

            // increment ref count of the buffer and create it's duplicate that shares the content
            // duplicate will be the output of this decoded and input for the next one
            ByteBuf messageBuf = in.retainedDuplicate();

            // signal that whole message was read by making input buffer seem like it was fully read/consumed
            in.readerIndex( in.readableBytes() );

            // pass the full message to the next handler in the pipeline
            out.add( messageBuf );

            readMessageBoundary = false;
        }
    }
}
