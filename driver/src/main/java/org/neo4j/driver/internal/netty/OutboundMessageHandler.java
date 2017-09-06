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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.util.List;

import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.PackStreamMessageFormatV1;

import static org.neo4j.driver.internal.netty.ProtocolUtil.messageBoundary;

public class OutboundMessageHandler extends MessageToMessageEncoder<Message>
{
    private static final int MAX_CHUNK_SIZE = 8192;

    private final ByteBufPackOutput packOutput;
    private final MessageFormat.Writer writer;

    public OutboundMessageHandler()
    {
        this.packOutput = new ByteBufPackOutput();
        this.writer = new PackStreamMessageFormatV1.Writer( packOutput, new Runnable()
        {
            @Override
            public void run()
            {
            }
        }, true );
    }

    @Override
    protected void encode( ChannelHandlerContext ctx, Message msg, List<Object> out ) throws Exception
    {
        System.out.println( "Sending " + msg );
        ByteBuf bodyBuf = ctx.alloc().ioBuffer();
        packOutput.setBuf( bodyBuf );

        writer.write( msg );
        int bytesWritten = bodyBuf.writerIndex();

        if ( bytesWritten <= MAX_CHUNK_SIZE )
        {
            ByteBuf headerBuf = ctx.alloc().ioBuffer( 2 );
            headerBuf.writeShort( bytesWritten );

            out.add( headerBuf );
            out.add( bodyBuf );
            out.add( messageBoundary() );
        }
        else
        {
            throw new UnsupportedOperationException(); // todo
        }
    }
}
