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
package org.neo4j.driver.internal.async.outbound;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.util.List;

import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Logging;

import static org.neo4j.driver.internal.async.ProtocolUtil.messageBoundary;

public class OutboundMessageHandler extends MessageToMessageEncoder<Message>
{
    public static final String NAME = OutboundMessageHandler.class.getSimpleName();

    private final MessageFormat messageFormat;
    private final ChunkAwareByteBufOutput output;
    private final MessageFormat.Writer writer;
    private final Logger log;

    public OutboundMessageHandler( MessageFormat messageFormat, Logging logging )
    {
        this( messageFormat, true, logging.getLog( NAME ) );
    }

    private OutboundMessageHandler( MessageFormat messageFormat, boolean byteArraySupportEnabled, Logger log )
    {
        this.messageFormat = messageFormat;
        this.output = new ChunkAwareByteBufOutput();
        this.writer = messageFormat.newWriter( output, byteArraySupportEnabled );
        this.log = log;
    }

    @Override
    protected void encode( ChannelHandlerContext ctx, Message msg, List<Object> out ) throws Exception
    {
        log.debug( "Sending message %s", msg );

        ByteBuf messageBuf = ctx.alloc().ioBuffer();
        output.start( messageBuf );
        writer.write( msg );
        output.stop();

        out.add( messageBuf );
        out.add( messageBoundary() );
    }

    public OutboundMessageHandler withoutByteArraySupport()
    {
        return new OutboundMessageHandler( messageFormat, false, log );
    }
}
