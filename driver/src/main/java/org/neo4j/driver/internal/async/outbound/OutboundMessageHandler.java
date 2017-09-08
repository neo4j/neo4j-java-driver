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
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Logging;

import static org.neo4j.driver.internal.async.ProtocolUtil.messageBoundary;

public class OutboundMessageHandler extends MessageToMessageEncoder<Message>
{
    private final OutboundMessageWriter writer;
    private final Logger log;

    public OutboundMessageHandler( Logging logging )
    {
        this( new PackStreamMessageWriter(), logging );
    }

    OutboundMessageHandler( OutboundMessageWriter writer, Logging logging )
    {
        this.writer = writer;
        this.log = logging.getLog( getClass().getSimpleName() );
    }

    @Override
    protected void encode( ChannelHandlerContext ctx, Message msg, List<Object> out ) throws Exception
    {
        log.debug( "Sending message %s", msg );

        ByteBuf messageBody = ctx.alloc().ioBuffer();
        writer.write( msg, messageBody );

        out.add( messageBody );
        out.add( messageBoundary() );
    }
}
