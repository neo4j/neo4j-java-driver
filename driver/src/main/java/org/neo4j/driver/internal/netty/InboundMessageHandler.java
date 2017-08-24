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
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.LinkedList;
import java.util.Queue;

import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.PackStreamMessageFormatV1;

public class InboundMessageHandler extends SimpleChannelInboundHandler<ByteBuf>
{
    private final ByteBufPackInput packInput;
    private final MessageFormat.Reader reader;

    private final Queue<ResponseHandler> handlers = new LinkedList<>();
    private final ResponseMessageHandler responseMessageHandler = new ResponseMessageHandler( handlers );

    public InboundMessageHandler()
    {
        this.packInput = new ByteBufPackInput();
        this.reader = new PackStreamMessageFormatV1.Reader( packInput, new Runnable()
        {
            @Override
            public void run()
            {
            }
        } );
    }

    public void addHandler( ResponseHandler handler )
    {
        handlers.add( handler );
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, ByteBuf msg ) throws Exception
    {
        packInput.setBuf( msg );
        reader.read( responseMessageHandler );
    }
}
