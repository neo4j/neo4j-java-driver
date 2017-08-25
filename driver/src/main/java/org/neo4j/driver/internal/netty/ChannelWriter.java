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
import io.netty.channel.ChannelPipeline;

import org.neo4j.driver.internal.messaging.Message;

public final class ChannelWriter
{
    private ChannelWriter()
    {
    }

    public static void write( Channel channel, Message message, ResponseHandler handler, boolean flush )
    {
        ChannelPipeline pipeline = channel.pipeline();

        InboundMessageHandler messageHandler = pipeline.get( InboundMessageHandler.class );
        messageHandler.addHandler( handler );

        if ( flush )
        {
            // todo: add any kind of listener???
            channel.writeAndFlush( message );
        }
        else
        {
            // todo: add any kind of listener???
            channel.write( message );
        }
    }
}
