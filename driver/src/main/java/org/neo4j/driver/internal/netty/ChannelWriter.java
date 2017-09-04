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

import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.spi.ResponseHandler;

import static org.neo4j.driver.internal.netty.ChannelAttributes.address;
import static org.neo4j.driver.internal.netty.ChannelAttributes.creationTimestamp;
import static org.neo4j.driver.internal.netty.ChannelAttributes.responseHandlersHolder;

public final class ChannelWriter
{
    private ChannelWriter()
    {
    }

    public static void write( Channel channel, Message message, ResponseHandler handler, boolean flush )
    {
        try
        {
            ResponseHandlersHolder responseHandlersHolder = responseHandlersHolder( channel );
            responseHandlersHolder.queue( handler );

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
        catch ( Throwable t )
        {
            System.out.println( "-------> Failed to write message: " + message + " because " + t );
            System.out.println( "-------> Channel isOpen=" + channel.isOpen() + " isActive=" + channel.isActive() +
                                " isRegistered=" + channel.isRegistered() + " isWritable=" + channel.isWritable() );
            System.out.println( "-------> Channel address: " + address( channel ) + " creationTimestamp=" +
                                creationTimestamp( channel ) );
            throw t;
        }
    }
}
