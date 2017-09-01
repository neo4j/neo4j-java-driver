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
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.driver.internal.messaging.FailureMessage;
import org.neo4j.driver.internal.messaging.PackStreamMessageFormatV1;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.neo4j.driver.internal.netty.ProtocolUtil.messageBoundary;

public class InboundMessageDispatcherTest
{
    @Test
    public void encodeAndDecode()
    {
        EmbeddedChannel ch = new EmbeddedChannel();
        // outbound
        ch.pipeline().addLast( new OutboundMessageHandler() );
        // inbound
        ch.pipeline().addLast( new ChunkDecoder(), new MessageDecoder(), new InboundMessageDispatcher() );

        assertTrue( ch.writeOutbound( new FailureMessage( "Hello ", "World!" ) ) );

        ByteBuf read1 = ch.readOutbound();
        assertNotNull( read1 );
        assertEquals( 2, read1.readableBytes() );

        ByteBuf read2 = ch.readOutbound();
        assertNotNull( read2 );
        assertEquals( read1.duplicate().readShort(), read2.readableBytes() );

        ByteBuf read3 = ch.readOutbound();
        assertNotNull( read3 );
        assertEquals( messageBoundary(), read3 );

        ch.writeInbound( read1, read2, read3 );

        // now check the sout
    }

    @Test
    public void decode() throws IOException
    {
        EmbeddedChannel channel =
                new EmbeddedChannel( new ChunkDecoder(), new MessageDecoder(), new InboundMessageDispatcher() );

        ByteBuf buf = Unpooled.buffer();
        ByteBufPackOutput packOutput = new ByteBufPackOutput();
        packOutput.setBuf( buf );

        buf.writeByte( 0x00 );
        buf.writeByte( 0x1E );

        PackStreamMessageFormatV1.Writer writer = new PackStreamMessageFormatV1.Writer( packOutput, new Runnable()
        {
            @Override
            public void run()
            {
            }
        }, true );

        writer.write( new FailureMessage( "Hello ", "World!" ) );

        // message boundary
        buf.writeByte( 0x00 );
        buf.writeByte( 0x00 );

        channel.writeInbound( buf );

        // now check the sout
    }
}
