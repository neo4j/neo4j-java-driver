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
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

import static io.netty.buffer.Unpooled.buffer;
import static io.netty.buffer.Unpooled.copyShort;
import static io.netty.buffer.Unpooled.wrappedBuffer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ChunkDecoderTest
{
    @Test
    public void shouldDecodeFullChunk()
    {
        EmbeddedChannel channel = new EmbeddedChannel( new ChunkDecoder() );

        // whole chunk with header and body arrives at once
        ByteBuf input = buffer();
        input.writeShort( 7 );
        input.writeByte( 1 );
        input.writeByte( 11 );
        input.writeByte( 2 );
        input.writeByte( 22 );
        input.writeByte( 3 );
        input.writeByte( 33 );
        input.writeByte( 4 );

        // after buffer is written there should be something to read on the other side
        assertTrue( channel.writeInbound( input ) );
        assertTrue( channel.finish() );

        // there should only be a single chunk available for reading
        assertEquals( 1, channel.inboundMessages().size() );
        // it should have no size header and expected body
        assertEquals( input.slice( 2, 7 ), channel.readInbound() );
    }

    @Test
    public void shouldDecodeSplitChunk()
    {
        EmbeddedChannel channel = new EmbeddedChannel( new ChunkDecoder() );

        // first part of the chunk contains size header and some bytes
        ByteBuf input1 = buffer();
        input1.writeShort( 9 );
        input1.writeByte( 1 );
        input1.writeByte( 11 );
        input1.writeByte( 2 );
        // nothing should be available for reading
        assertFalse( channel.writeInbound( input1 ) );

        // second part contains just a single byte
        ByteBuf input2 = buffer();
        input2.writeByte( 22 );
        // nothing should be available for reading
        assertFalse( channel.writeInbound( input2 ) );

        // third part contains couple more bytes
        ByteBuf input3 = buffer();
        input3.writeByte( 3 );
        input3.writeByte( 33 );
        input3.writeByte( 4 );
        // nothing should be available for reading
        assertFalse( channel.writeInbound( input3 ) );

        // fourth part contains couple more bytes, and the chunk is now complete
        ByteBuf input4 = buffer();
        input4.writeByte( 44 );
        input4.writeByte( 5 );
        // there should be something to read now
        assertTrue( channel.writeInbound( input4 ) );

        assertTrue( channel.finish() );

        // there should only be a single chunk available for reading
        assertEquals( 1, channel.inboundMessages().size() );
        // it should have no size header and expected body
        assertEquals( wrappedBuffer( new byte[]{1, 11, 2, 22, 3, 33, 4, 44, 5} ), channel.readInbound() );
    }

    @Test
    public void shouldDecodeEmptyChunk()
    {
        EmbeddedChannel channel = new EmbeddedChannel( new ChunkDecoder() );

        // chunk contains just the size header which is zero
        ByteBuf input = copyShort( 0 );
        assertTrue( channel.writeInbound( input ) );
        assertTrue( channel.finish() );

        // there should only be a single chunk available for reading
        assertEquals( 1, channel.inboundMessages().size() );
        // it should have no size header and empty body
        assertEquals( wrappedBuffer( new byte[0] ), channel.readInbound() );
    }
}
