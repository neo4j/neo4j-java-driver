/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;

import static io.netty.buffer.ByteBufUtil.hexDump;
import static io.netty.buffer.Unpooled.buffer;
import static io.netty.buffer.Unpooled.copyShort;
import static io.netty.buffer.Unpooled.wrappedBuffer;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.util.TestUtil.assertByteBufEquals;

class ChunkDecoderTest
{
    private ByteBuf buffer;
    private EmbeddedChannel channel = new EmbeddedChannel( newChunkDecoder() );

    @AfterEach
    void tearDown()
    {
        if ( buffer != null )
        {
            buffer.release( buffer.refCnt() );
        }
        if ( channel != null )
        {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void shouldDecodeFullChunk()
    {
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
        assertByteBufEquals( input.slice( 2, 7 ), channel.readInbound() );
    }

    @Test
    void shouldDecodeSplitChunk()
    {
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
        assertByteBufEquals( wrappedBuffer( new byte[]{1, 11, 2, 22, 3, 33, 4, 44, 5} ), channel.readInbound() );
    }

    @Test
    void shouldDecodeEmptyChunk()
    {
        // chunk contains just the size header which is zero
        ByteBuf input = copyShort( 0 );
        assertTrue( channel.writeInbound( input ) );
        assertTrue( channel.finish() );

        // there should only be a single chunk available for reading
        assertEquals( 1, channel.inboundMessages().size() );
        // it should have no size header and empty body
        assertByteBufEquals( wrappedBuffer( new byte[0] ), channel.readInbound() );
    }

    @Test
    void shouldLogEmptyChunkOnTraceLevel()
    {
        Logger logger = newTraceLogger();
        channel = new EmbeddedChannel( new ChunkDecoder( newLogging( logger ) ) );

        buffer = copyShort( 0 );
        assertTrue( channel.writeInbound( buffer.copy() ) ); // copy buffer so we can verify against it later
        assertTrue( channel.finish() );

        ArgumentCaptor<String> messageCaptor = ArgumentCaptor.forClass( String.class );
        verify( logger ).trace( anyString(), messageCaptor.capture() );

        // pretty hex dump should be logged
        assertEquals( hexDump( buffer ), messageCaptor.getValue() );
        // single empty chunk should be available for reading
        assertEquals( 1, channel.inboundMessages().size() );
        assertByteBufEquals( wrappedBuffer( new byte[0] ), channel.readInbound() );
    }

    @Test
    void shouldLogNonEmptyChunkOnTraceLevel()
    {
        Logger logger = newTraceLogger();
        channel = new EmbeddedChannel( new ChunkDecoder( newLogging( logger ) ) );

        byte[] bytes = "Hello".getBytes();
        buffer = buffer();
        buffer.writeShort( bytes.length );
        buffer.writeBytes( bytes );

        assertTrue( channel.writeInbound( buffer.copy() ) ); // copy buffer so we can verify against it later
        assertTrue( channel.finish() );

        ArgumentCaptor<String> messageCaptor = ArgumentCaptor.forClass( String.class );
        verify( logger ).trace( anyString(), messageCaptor.capture() );

        // pretty hex dump should be logged
        assertEquals( hexDump( buffer ), messageCaptor.getValue() );
        // single chunk should be available for reading
        assertEquals( 1, channel.inboundMessages().size() );
        assertByteBufEquals( wrappedBuffer( bytes ), channel.readInbound() );
    }

    @Test
    public void shouldDecodeMaxSizeChunk()
    {
        byte[] message = new byte[0xFFFF];

        ByteBuf input = buffer();
        input.writeShort( message.length ); // chunk header
        input.writeBytes( message ); // chunk body

        assertTrue( channel.writeInbound( input ) );
        assertTrue( channel.finish() );

        assertEquals( 1, channel.inboundMessages().size() );
        assertByteBufEquals( wrappedBuffer( message ), channel.readInbound() );
    }

    private static ChunkDecoder newChunkDecoder()
    {
        return new ChunkDecoder( DEV_NULL_LOGGING );
    }

    private static Logger newTraceLogger()
    {
        Logger logger = mock( Logger.class );
        when( logger.isTraceEnabled() ).thenReturn( true );
        return logger;
    }

    private static Logging newLogging( Logger logger )
    {
        Logging logging = mock( Logging.class );
        when( logging.getLog( anyString() ) ).thenReturn( logger );
        return logging;
    }
}
