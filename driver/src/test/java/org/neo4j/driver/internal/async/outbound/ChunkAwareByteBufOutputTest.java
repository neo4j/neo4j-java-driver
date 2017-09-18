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
import io.netty.buffer.Unpooled;
import org.junit.Test;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.neo4j.driver.v1.util.TestUtil.assertByteBufContains;

public class ChunkAwareByteBufOutputTest
{
    @Test
    public void shouldThrowForIllegalMaxChunkSize()
    {
        try
        {
            new ChunkAwareByteBufOutput( -42 );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalArgumentException.class ) );
        }
    }

    @Test
    public void shouldThrowWhenStartedWithNullBuf()
    {
        ChunkAwareByteBufOutput output = new ChunkAwareByteBufOutput( 16 );

        try
        {
            output.start( null );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( NullPointerException.class ) );
        }
    }

    @Test
    public void shouldThrowWhenStartedTwice()
    {
        ChunkAwareByteBufOutput output = new ChunkAwareByteBufOutput( 16 );
        output.start( mock( ByteBuf.class ) );

        try
        {
            output.start( mock( ByteBuf.class ) );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void shouldWriteByteAtTheBeginningOfChunk()
    {
        ChunkAwareByteBufOutput output = new ChunkAwareByteBufOutput( 16 );
        ByteBuf buf = Unpooled.buffer();

        output.start( buf );
        output.writeByte( (byte) 42 );
        output.stop();

        assertByteBufContains( buf, (short) 1, (byte) 42 );
    }

    @Test
    public void shouldWriteByteWhenCurrentChunkContainsSpace()
    {
        ChunkAwareByteBufOutput output = new ChunkAwareByteBufOutput( 16 );
        ByteBuf buf = Unpooled.buffer();

        output.start( buf );
        output.writeByte( (byte) 1 );
        output.writeByte( (byte) 2 );
        output.writeByte( (byte) -24 );

        output.writeByte( (byte) 42 );
        output.stop();

        assertByteBufContains( buf, (short) 4, (byte) 1, (byte) 2, (byte) -24, (byte) 42 );
    }

    @Test
    public void shouldWriteByteWhenCurrentChunkIsFull()
    {
        ChunkAwareByteBufOutput output = new ChunkAwareByteBufOutput( 5 );
        ByteBuf buf = Unpooled.buffer();

        output.start( buf );
        output.writeByte( (byte) 5 );
        output.writeByte( (byte) 3 );
        output.writeByte( (byte) -5 );

        output.writeByte( (byte) 42 );
        output.stop();

        assertByteBufContains( buf,
                (short) 3, (byte) 5, (byte) 3, (byte) -5, // chunk 1
                (short) 1, (byte) 42 // chunk 2
        );
    }

    @Test
    public void shouldWriteShortAtTheBeginningOfChunk()
    {
        ChunkAwareByteBufOutput output = new ChunkAwareByteBufOutput( 10 );
        ByteBuf buf = Unpooled.buffer();

        output.start( buf );
        output.writeShort( Short.MAX_VALUE );
        output.stop();

        assertByteBufContains( buf, (short) 2, Short.MAX_VALUE );
    }

    @Test
    public void shouldWriteShortWhenCurrentChunkContainsSpace()
    {
        ChunkAwareByteBufOutput output = new ChunkAwareByteBufOutput( 12 );
        ByteBuf buf = Unpooled.buffer();

        output.start( buf );
        output.writeShort( (short) 1 );
        output.writeShort( (short) 42 );
        output.writeShort( (short) 4242 );
        output.writeShort( (short) 4242 );

        output.writeShort( (short) -30 );
        output.stop();

        assertByteBufContains( buf, (short) 10, (short) 1, (short) 42, (short) 4242, (short) 4242, (short) -30 );
    }

    @Test
    public void shouldWriteShortWhenCurrentChunkIsFull()
    {
        ChunkAwareByteBufOutput output = new ChunkAwareByteBufOutput( 8 );
        ByteBuf buf = Unpooled.buffer();

        output.start( buf );
        output.writeShort( (short) 14 );
        output.writeShort( (short) -99 );
        output.writeShort( (short) 202 );

        output.writeShort( Short.MIN_VALUE );
        output.stop();

        assertByteBufContains( buf,
                (short) 6, (short) 14, (short) -99, (short) 202, // chunk 1
                (short) 2, Short.MIN_VALUE // chunk 2
        );
    }

    @Test
    public void shouldWriteIntAtTheBeginningOfChunk()
    {
        ChunkAwareByteBufOutput output = new ChunkAwareByteBufOutput( 18 );
        ByteBuf buf = Unpooled.buffer();

        output.start( buf );
        output.writeInt( 73649 );
        output.stop();

        assertByteBufContains( buf, (short) 4, 73649 );
    }

    @Test
    public void shouldWriteIntWhenCurrentChunkContainsSpace()
    {
        ChunkAwareByteBufOutput output = new ChunkAwareByteBufOutput( 40 );
        ByteBuf buf = Unpooled.buffer();

        output.start( buf );
        output.writeInt( Integer.MAX_VALUE );
        output.writeInt( 20 );
        output.writeInt( -173 );

        output.writeInt( Integer.MIN_VALUE );
        output.stop();

        assertByteBufContains( buf, (short) 16, Integer.MAX_VALUE, 20, -173, Integer.MIN_VALUE );
    }

    @Test
    public void shouldWriteIntWhenCurrentChunkIsFull()
    {
        ChunkAwareByteBufOutput output = new ChunkAwareByteBufOutput( 27 );
        ByteBuf buf = Unpooled.buffer();

        output.start( buf );
        output.writeInt( 42 );
        output.writeInt( -73467193 );
        output.writeInt( 373 );
        output.writeInt( -93 );
        output.writeInt( 1312345 );
        output.writeInt( 785 );

        output.writeInt( 42 );
        output.stop();

        assertByteBufContains( buf,
                (short) 24, 42, -73467193, 373, -93, 1312345, 785, // chunk 1
                (short) 4, 42 // chunk 2
        );
    }

    @Test
    public void shouldWriteLongAtTheBeginningOfChunk()
    {
        ChunkAwareByteBufOutput output = new ChunkAwareByteBufOutput( 12 );
        ByteBuf buf = Unpooled.buffer();

        output.start( buf );
        output.writeLong( 15 );
        output.stop();

        assertByteBufContains( buf, (short) 8, 15L );
    }

    @Test
    public void shouldWriteLongWhenCurrentChunkContainsSpace()
    {
        ChunkAwareByteBufOutput output = new ChunkAwareByteBufOutput( 34 );
        ByteBuf buf = Unpooled.buffer();

        output.start( buf );
        output.writeLong( Long.MAX_VALUE );
        output.writeLong( -1 );
        output.writeLong( -100 );

        output.writeLong( Long.MIN_VALUE / 2 );
        output.stop();

        assertByteBufContains( buf, (short) 32, Long.MAX_VALUE, -1L, -100L, Long.MIN_VALUE / 2 );
    }

    @Test
    public void shouldWriteLongWhenCurrentChunkIsFull()
    {
        ChunkAwareByteBufOutput output = new ChunkAwareByteBufOutput( 38 );
        ByteBuf buf = Unpooled.buffer();

        output.start( buf );
        output.writeLong( 12 );
        output.writeLong( 8741 );
        output.writeLong( 2314 );
        output.writeLong( -85793 );

        output.writeLong( -57999999 );
        output.stop();

        assertByteBufContains( buf,
                (short) 32, 12L, 8741L, 2314L, -85793L, // chunk 1
                (short) 8, -57999999L // chunk 2
        );
    }

    @Test
    public void shouldWriteDoubleAtTheBeginningOfChunk()
    {
        ChunkAwareByteBufOutput output = new ChunkAwareByteBufOutput( 10 );
        ByteBuf buf = Unpooled.buffer();

        output.start( buf );
        output.writeDouble( 12.99937 );
        output.stop();

        assertByteBufContains( buf, (short) 8, 12.99937D );
    }

    @Test
    public void shouldWriteDoubleWhenCurrentChunkContainsSpace()
    {
        ChunkAwareByteBufOutput output = new ChunkAwareByteBufOutput( 18 );
        ByteBuf buf = Unpooled.buffer();

        output.start( buf );
        output.writeDouble( -5 );

        output.writeDouble( 991.3333 );
        output.stop();

        assertByteBufContains( buf, (short) 16, -5D, 991.3333D );
    }

    @Test
    public void shouldWriteDoubleWhenCurrentChunkIsFull()
    {
        ChunkAwareByteBufOutput output = new ChunkAwareByteBufOutput( 20 );
        ByteBuf buf = Unpooled.buffer();

        output.start( buf );
        output.writeDouble( 1839 );
        output.writeDouble( 5710923.34873 );

        output.writeDouble( -47389.333399 );
        output.stop();

        assertByteBufContains( buf,
                (short) 16, 1839D, 5710923.34873D, // chunk 1
                (short) 8, -47389.333399D // chunk 2
        );
    }

    @Test
    public void shouldWriteBytesAtTheBeginningOfChunk()
    {
        ChunkAwareByteBufOutput output = new ChunkAwareByteBufOutput( 10 );
        ByteBuf buf = Unpooled.buffer();

        output.start( buf );
        output.writeBytes( new byte[]{1, 2, 3, -1, -2, -3, 127} );
        output.stop();

        assertByteBufContains( buf,
                (short) 7, (byte) 1, (byte) 2, (byte) 3, (byte) -1, (byte) -2, (byte) -3, (byte) 127 );
    }

    @Test
    public void shouldWriteBytesWhenCurrentChunkContainsSpace()
    {
        ChunkAwareByteBufOutput output = new ChunkAwareByteBufOutput( 13 );
        ByteBuf buf = Unpooled.buffer();

        output.start( buf );
        output.writeBytes( new byte[]{9, 8, -10} );
        output.writeBytes( new byte[]{127, 126, -128, -126} );
        output.writeBytes( new byte[]{0, 99} );

        output.writeBytes( new byte[]{-42, 42} );
        output.stop();

        assertByteBufContains( buf, (short) 11, (byte) 9, (byte) 8, (byte) -10, (byte) 127, (byte) 126, (byte) -128,
                (byte) -126, (byte) 0, (byte) 99, (byte) -42, (byte) 42 );
    }

    @Test
    public void shouldWriteBytesWhenCurrentChunkIsFull()
    {
        ChunkAwareByteBufOutput output = new ChunkAwareByteBufOutput( 9 );
        ByteBuf buf = Unpooled.buffer();

        output.start( buf );
        output.writeBytes( new byte[]{1, 2} );
        output.writeBytes( new byte[]{3, 4, 5} );
        output.writeBytes( new byte[]{10} );

        output.writeBytes( new byte[]{-1, -42, -43} );
        output.stop();

        assertByteBufContains( buf,
                (short) 7, (byte) 1, (byte) 2, (byte) 3, (byte) 4, (byte) 5, (byte) 10, (byte) -1, // chunk 1
                (short) 2, (byte) -42, (byte) -43 // chunk 2
        );
    }

    @Test
    public void shouldWriteBytesThatSpanMultipleChunks()
    {
        ChunkAwareByteBufOutput output = new ChunkAwareByteBufOutput( 7 );
        ByteBuf buf = Unpooled.buffer();

        output.start( buf );
        output.writeBytes( new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18} );
        output.stop();

        assertByteBufContains( buf,
                (short) 5, (byte) 1, (byte) 2, (byte) 3, (byte) 4, (byte) 5, // chunk 1
                (short) 5, (byte) 6, (byte) 7, (byte) 8, (byte) 9, (byte) 10, // chunk 2
                (short) 5, (byte) 11, (byte) 12, (byte) 13, (byte) 14, (byte) 15, // chunk 3
                (short) 3, (byte) 16, (byte) 17, (byte) 18 // chunk 4
        );
    }

    @Test
    public void shouldWriteDataToMultipleChunks()
    {
        ChunkAwareByteBufOutput output = new ChunkAwareByteBufOutput( 13 );
        ByteBuf buf = Unpooled.buffer();

        output.start( buf );
        output.writeDouble( 12.3 );
        output.writeByte( (byte) 42 );
        output.writeInt( -10 );
        output.writeInt( 99 );
        output.writeLong( 99 );
        output.writeBytes( new byte[]{9, 8, 7, 6} );
        output.writeDouble( 0.333 );
        output.writeShort( (short) 0 );
        output.writeShort( (short) 1 );
        output.writeInt( 12345 );
        output.writeBytes( new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10} );
        output.stop();

        assertByteBufContains( buf,
                (short) 9, 12.3D, (byte) 42, // chunk 1
                (short) 8, -10, 99, // chunk 2
                (short) 11, 99L, (byte) 9, (byte) 8, (byte) 7, // chunk 3
                (short) 11, (byte) 6, 0.333D, (short) 0, // chunk 4
                (short) 11, (short) 1, 12345, (byte) 1, (byte) 2, (byte) 3, (byte) 4, (byte) 5, // chunk 5
                (short) 5, (byte) 6, (byte) 7, (byte) 8, (byte) 9, (byte) 10 // chunk 6
        );
    }
}
