/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.driver.internal.net;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;

import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.util.RecordingByteChannel;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BufferingChunkedInputTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldReadOneByteInOneChunk() throws IOException
    {
        // Given
        BufferingChunkedInput input = new BufferingChunkedInput( packet( 0, 2, 13, 37, 0, 0 ) );

        // When
        byte b1 = input.readByte();
        byte b2 = input.readByte();

        // Then
        assertThat( b1, equalTo( (byte) 13 ) );
        assertThat( b2, equalTo( (byte) 37 ) );
    }

    @Test
    public void shouldReadOneByteInTwoChunks() throws IOException
    {
        // Given
        BufferingChunkedInput input = new BufferingChunkedInput( packet( 0, 1, 13, 0, 1, 37, 0, 0 ) );

        // When
        byte b1 = input.readByte();
        byte b2 = input.readByte();

        // Then
        assertThat( b1, equalTo( (byte) 13 ) );
        assertThat( b2, equalTo( (byte) 37 ) );
    }

    @Test
    public void shouldReadOneByteWhenSplitHeader() throws IOException
    {
        // Given
        BufferingChunkedInput input =
                new BufferingChunkedInput( packets( packet( 0 ), packet( 1, 13, 0, 1, 37, 0, 0 ) ) );

        // When
        byte b1 = input.readByte();
        byte b2 = input.readByte();

        // Then
        assertThat( b1, equalTo( (byte) 13 ) );
        assertThat( b2, equalTo( (byte) 37 ) );
    }

    @Test
    public void shouldReadBytesAcrossHeaders() throws IOException
    {
        // Given
        BufferingChunkedInput input =
                new BufferingChunkedInput( packets( packet( 0, 2, 1, 2, 0, 6), packet(3, 4, 5, 6, 7, 8, 0, 0 ) ) );

        // When
        byte[] dst = new byte[8];
        input.readBytes(dst, 0, 8);

        // Then
        assertThat( dst, equalTo( new byte[]{1, 2, 3, 4, 5, 6, 7, 8} ) );
    }

    @Test
    public void shouldReadChunkWithSplitHeaderForBigMessages() throws IOException
    {
        // Given
        int packetSize = 384;
        BufferingChunkedInput input =
                new BufferingChunkedInput( packets( packet( 1 ), packet( -128 ), fillPacket( packetSize, 1 ) ) );

        // Then
        assertThat( input.readByte(), equalTo( (byte) 1 ) );
        assertThat( input.remainingChunkSize(), equalTo( packetSize - 1 ) );

        for ( int i = 1; i < packetSize; i++ )
        {
            assertThat( input.readByte(), equalTo( (byte) 1 ) );
        }
        assertThat( input.remainingChunkSize(), equalTo( 0 ) );
    }

    @Test
    public void shouldReadChunkWithSplitHeaderForBigMessagesWhenInternalBufferHasOneByte() throws IOException
    {
        // Given
        int packetSize = 32780;
        BufferingChunkedInput input =
                new BufferingChunkedInput( packets( packet( -128 ), packet( 12 ), fillPacket( packetSize, 1 ) ), 1);

        // Then
        assertThat( input.readByte(), equalTo( (byte) 1 ) );
        assertThat( input.remainingChunkSize(), equalTo( packetSize - 1 ) );
    }

    @Test
    public void shouldReadUnsignedByteFromBuffer() throws IOException
    {
        ByteBuffer buffer = ByteBuffer.allocate( 1 );
        buffer.put( (byte) -1 );
        buffer.flip();
        assertThat(BufferingChunkedInput.getUnsignedByteFromBuffer( buffer ), equalTo( 255 ));
    }

    @Test
    public void shouldReadOneByteInOneChunkWhenBustingBuffer() throws IOException
    {
        // Given
        BufferingChunkedInput input = new BufferingChunkedInput( packet( 0, 2, 13, 37, 0, 0 ), 2 );

        // When
        byte b1 = input.readByte();
        byte b2 = input.readByte();

        // Then
        assertThat( b1, equalTo( (byte) 13 ) );
        assertThat( b2, equalTo( (byte) 37 ) );
    }

    @Test
    public void shouldExposeMultipleChunksAsCohesiveStream() throws Throwable
    {
        // Given
        BufferingChunkedInput ch = new BufferingChunkedInput( packet( 0, 5, 1, 2, 3, 4, 5 ), 2 );

        // When
        byte[] bytes = new byte[5];
        ch.readBytes( bytes, 0, 5 );

        // Then
        assertThat( bytes, equalTo( new byte[]{1, 2, 3, 4, 5} ) );
    }

    @Test
    public void shouldReadIntoMisalignedDestinationBuffer() throws Throwable
    {
        // Given
        BufferingChunkedInput ch = new BufferingChunkedInput( packet( 0, 7, 1, 2, 3, 4, 5, 6, 7 ), 2 );
        byte[] bytes = new byte[3];

        // When I read {1,2,3}
        ch.readBytes( bytes, 0, 3 );

        // Then
        assertThat( bytes, equalTo( new byte[]{1, 2, 3} ) );


        // When I read {4,5,6}
        ch.readBytes( bytes, 0, 3 );

        // Then
        assertThat( bytes, equalTo( new byte[]{4, 5, 6} ) );


        // When I read {7}
        Arrays.fill( bytes, (byte) 0 );
        ch.readBytes( bytes, 0, 1 );

        // Then
        assertThat( bytes, equalTo( new byte[]{7, 0, 0} ) );
    }

    @Test
    public void canReadBytesAcrossChunkBoundaries() throws Exception
    {
        // Given
        byte[] inputBuffer = {
                0, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,   // chunk 1 with size 10
                0, 5, 1, 2, 3, 4, 5                     // chunk 2 with size 5
        };
        RecordingByteChannel ch = new RecordingByteChannel();
        ch.write( ByteBuffer.wrap( inputBuffer ) );

        BufferingChunkedInput input = new BufferingChunkedInput( ch );

        byte[] outputBuffer = new byte[15];

        // When
        input.hasMoreData();

        // Then
        input.readBytes( outputBuffer, 0, 15 );
        assertThat( outputBuffer, equalTo( new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4, 5} ) );
    }

    @Test
    public void canReadBytesAcrossChunkBoundariesWithMisalignedBuffer() throws Exception
    {
        // Given
        byte[] inputBuffer = {
                0, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,   // chunk 1 with size 10
                0, 5, 1, 2, 3, 4, 5                     // chunk 2 with size 5
        };
        RecordingByteChannel ch = new RecordingByteChannel();
        ch.write( ByteBuffer.wrap( inputBuffer ) );

        BufferingChunkedInput input = new BufferingChunkedInput( ch, 11 );

        byte[] outputBuffer = new byte[15];

        // When
        input.hasMoreData();

        // Then
        input.readBytes( outputBuffer, 0, 15 );
        assertThat( outputBuffer, equalTo( new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4, 5} ) );
    }

    @Test
    public void canReadAllNumberSizes() throws Exception
    {
        // Given
        RecordingByteChannel ch = new RecordingByteChannel();
        ChunkedOutput out = new ChunkedOutput( ch );

        // these are written in one go on purpose, to check for buffer pointer errors where writes
        // would interfere with one another, writing at the wrong offsets
        out.writeByte( Byte.MAX_VALUE );
        out.writeByte( (byte) 1 );
        out.writeByte( Byte.MIN_VALUE );

        out.writeLong( Long.MAX_VALUE );
        out.writeLong( 0L );
        out.writeLong( Long.MIN_VALUE );

        out.writeShort( Short.MAX_VALUE );
        out.writeShort( (short) 0 );
        out.writeShort( Short.MIN_VALUE );

        out.writeInt( Integer.MAX_VALUE );
        out.writeInt( 0 );
        out.writeInt( Integer.MIN_VALUE );

        out.writeDouble( Double.MAX_VALUE );
        out.writeDouble( 0d );
        out.writeDouble( Double.MIN_VALUE );

        out.flush();

        BufferingChunkedInput in = new BufferingChunkedInput( ch );

        // when / then
        assertEquals( Byte.MAX_VALUE, in.readByte() );
        assertEquals( (byte) 1, in.readByte() );
        assertEquals( Byte.MIN_VALUE, in.readByte() );

        assertEquals( Long.MAX_VALUE, in.readLong() );
        assertEquals( 0L, in.readLong() );
        assertEquals( Long.MIN_VALUE, in.readLong() );

        assertEquals( Short.MAX_VALUE, in.readShort() );
        assertEquals( (short) 0, in.readShort() );
        assertEquals( Short.MIN_VALUE, in.readShort() );

        assertEquals( Integer.MAX_VALUE, in.readInt() );
        assertEquals( 0, in.readInt() );
        assertEquals( Integer.MIN_VALUE, in.readInt() );

        assertEquals( Double.MAX_VALUE, in.readDouble(), 0d );
        assertEquals( 0D, in.readDouble(), 0d );
        assertEquals( Double.MIN_VALUE, in.readDouble(), 0d );
    }

    @Test
    public void shouldNotReadMessageEndingWhenByteLeftInBuffer() throws IOException
    {
        // Given
        ReadableByteChannel channel = Channels.newChannel(
                new ByteArrayInputStream( new byte[]{0, 5, 1, 2, 3, 4, 5, 0, 0} ) );
        BufferingChunkedInput ch = new BufferingChunkedInput( channel, 2 );

        byte[] bytes = new byte[4];
        ch.readBytes( bytes, 0, 4 );
        assertThat( bytes, equalTo( new byte[]{1, 2, 3, 4} ) );

        // When
        try
        {
            ch.messageBoundaryHook().run();
            fail( "The expected ClientException is not thrown" );
        }
        catch ( ClientException e )
        {
            assertEquals( "org.neo4j.driver.v1.exceptions.ClientException: Trying to read message complete ending " +
                          "'00 00' while there are more data left in the message content unread: buffer [], " +
                          "unread chunk size 1", e.toString() );
        }
    }

    @Test
    public void shouldGiveHelpfulMessageOnInterrupt() throws IOException
    {
        // Given
        ReadableByteChannel channel = mock( ReadableByteChannel.class );
        when( channel.read( any( ByteBuffer.class ) ) ).thenThrow( new ClosedByInterruptException() );

        BufferingChunkedInput ch = new BufferingChunkedInput( channel, 2 );

        // Expect
        exception.expectMessage(
                "Connection to the database was lost because someone called `interrupt()` on the driver thread " +
                "waiting for a reply. " +
                "This normally happens because the JVM is shutting down, but it can also happen because your " +
                "application code or some " +
                "framework you are using is manually interrupting the thread." );

        // When
        ch.readByte();
    }

    @Test
    public void shouldPeekOneByteInOneChunk() throws IOException
    {
        // Given
        BufferingChunkedInput input = new BufferingChunkedInput( packet( 0, 2, 13, 37, 0, 0 ) );

        // When
        byte peeked1 = input.peekByte();
        byte read1 = input.readByte();
        byte peeked2 = input.peekByte();
        byte read2 = input.readByte();

        // Then
        assertThat( peeked1, equalTo( (byte) 13 ) );
        assertThat( read1, equalTo( (byte) 13 ) );
        assertThat( peeked2, equalTo( (byte) 37 ) );
        assertThat( read2, equalTo( (byte) 37 ) );
    }

    @Test
    public void shouldPeekOneByteInTwoChunks() throws IOException
    {
        // Given
        BufferingChunkedInput input = new BufferingChunkedInput( packet( 0, 1, 13, 0, 1, 37, 0, 0 ) );

        // When
        byte peeked1 = input.peekByte();
        byte read1 = input.readByte();
        byte peeked2 = input.peekByte();
        byte read2 = input.readByte();

        // Then
        assertThat( peeked1, equalTo( (byte) 13 ) );
        assertThat( read1, equalTo( (byte) 13 ) );
        assertThat( peeked2, equalTo( (byte) 37 ) );
        assertThat( read2, equalTo( (byte) 37 ) );
    }

    @Test
    public void shouldPeekOneByteWhenSplitHeader() throws IOException
    {
        // Given
        BufferingChunkedInput input =
                new BufferingChunkedInput( packets( packet( 0 ), packet( 1, 13, 0, 1, 37, 0, 0 ) ) );

        // When
        byte peeked1 = input.peekByte();
        byte read1 = input.readByte();
        byte peeked2 = input.peekByte();
        byte read2 = input.readByte();

        // Then
        assertThat( peeked1, equalTo( (byte) 13 ) );
        assertThat( read1, equalTo( (byte) 13 ) );
        assertThat( peeked2, equalTo( (byte) 37 ) );
        assertThat( read2, equalTo( (byte) 37 ) );
    }

    @Test
    public void shouldPeekOneByteInOneChunkWhenBustingBuffer() throws IOException
    {
        // Given
        BufferingChunkedInput input = new BufferingChunkedInput( packet( 0, 2, 13, 37, 0, 0 ), 2 );

        // When
        byte peeked1 = input.peekByte();
        byte read1 = input.readByte();
        byte peeked2 = input.peekByte();
        byte read2 = input.readByte();

        // Then
        assertThat( peeked1, equalTo( (byte) 13 ) );
        assertThat( read1, equalTo( (byte) 13 ) );
        assertThat( peeked2, equalTo( (byte) 37 ) );
        assertThat( read2, equalTo( (byte) 37 ) );
    }

    @Test
    public void shouldNotStackOverflowWhenDataIsNotAvailable() throws IOException
    {
        // Given a channel that does not get data from the channel
        ReadableByteChannel channel = new ReadableByteChannel()
        {
            private int counter = 0;
            private int numberOfTries = 10000;

            @Override
            public int read( ByteBuffer dst ) throws IOException
            {
                if ( counter++ < numberOfTries )
                {
                    return 0;
                }
                else
                {
                    dst.put( (byte) 11 );
                    return 1;
                }
            }

            @Override
            public boolean isOpen()
            {
                return true;
            }

            @Override
            public void close() throws IOException
            {

            }
        };

        // When
        BufferingChunkedInput input = new BufferingChunkedInput( channel );

        // Then
        assertThat( input.readByte(), equalTo( (byte) 11 ) );

    }

    @Test
    public void shouldFailNicelyOnClosedConnections() throws IOException
    {
        // Given
        ReadableByteChannel channel = mock( ReadableByteChannel.class );
        when( channel.read( any( ByteBuffer.class ) ) ).thenReturn( -1 );
        BufferingChunkedInput input = new BufferingChunkedInput( channel );

        //Expect
        exception.expect( ClientException.class );
        exception.expectMessage( "Connection terminated while receiving data. This can happen due to network " +
                                 "instabilities, or due to restarts of the database." );
        // When
        input.readByte();
    }

    private ReadableByteChannel fillPacket( int size, int value )
    {
        int[] ints = new int[size];
        for ( int i = 0; i < size; i++ )
        {
            ints[i] = value;
        }

        return packet( ints );
    }

    private ReadableByteChannel packet( int... bytes )
    {
        byte[] byteArray = new byte[bytes.length];
        for ( int i = 0; i < bytes.length; i++ )
        {
            byteArray[i] = (byte) bytes[i];
        }

        return Channels.newChannel(
                new ByteArrayInputStream( byteArray ) );
    }

    private ReadableByteChannel packets( final ReadableByteChannel... channels )
    {

        return new ReadableByteChannel()
        {
            private int index = 0;

            @Override
            public int read( ByteBuffer dst ) throws IOException
            {
                return channels[index++].read( dst );
            }

            @Override
            public boolean isOpen()
            {
                return false;
            }

            @Override
            public void close() throws IOException
            {

            }
        };
    }

}