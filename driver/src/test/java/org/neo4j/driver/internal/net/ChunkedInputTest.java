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
import org.mockito.Matchers;

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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ChunkedInputTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldExposeMultipleChunksAsCohesiveStream() throws Throwable
    {
        // Given
        ReadableByteChannel channel = Channels.newChannel(
                new ByteArrayInputStream( new byte[]{ 0, 5, 1, 2, 3, 4, 5} ) );
        ChunkedInput ch = new ChunkedInput( 2, channel );

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
        ReadableByteChannel channel = Channels.newChannel(
                new ByteArrayInputStream( new byte[]{0, 7, 1, 2, 3, 4, 5, 6, 7} ) );
        ChunkedInput ch = new ChunkedInput( 2, channel );
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

        ChunkedInput input = new ChunkedInput( ch );

        byte[] outputBuffer = new byte[15];

        // When
        input.hasMoreData();

        // Then
        input.readBytes( outputBuffer, 0, 15 );
        assertThat( outputBuffer, equalTo( new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4, 5} ) );
    }

    @Test
    public void canReadAllIntegerSizes() throws Exception
    {
        // Given
        RecordingByteChannel ch = new RecordingByteChannel();
        ChunkedOutput out = new ChunkedOutput( ch );

        // these are written in one go on purpose, to check for buffer pointer errors where writes
        // would interfere with one another, writing at the wrong offsets
        out.writeByte( Byte.MAX_VALUE );
        out.writeByte( (byte)1 );
        out.writeByte( Byte.MIN_VALUE );

        out.writeLong( Long.MAX_VALUE );
        out.writeLong( 0l );
        out.writeLong( Long.MIN_VALUE );

        out.writeShort( Short.MAX_VALUE );
        out.writeShort( (short)0 );
        out.writeShort( Short.MIN_VALUE );

        out.writeInt( Integer.MAX_VALUE );
        out.writeInt( 0 );
        out.writeInt( Integer.MIN_VALUE );

        out.flush();

        ChunkedInput in = new ChunkedInput( ch );

        // when / then
        assertEquals( Byte.MAX_VALUE, in.readByte() );
        assertEquals( (byte)1,        in.readByte() );
        assertEquals( Byte.MIN_VALUE, in.readByte() );

        assertEquals( Long.MAX_VALUE, in.readLong() );
        assertEquals( 0l,             in.readLong() );
        assertEquals( Long.MIN_VALUE, in.readLong() );

        assertEquals( Short.MAX_VALUE, in.readShort() );
        assertEquals( (short)0,        in.readShort() );
        assertEquals( Short.MIN_VALUE, in.readShort() );

        assertEquals( Integer.MAX_VALUE, in.readInt() );
        assertEquals( 0,                 in.readInt() );
        assertEquals( Integer.MIN_VALUE, in.readInt() );
    }

    @Test
    public void shouldNotReadMessageEndingWhenByteLeftInBuffer()
    {
        // Given
        ReadableByteChannel channel = Channels.newChannel(
                new ByteArrayInputStream( new byte[]{ 0, 5, 1, 2, 3, 4, 5, 0, 0} ) );
        ChunkedInput ch = new ChunkedInput( 2, channel );

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
        ReadableByteChannel channel = mock(ReadableByteChannel.class);
        when(channel.read( Matchers.any(ByteBuffer.class) )).thenThrow( new ClosedByInterruptException() );

        ChunkedInput ch = new ChunkedInput( 2, channel );

        // Expect
        exception.expectMessage( "Connection to the database was lost because someone called `interrupt()` on the driver thread waiting for a reply. " +
                                 "This normally happens because the JVM is shutting down, but it can also happen because your application code or some " +
                                 "framework you are using is manually interrupting the thread." );

        // When
        ch.readByte();
    }
}
