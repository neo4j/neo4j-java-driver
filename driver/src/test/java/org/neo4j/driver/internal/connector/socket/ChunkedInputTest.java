/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.driver.internal.connector.socket;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;

import org.neo4j.driver.util.RecordingByteChannel;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class ChunkedInputTest
{
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

        ChunkedInput input = new ChunkedInput(ch);

        byte[] outputBuffer = new byte[15];

        // When
        input.hasMoreData();

        // Then
        input.readBytes( outputBuffer, 0, 15 );
        assertThat( outputBuffer, equalTo( new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4, 5} ) );
    }
}