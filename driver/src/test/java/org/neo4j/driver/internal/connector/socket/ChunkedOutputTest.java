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

import org.neo4j.driver.internal.util.BytePrinter;
import org.neo4j.driver.util.RecordingByteChannel;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class ChunkedOutputTest
{
    private final RecordingByteChannel channel = new RecordingByteChannel();
    private final ChunkedOutput out = new ChunkedOutput( 16, channel );

    @Test
    public void shouldChunkSingleMessage() throws Throwable
    {
        // When
        out.writeByte( (byte) 1 ).writeShort( (short) 2 );
        out.messageBoundaryHook().run();
        out.flush();

        // Then
        assertThat( BytePrinter.hex( channel.getBytes() ),
                equalTo( "00 03 01 00 02 00 00 " ) );
    }

    @Test
    public void shouldChunkMessageSpanningMultipleChunks() throws Throwable
    {
        // When
        out.writeLong( 1 )
           .writeLong( 2 )
           .writeLong( 3 );
        out.messageBoundaryHook().run();
        out.flush();

        // Then
        assertThat( BytePrinter.hex( channel.getBytes() ),
                equalTo( "00 08 00 00 00 00 00 00    00 01 00 08 00 00 00 00    " +
                         "00 00 00 02 00 08 00 00    00 00 00 00 00 03 00 00\n" ) );
    }

    @Test
    public void shouldReserveSpaceForChunkHeaderWhenWriteDataToNewChunk() throws Throwable
    {
        // Given 2 bytes left in buffer + chunk is closed
        out.writeBytes( new byte[10], 0, 10 );  // 2 (header) + 10
        out.messageBoundaryHook().run();        // 2 (ending)

        // When write 2 bytes
        out.writeShort( (short) 33 );           // 2 (header) + 2

        // Then the buffer should auto flash if space left (2) is smaller than new data and chunk header (2 + 2)
        assertThat( BytePrinter.hex( channel.getBytes() ),
                equalTo( "00 0a 00 00 00 00 00 00    00 00 00 00 00 00 " ) );
    }

    @Test
    public void shouldSendOutDataWhoseSizeIsGreaterThanOutputBufferCapacity() throws Throwable
    {
        out.writeBytes( new byte[16], 0, 16 );  // 2 + 16 is greater than the default max size 16
        out.messageBoundaryHook().run();
        out.flush();

        assertThat( BytePrinter.hex( channel.getBytes() ),
                equalTo( "00 0e 00 00 00 00 00 00    00 00 00 00 00 00 00 00    00 02 00 00 00 00 " ) );
    }
}