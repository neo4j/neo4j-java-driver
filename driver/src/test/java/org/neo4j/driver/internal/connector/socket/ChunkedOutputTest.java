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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.driver.util.BytePrinter;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class ChunkedOutputTest
{
    private final ByteBuffer writtenData = ByteBuffer.allocate( 1024 );
    private final ChunkedOutput out = new ChunkedOutput( 16 );

    @Test
    public void shouldChunkSingleMessage() throws Throwable
    {
        // When
        out.ensure( 3 ).put( (byte) 1 ).putShort( (short) 2 );
        out.messageBoundaryHook().run();
        out.flush();

        // Then
        assertThat( writtenData.position(), equalTo( 7 ) );
        assertThat( BytePrinter.hex( writtenData, 0, 7 ),
                equalTo( "00 03 01 00 02 00 00 " ) );
    }

    @Test
    public void shouldChunkMessageSpanningMultipleChunks() throws Throwable
    {
        // When
        out.ensure( 8 ).putLong( 1 )
                .ensure( 8 ).putLong( 2 )
                .ensure( 8 ).putLong( 3 );
        out.messageBoundaryHook().run();
        out.flush();

        // Then
        assertThat( writtenData.position(), equalTo( 32 ) );
        assertThat( BytePrinter.hex( writtenData, 0, 32 ),
                equalTo( "00 08 00 00 00 00 00 00    00 01 00 08 00 00 00 00    " +
                         "00 00 00 02 00 08 00 00    00 00 00 00 00 03 00 00\n" ) );
    }

    @Before
    public void setup()
    {
        OutputStream st = new OutputStream()
        {
            @Override
            public void write( int b ) throws IOException
            {
            }

            @Override
            public void write( byte[] buffer ) throws IOException
            {
                writtenData.put( buffer );
            }
        };

        out.setOutputStream( st );
    }
}