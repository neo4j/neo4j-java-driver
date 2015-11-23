/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.driver.v1.internal.packstream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;

/**
 * An {@link PackInput} implementation that reads from an input channel into an internal buffer.
 */
public class BufferedChannelInput implements PackInput
{
    private final ByteBuffer buffer;
    private ReadableByteChannel channel;

    public BufferedChannelInput( int bufferCapacity )
    {
        this( bufferCapacity, null );
    }

    public BufferedChannelInput( int bufferCapacity, ReadableByteChannel ch )
    {
        this.buffer = ByteBuffer.allocateDirect( bufferCapacity ).order( ByteOrder.BIG_ENDIAN );
        reset( ch );
    }

    public BufferedChannelInput reset( ReadableByteChannel ch )
    {
        this.channel = ch;
        this.buffer.position( 0 );
        this.buffer.limit( 0 );
        return this;
    }

    @Override
    public boolean hasMoreData() throws IOException
    {
        return attempt( 1 );
    }

    @Override
    public byte readByte() throws IOException
    {
        ensure( 1 );
        return buffer.get();
    }

    @Override
    public short readShort() throws IOException
    {
        ensure( 2 );
        return buffer.getShort();
    }

    @Override
    public int readInt() throws IOException
    {
        ensure( 4 );
        return buffer.getInt();
    }

    @Override
    public long readLong() throws IOException
    {
        ensure( 8 );
        return buffer.getLong();
    }

    @Override
    public double readDouble() throws IOException
    {
        ensure( 8 );
        return buffer.getDouble();
    }

    @Override
    public PackInput readBytes( byte[] into, int index, int toRead ) throws IOException
    {
        int endIndex = index + toRead;
        while ( index < endIndex)
        {
            toRead = Math.min( buffer.remaining(), endIndex - index );
            buffer.get( into, index, toRead );
            index += toRead;
            if ( buffer.remaining() == 0 && index < endIndex )
            {
                attempt( endIndex - index );
                if ( buffer.remaining() == 0 )
                {
                    throw new PackStream.EndOfStream( "Expected " + (endIndex - index) + " bytes available, " +
                                                      "but no more bytes accessible from underlying stream." );
                }
            }
        }
        return this;
    }

    @Override
    public byte peekByte() throws IOException
    {
        ensure( 1 );
        return buffer.get( buffer.position() );
    }

    private boolean attempt( int numBytes ) throws IOException
    {
        if ( buffer.remaining() >= numBytes )
        {
            return true;
        }

        if ( buffer.remaining() > 0 )
        {
            // If there is data remaining in the buffer, shift that remaining data to the beginning of the buffer.
            buffer.compact();
        }
        else
        {
            buffer.clear();
        }

        int count;
        do
        {
            count = channel.read( buffer );
        }
        while ( count >= 0 && (buffer.position() < numBytes && buffer.remaining() != 0) );

        buffer.flip();
        return buffer.remaining() >= numBytes;
    }

    private void ensure( int numBytes ) throws IOException
    {
        if ( !attempt( numBytes ) )
        {
            throw new PackStream.EndOfStream( "Unexpected end of stream while trying to read " + numBytes + " bytes." );
        }
    }
}
