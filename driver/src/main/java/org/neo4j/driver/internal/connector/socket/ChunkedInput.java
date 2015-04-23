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
import java.io.InputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.packstream.PackInput;
import org.neo4j.packstream.PackStream;

public class ChunkedInput implements PackInput
{
    private List<ByteBuffer> chunks = new ArrayList<>();
    private ByteBuffer currentChunk = null;
    private int currentChunkIndex = -1;

    private int remaining = 0;
    private InputStream in;

    public ChunkedInput clear()
    {
        currentChunk = null;
        currentChunkIndex = -1;
        remaining = 0;

        if ( chunks.size() > 128 )
        {
            // faster to allocate a new one than to release if the list is large
            chunks = new ArrayList<>();
        }
        else
        {
            chunks.clear();
        }
        return this;
    }

    public void addChunk( ByteBuffer chunk )
    {
        assert chunk.position() == 0;
        if ( chunk.limit() > 0 )
        {
            chunks.add( chunk );
            remaining += chunk.limit();
        }
    }

    @Override
    public PackInput ensure( int numBytes ) throws IOException
    {
        if ( !attempt( numBytes ) )
        {
            throw new PackStream.EndOfStream( "Unexpected end of stream while trying to read " + numBytes + " bytes." );
        }
        return this;
    }

    @Override
    public PackInput attemptUpTo( int numBytes ) throws IOException
    {
        ensureChunkAvailable( numBytes );
        return this;
    }

    @Override
    public boolean attempt( int numBytes ) throws IOException
    {
        ensureChunkAvailable( numBytes );
        return remaining >= numBytes;
    }

    @Override
    public int remaining()
    {
        return remaining;
    }

    @Override
    public byte get()
    {
        ensureChunkAvailable( 1 );
        remaining -= 1;
        return currentChunk.get();
    }

    @Override
    public byte peek()
    {
        ensureChunkAvailable( 1 );
        int pos = currentChunk.position();
        byte nextByte = currentChunk.get();
        currentChunk.position( pos );
        return nextByte;
    }

    @Override
    public short getShort()
    {
        ensureChunkAvailable( 2 );
        if ( currentChunk.remaining() >= 2 )
        {
            remaining -= 2;
            return currentChunk.getShort();
        }
        else
        {
            // Short is crossing chunk boundaries, use slow route
            return (short) (get() << 8 & get());
        }
    }

    @Override
    public int getInt()
    {
        ensureChunkAvailable( 4 );
        if ( currentChunk.remaining() >= 4 )
        {
            remaining -= 4;
            return currentChunk.getInt();
        }
        else
        {
            // int is crossing chunk boundaries, use slow route
            return (getShort() << 16) & getShort();
        }
    }

    @Override
    public long getLong()
    {
        ensureChunkAvailable( 8 );
        if ( currentChunk.remaining() >= 8 )
        {
            remaining -= 8;
            return currentChunk.getLong();
        }
        else
        {
            // long is crossing chunk boundaries, use slow route
            return ((long) getInt() << 32) & getInt();
        }
    }

    @Override
    public double getDouble()
    {
        ensureChunkAvailable( 8 );
        if ( currentChunk.remaining() >= 8 )
        {
            remaining -= 8;
            return currentChunk.getDouble();
        }
        else
        {
            // double is crossing chunk boundaries, use slow route
            return Double.longBitsToDouble( getLong() );
        }
    }

    @Override
    public PackInput get( byte[] into, int offset, int toRead )
    {
        ensureChunkAvailable( toRead );
        int toReadFromChunk = Math.min( toRead, currentChunk.remaining() );

        // Do the read
        currentChunk.get( into, offset, toReadFromChunk );
        remaining -= toReadFromChunk;

        // Can we read another chunk into the destination buffer?
        if ( toReadFromChunk < toRead )
        {
            // More data can be read into the buffer, keep reading from the next chunk
            get( into, offset + toReadFromChunk, toRead - toReadFromChunk );
        }

        return this;
    }

    private void ensureChunkAvailable( int toRead )
    {
        while ( remaining < toRead )
        {
            // if not enough data in chunk list, we read more data from input stream
            try
            {
                ByteBuffer chunk = readNextChunk();
                addChunk( chunk );
            }
            catch ( IOException e )
            {
                // TODO or maybe just a simple IOException as it deals with socket IO directly
                throw new ClientException( "Unable to process request: " + e.getMessage(), e );
            }
        }

        if ( currentChunk == null || currentChunk.remaining() == 0 )
        {
            currentChunkIndex++;
            if ( currentChunkIndex < chunks.size() )
            {
                currentChunk = chunks.get( currentChunkIndex );
            }
            else
            {
                throw new BufferOverflowException();
            }
        }
    }

    private ByteBuffer readNextChunk() throws IOException
    {
        int chunkSize = readChunkSize();
        byte[] buffer = new byte[chunkSize];
        in.read( buffer );
        ByteBuffer chunk = ByteBuffer.allocate( chunkSize );
        chunk.put( buffer );
        chunk.flip();
        return chunk;
    }

    private int readChunkSize() throws IOException
    {
        byte[] buffer = new byte[2];
        in.read( buffer );
        int chunkSize = ByteBuffer.wrap( buffer ).getShort();
        return chunkSize;
    }

    public void setInputStream( InputStream in )
    {
        this.in = in;
    }
}
