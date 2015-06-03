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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;

import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.packstream.PackInput;
import org.neo4j.driver.internal.util.BytePrinter;

import static java.lang.Math.min;
import static org.neo4j.driver.internal.util.InputStreams.readAll;

public class ChunkedInput implements PackInput
{
    private final ByteBuffer buffer;

    /* a special buffer for chunk header */
    private final ByteBuffer chunkHeaderBuffer = ByteBuffer.allocateDirect( 2 );

    /* the size of bytes that have not been read in current incoming chunk */
    private int unreadChunkSize = 0;

    private final ReadableByteChannel channel;

    public ChunkedInput( ReadableByteChannel ch )
    {
        this( 1024 * 8, ch );
    }

    public ChunkedInput( int bufferCapacity, ReadableByteChannel channel )
    {
        assert bufferCapacity >= 1;
        buffer = ByteBuffer.allocateDirect( bufferCapacity ).order( ByteOrder.BIG_ENDIAN );
        buffer.limit( 0 );
        this.channel = channel;
    }

    @Override
    public boolean hasMoreData() throws IOException
    {
        return buffer.remaining() > 0;
    }

    @Override
    public byte readByte()
    {
        ensure( 1 );
        return buffer.get();
    }

    @Override
    public short readShort()
    {
        attempt( 2 );
        if ( remainingData() >= 2 )
        {
            return buffer.getShort();
        }
        else
        {
            // Short is crossing chunk boundaries, use slow route
            return (short) (readByte() << 8 & readByte());
        }
    }

    @Override
    public int readInt()
    {
        attempt( 4 );
        if ( remainingData() >= 4 )
        {
            return buffer.getShort();
        }
        else
        {
            // Short is crossing chunk boundaries, use slow route
            return readShort() << 16 & readShort();
        }
    }

    @Override
    public long readLong()
    {
        attempt( 8 );
        if ( remainingData() >= 8 )
        {
            return buffer.getLong();
        }
        else
        {
            // long is crossing chunk boundaries, use slow route
            return ((long) readInt() << 32) & readInt();
        }
    }

    @Override
    public double readDouble()
    {
        attempt( 8 );
        if ( remainingData() >= 8 )
        {
            return buffer.getDouble();
        }
        else
        {
            // double is crossing chunk boundaries, use slow route
            return Double.longBitsToDouble( readLong() );
        }
    }

    @Override
    public PackInput readBytes( byte[] into, int offset, int toRead )
    {
        int toReadFromChunk = min( toRead, freeSpace() );
        ensure( toReadFromChunk );

        // Do the read
        buffer.get( into, offset, toReadFromChunk );

        // Can we read another chunk into the destination buffer?
        if ( toReadFromChunk < toRead )
        {
            // More data can be read into the buffer, keep reading from the next chunk
            readBytes( into, offset + toReadFromChunk, toRead - toReadFromChunk );
        }

        return this;
    }

    @Override
    public byte peekByte()
    {
        ensure( 1 );
        int pos = buffer.position();
        byte nextByte = buffer.get();
        buffer.position( pos );
        return nextByte;
    }

    /**
     * Return the size of free space in a buffer.
     * E.g. Given a buffer with pointers 0 <= position <= limit <= capacity,
     * E.g.
     * Buffer: | 0, 0, 1, 2, 3, 4, 0, 0, 0, 0, 0 |
     *           |     |        |              |
     *           0  position  limit         capacity
     * This method returns capacity - limit + position
     * @return
     */
    private int freeSpace()
    {
        return buffer.capacity() - buffer.limit() + buffer.position();
    }

    /**
     * Return the size of bytes in a buffer.
     * E.g. Given a buffer with pointers 0 <= position <= limit <= capacity,
     * Buffer: | 0, 0, 1, 2, 3, 4, 0, 0, 0, 0, 0 |
     *           |     |        |              |
     *           0  position  limit         capacity
     * this method returns limit - position
     * @return
     */
    private int remainingData()
    {
        return buffer.remaining();
    }

    private void attempt( int toRead )
    {
        if( toRead == 0 || remainingData() >= toRead )
        {
            return;
        }
        int freeSpace = freeSpace();
        ensure( Math.min( freeSpace, toRead ) );
    }

    private void ensure( int toRead )
    {
        if( toRead == 0 || remainingData() >= toRead )
        {
            return;
        }
        assert toRead <= freeSpace();
        while ( remainingData() < toRead )
        {
            // first compact the data in the buffer
            if ( remainingData() > 0 )
            {
                // If there is data remaining in the buffer, shift that remaining data to the beginning of the buffer.
                buffer.compact();
            }
            else
            {
                buffer.clear();
            }
            /* the buffer is ready for writing */
            try
            {
                if( unreadChunkSize > 0 )
                {
                    int freeSpace = buffer.remaining();
                    readChunk( min( freeSpace, unreadChunkSize ) );
                    unreadChunkSize -= freeSpace;
                }
                else
                {
                    int chunkSize = readChunkSize();
                    assert chunkSize != 0;
                    readChunk( chunkSize );
                }
            }
            catch ( IOException e )
            {
                throw new ClientException( "Unable to process request: " + e.getMessage() + ", expect: " + toRead +
                                           ", buffer: \n" + BytePrinter.hex( buffer ), e );
            }
            /* buffer ready for reading again */
        }
    }

    private int readChunkSize() throws IOException
    {
        chunkHeaderBuffer.clear();
        readAll( channel, chunkHeaderBuffer );
        chunkHeaderBuffer.flip();
        return chunkHeaderBuffer.getShort();
    }

    private void readChunk( int chunkSize ) throws IOException
    {
        if ( chunkSize <= buffer.remaining() )
        {
            buffer.limit( buffer.position() + chunkSize );
            readAll( channel, buffer );
            buffer.flip();
        }
        else
        {
            unreadChunkSize = chunkSize - buffer.remaining();
            readAll( channel, buffer ); //current is full after this
            buffer.flip();
        }
    }



    private Runnable onMessageComplete = new Runnable()
    {
        @Override
        public void run()
        {
            try
            {
                // read message boundary
                int chunkSize = readChunkSize();
                if ( chunkSize != 0 )
                {
                    throw new ClientException( "Expecting message complete ending '00 00', but got " +
                                               ByteBuffer.allocate( 2 ).putShort( (short) chunkSize ).array() );
                }
            }
            catch ( IOException e )
            {
                throw new ClientException( "Error while receiving message complete ending '00 00'.", e );
            }

        }
    };

    public Runnable messageBoundaryHook()
    {
        return this.onMessageComplete;
    }
}
