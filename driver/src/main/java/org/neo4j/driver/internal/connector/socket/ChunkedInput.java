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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.LinkedList;

import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.packstream.PackInput;

/**
 * This class provides a non-thread-safe buffer for reading data from an input channel.
 * The buffer is initialized with an initial size but the size could grow linearly if more space is required.
 */
public class ChunkedInput implements PackInput
{
    /* This empty buffer list enables the buffer to grow linearly automatically */
    private final LinkedList<ByteBuffer> emptyBuffers = new LinkedList<>();
    private final LinkedList<ByteBuffer> filledBuffers = new LinkedList<>();

    /* count how many bytes are readable in filledBuffers */
    private int remaining = 0;

    /* A reference to the current readable buffer.
     * It always points to the first buffer in filledBuffers */
    private ByteBuffer readBuffer;

    /* A reference to the current writable buffer.
     It always points to the last buffer in filledBuffers */
    private ByteBuffer writeBuffer;

    /* a special buffer for chunk header */
    private final ByteBuffer headerSizeBuffer = ByteBuffer.allocateDirect( 2 );

    private final int bufferCapicity;

    private ReadableByteChannel channel;

    public ChunkedInput()
    {
        this( 1024 * 8, null );
    }

    public ChunkedInput( int bufferCapacity, ReadableByteChannel channel )
    {
        this.bufferCapicity = bufferCapacity;
        writeBuffer = newBuffer();
        filledBuffers.add( writeBuffer );
        this.channel = channel;
    }

    public ChunkedInput reset( ReadableByteChannel ch )
    {
        this.channel = ch;
        for ( ByteBuffer filled : filledBuffers )
        {
            remaining -= filled.remaining();
            filled.limit( 0 );
            filledBuffers.pop();
            emptyBuffers.add( filled );
        }
        assert remaining == 0;
        return this;
    }

    @Override
    public boolean hasMoreData() throws IOException
    {
        return remaining > 0;
    }

    @Override
    public byte readByte()
    {
        ensure( 1 );
        remaining -= 1;
        return readBuffer.get();
    }

    @Override
    public short readShort()
    {
        ensure( 2 );
        if ( readBuffer.remaining() >= 2 )
        {
            remaining -= 2;
            return readBuffer.getShort();
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
        ensure( 4 );
        if ( readBuffer.remaining() >= 4 )
        {
            remaining -= 4;
            return readBuffer.getShort();
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
        ensure( 8 );
        if ( readBuffer.remaining() >= 8 )
        {
            remaining -= 8;
            return readBuffer.getLong();
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
        ensure( 8 );
        if ( readBuffer.remaining() >= 8 )
        {
            remaining -= 8;
            return readBuffer.getDouble();
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
        ensure( toRead );
        int toReadFromChunk = Math.min( toRead, readBuffer.remaining() );

        // Do the read
        readBuffer.get( into, offset, toReadFromChunk );
        remaining -= toReadFromChunk;

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
        int pos = readBuffer.position();
        byte nextByte = readBuffer.get();
        readBuffer.position( pos );
        return nextByte;
    }

    private void ensure( int toRead )
    {
        if ( toRead == 0 )
        {
            return;
        }

        while ( remaining < toRead )
        {
            try
            {
                readNextChunk();
            }
            catch ( IOException e )
            {
                throw new ClientException( "Unable to process request: " + e.getMessage() + ", expect: " + toRead +
                                           ", " + remaining + " bytes remaining.", e );
            }
        }

        if ( readBuffer == null || readBuffer.remaining() == 0 )
        {
            if ( filledBuffers.size() > 0 )
            {
                if ( readBuffer != null )
                {
                    readBuffer.clear();
                    emptyBuffers.add( readBuffer );
                }
                readBuffer = filledBuffers.pop();
            }
            else
            {

                throw new ClientException( "Fatal error while reading network data, expected: " + toRead + ", " +
                                           remaining + " bytes remaining. " );

            }
        }
    }

    private ByteBuffer newBuffer()
    {
        ByteBuffer buffer = ByteBuffer.allocateDirect( bufferCapicity ).order( ByteOrder.BIG_ENDIAN );
        buffer.limit( 0 );
        return buffer;
    }

    private void readNextChunk() throws IOException
    {
        int chunkSize = readChunkSize();
        assert chunkSize != 0;
        readChunk( chunkSize );
        remaining += chunkSize;

    }

    private int readChunkSize() throws IOException
    {
        headerSizeBuffer.clear();
        channel.read( headerSizeBuffer );// TODO should test for how many bytes has been read for every channe.read?
        headerSizeBuffer.flip();
        return headerSizeBuffer.getShort();
    }

    private void readChunk( int chunkSize ) throws IOException
    {
        if ( writeBuffer.remaining() > 0 )
        {
            // If there is data remaining in the buffer, shift that remaining data to the beginning of the buffer.
            writeBuffer.compact();
        }
        else
        {
            writeBuffer.clear();
        }

        if ( chunkSize <= writeBuffer.remaining() )
        {
            writeBuffer.limit( writeBuffer.position() + chunkSize );
            channel.read( writeBuffer );
            writeBuffer.flip();
        }
        else
        {
            int left = chunkSize - writeBuffer.remaining();
            channel.read( writeBuffer ); //current is full after this
            writeBuffer.flip();

            // get next empty buffer or create a new one if no left
            writeBuffer = this.emptyBuffers.size() > 0 ? this.emptyBuffers.pop() : newBuffer();
            filledBuffers.add( writeBuffer );

            readChunk( left );
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

    public void setInputStream( InputStream in )
    {
        this.channel = Channels.newChannel( in );
    }
}
