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
package org.neo4j.driver.v1.internal.connector.socket;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ReadableByteChannel;

import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.internal.packstream.PackInput;
import org.neo4j.driver.v1.internal.util.BytePrinter;

import static java.lang.Math.min;

public class ChunkedInput implements PackInput
{
    // http://stackoverflow.com/questions/2613734/maximum-packet-size-for-a-tcp-connection
    public static final int STACK_OVERFLOW_SUGGESTED_BUFFER_SIZE = 1400;
    private final ByteBuffer buffer;

    /* a special buffer for chunk header */
    private final ByteBuffer chunkHeaderBuffer = ByteBuffer.allocateDirect( 2 );

    /* the size of bytes that have not been read in current incoming chunk */
    private int unreadChunkSize = 0;

    private final ReadableByteChannel channel;

    public ChunkedInput( ReadableByteChannel ch )
    {
        this( STACK_OVERFLOW_SUGGESTED_BUFFER_SIZE, ch );
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
        return hasMoreDataUnreadInCurrentChunk();
        // TODO change the reading mode to non-blocking so that we could also detect
        // if there are more chunks in the channel?
        // this method currently is only valid if we are in the middle of a chunk
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
            return buffer.getInt();
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

    /**
     * Attempts to read {@code toRead} bytes from the channel, however if {@code freeSpace}, the free space in
     * current buffer is less than {@Code toRead}, then only {@code freeSpace} bytes will be read.
     * @param toRead
     */
    private void attempt( int toRead )
    {
        if( toRead == 0 || remainingData() >= toRead )
        {
            return;
        }
        int freeSpace = freeSpace();
        ensure( Math.min( freeSpace, toRead ) );
    }

    /**
     * Block until {@code toRead} bytes are read from channel
     * @param toRead
     */
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
                    if( chunkSize <= 0 )
                    {
                        throw new ClientException( "Invalid non-positive chunk size: " + chunkSize );
                    }
                    readChunk( chunkSize );
                }
            }
            catch( ClosedByInterruptException e )
            {
                throw new ClientException(
                                "Connection to the database was lost because someone called `interrupt()` on the driver thread waiting for a reply. " +
                                "This normally happens because the JVM is shutting down, but it can also happen because your application code or some " +
                                "framework you are using is manually interrupting the thread." );
            }
            catch ( IOException e )
            {
                String message = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
                throw new ClientException( "Unable to process request: " + message + ", expected: " + toRead +
                                           " bytes, buffer: \n" + BytePrinter.hex( buffer ), e );
            }
            /* buffer ready for reading again */
        }
    }

    protected int readChunkSize() throws IOException
    {
        chunkHeaderBuffer.clear();
        channel.read( chunkHeaderBuffer );
        chunkHeaderBuffer.flip();
        return chunkHeaderBuffer.getShort() & 0xffff;
    }

    private void readChunk( int chunkSize ) throws IOException
    {
        if ( chunkSize <= buffer.remaining() )
        {
            buffer.limit( buffer.position() + chunkSize );
            channel.read( buffer );
            buffer.flip();
        }
        else
        {
            unreadChunkSize = chunkSize - buffer.remaining();
            channel.read( buffer ); //current is full after this
            buffer.flip();
        }
    }

    private boolean hasMoreDataUnreadInCurrentChunk()
    {
        return buffer.remaining() > 0 || unreadChunkSize > 0;
    }


    private Runnable onMessageComplete = new Runnable()
    {
        @Override
        public void run()
        {
            // the on message complete should only be called when no data unread from the message buffer
            if( hasMoreDataUnreadInCurrentChunk() )
            {
                throw new ClientException( "Trying to read message complete ending '00 00' while there are more data " +
                                           "left in the message content unread: buffer [" +
                                           BytePrinter.hexInOneLine( buffer, buffer.position(), buffer.remaining() ) +
                                           "], unread chunk size " + unreadChunkSize );
            }
            try
            {
                // read message boundary
                int chunkSize = readChunkSize();
                if ( chunkSize != 0 )
                {
                    throw new ClientException( "Expecting message complete ending '00 00', but got " +
                                               BytePrinter.hex( ByteBuffer.allocate( 2 ).putShort( (short) chunkSize ) ) );
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
