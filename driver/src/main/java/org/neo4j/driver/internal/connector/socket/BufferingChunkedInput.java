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
package org.neo4j.driver.internal.connector.socket;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ReadableByteChannel;

import org.neo4j.driver.internal.packstream.PackInput;
import org.neo4j.driver.internal.util.BytePrinter;
import org.neo4j.driver.v1.exceptions.ClientException;

import static java.lang.Math.min;

/**
 * BufferingChunkedInput reads data in chunks but maintains a buffer so that every time it goes to the
 * underlying channel it reads up to {@value #STACK_OVERFLOW_SUGGESTED_BUFFER_SIZE} bytes.
 */
public class BufferingChunkedInput implements PackInput
{
    // http://stackoverflow.com/questions/2613734/maximum-packet-size-for-a-tcp-connection
    private static final int STACK_OVERFLOW_SUGGESTED_BUFFER_SIZE = 1400;

    /**
     * Main buffer, everytime we read from the underlying channel we try to fill up the entire buffer.
     */
    private final ByteBuffer buffer;

    /**
     * Scratch buffer used for obtaining results from the main buffer.
     */
    private final ByteBuffer scratchBuffer;

    /**
     * The underlying channel to read from
     */
    private final ReadableByteChannel channel;

    /**
     * State of the internal state machine used for reading from the channel.
     */
    private State state;

    /**
     * The remaining size of the current incoming chunk.
     */
    private int remainingChunkSize = 0;

    /**
     * Creates a BufferingChunkedInput from a given channel.
     * @param ch The channel to read from.
     */
    public BufferingChunkedInput( ReadableByteChannel ch )
    {
        this( ch, STACK_OVERFLOW_SUGGESTED_BUFFER_SIZE );
    }

    /**
     * Creates a BufferingChunkedInput from a given channel with a specified buffer size.
     * @param channel The channel to read from
     * @param bufferCapacity The capacity of the buffer.
     */
    public BufferingChunkedInput( ReadableByteChannel channel, int bufferCapacity )
    {
        assert bufferCapacity >= 1;
        this.buffer = ByteBuffer.allocateDirect( bufferCapacity ).order( ByteOrder.BIG_ENDIAN );
        this.buffer.limit( 0 );
        this.scratchBuffer = ByteBuffer.allocateDirect( 8 ).order( ByteOrder.BIG_ENDIAN );
        this.channel = channel;
        this.state = State.AWAITING_CHUNK;
    }

    /*
     * Use only in tests
     */
    int remainingChunkSize()
    {
        return remainingChunkSize;
    }

    /**
     * Internal state machine used for reading data from the channel into the buffer.
     */
    private enum State
    {
        AWAITING_CHUNK
                {
                    @Override
                    public State readChunkSize( BufferingChunkedInput ctx ) throws IOException
                    {
                        if ( ctx.buffer.remaining() == 0 )
                        {
                            //buffer empty, block until you get at least at least one byte
                            while ( ctx.buffer.remaining() == 0 )
                            {
                                readNextPacket( ctx.channel, ctx.buffer );
                            }
                            return AWAITING_CHUNK.readChunkSize( ctx );
                        }
                        else if ( ctx.buffer.remaining() >= 2 )
                        {
                            //enough space to read the whole chunk-size, store it and continue
                            //to read the rest of the chunk
                            ctx.remainingChunkSize = ctx.buffer.getShort() & 0xFFFF;
                            return IN_CHUNK;
                        }
                        else
                        {
                            //only 1 byte in buffer, read that and continue
                            //to read header
                            byte partialChunkSize = ctx.buffer.get();
                            ctx.remainingChunkSize = partialChunkSize << 8;
                            return IN_HEADER.readChunkSize( ctx );
                        }
                    }

                    @Override
                    public State read( BufferingChunkedInput ctx ) throws IOException
                    {
                        //read chunk size and then proceed to read the rest of the chunk.
                        return readChunkSize( ctx ).read( ctx );
                    }

                    @Override
                    public State peekByte( BufferingChunkedInput ctx ) throws IOException
                    {
                        //read chunk size and then proceed to read the rest of the chunk.
                        return readChunkSize( ctx ).peekByte( ctx );
                    }
                },
        IN_CHUNK
                {
                    @Override
                    public State readChunkSize( BufferingChunkedInput ctx ) throws IOException
                    {
                        if ( ctx.remainingChunkSize == 0 )
                        {
                            //we are done reading the chunk, start reading the next one
                            return AWAITING_CHUNK.readChunkSize( ctx );
                        }
                        else
                        {
                            //We should already have read the entire chunk size by now
                            throw new IllegalStateException( "Chunk size has already been read" );
                        }
                    }

                    @Override
                    public State read( BufferingChunkedInput ctx ) throws IOException
                    {
                        if ( ctx.remainingChunkSize == 0 )
                        {
                            //we are done reading the chunk, start reading the next one
                            return AWAITING_CHUNK.read( ctx );
                        }
                        else if ( ctx.buffer.remaining() < ctx.scratchBuffer.remaining() )
                        {
                            //not enough room in buffer, store what is there and then fetch more data
                            int bytesToRead = min( ctx.buffer.remaining(), ctx.remainingChunkSize );
                            copyBytes( ctx.buffer, ctx.scratchBuffer, bytesToRead );
                            ctx.remainingChunkSize -= bytesToRead;
                            readNextPacket( ctx.channel, ctx.buffer );
                            return IN_CHUNK.read( ctx );
                        }
                        else
                        {
                            //plenty of room in buffer, store it
                            int bytesToRead = min( ctx.scratchBuffer.remaining(), ctx.remainingChunkSize );
                            copyBytes( ctx.buffer, ctx.scratchBuffer, bytesToRead );
                            ctx.remainingChunkSize -= bytesToRead;
                            if ( ctx.scratchBuffer.remaining() == 0 )
                            {
                                //we have written all data that was asked for us
                                return IN_CHUNK;
                            }
                            else
                            {
                                //Reached a msg boundary, proceed to next chunk
                                return AWAITING_CHUNK.read( ctx );
                            }
                        }
                    }

                    @Override
                    public State peekByte( BufferingChunkedInput ctx ) throws IOException
                    {
                        if ( ctx.remainingChunkSize == 0 )
                        {
                            //we are done reading the chunk, start reading the next one
                            return AWAITING_CHUNK.peekByte( ctx );
                        }
                        else if ( ctx.buffer.remaining() == 0 )
                        {
                            //no data in buffer, fill it up an try again
                            readNextPacket( ctx.channel, ctx.buffer );
                            return IN_CHUNK.peekByte( ctx );
                        }
                        else
                        {
                            return IN_CHUNK;
                        }
                    }
                },
        IN_HEADER
                {
                    @Override
                    public State readChunkSize( BufferingChunkedInput ctx ) throws IOException
                    {
                        if ( ctx.buffer.remaining() >= 1 )
                        {
                            //Now we have enough space to read the rest of the chunk size
                            byte partialChunkSize = ctx.buffer.get();
                            ctx.remainingChunkSize = ctx.remainingChunkSize | (partialChunkSize & 0xFF);
                            return IN_CHUNK;
                        }
                        else
                        {
                            //Buffer is empty, fill it up and try again
                            readNextPacket( ctx.channel, ctx.buffer );
                            return IN_HEADER.readChunkSize( ctx );
                        }
                    }

                    @Override
                    public State read( BufferingChunkedInput ctx ) throws IOException
                    {
                        throw new IllegalStateException( "Cannot read data while in progress of reading header" );
                    }

                    @Override
                    public State peekByte( BufferingChunkedInput ctx ) throws IOException
                    {
                        throw new IllegalStateException( "Cannot read data while in progress of reading header" );
                    }
                };

        /**
         * Reads the size of the current incoming chunk.
         * @param ctx A reference to the input.
         * @return The next state.
         * @throws IOException
         */
        public abstract State readChunkSize( BufferingChunkedInput ctx ) throws IOException;

        /**
         * Reads the current incoming chunk.
         * @param ctx A reference to the input.
         * @return The next state.
         * @throws IOException
         */
        public abstract State read( BufferingChunkedInput ctx ) throws IOException;

        /**
         * Makes sure there is at least one byte in the buffer but doesn't consume it.
         * @param ctx A reference to the input.
         * @return The next state.
         * @throws IOException
         */
        public abstract State peekByte( BufferingChunkedInput ctx ) throws IOException;

        /**
         * Read data from the underlying channel into the buffer.
         * @param channel The channel to read from.
         * @param buffer The buffer to read into
         * @throws IOException
         */
        private static void readNextPacket( ReadableByteChannel channel, ByteBuffer buffer ) throws IOException
        {
            try
            {
                buffer.clear();
                int read = channel.read( buffer );
                if ( read == -1 )
                {
                    throw new ClientException(
                            "Connection terminated while receiving data. This can happen due to network " +
                            "instabilities, or due to restarts of the database." );
                }
                buffer.flip();
            }
            catch ( ClosedByInterruptException e )
            {
                throw new ClientException(
                        "Connection to the database was lost because someone called `interrupt()` on the driver " +
                        "thread waiting for a reply. " +
                        "This normally happens because the JVM is shutting down, but it can also happen because your " +
                        "application code or some " +
                        "framework you are using is manually interrupting the thread." );
            }
            catch ( IOException e )
            {
                String message = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
                throw new ClientException(
                        "Unable to process request: " + message + " buffer: \n" + BytePrinter.hex( buffer ), e );
            }
        }

        /**
         * Copy data from the buffer into the scratch buffer
         */
        private static void copyBytes( ByteBuffer from, ByteBuffer to, int bytesToRead )
        {
            //Use a temporary buffer and move over in one go
            ByteBuffer temporaryBuffer = from.duplicate();
            temporaryBuffer.limit( temporaryBuffer.position() + bytesToRead );
            to.put( temporaryBuffer );

            //move position so it looks like we have read from buffer
            from.position( from.position() + bytesToRead );
        }
    }

    @Override
    public boolean hasMoreData() throws IOException
    {
        return hasMoreDataUnreadInCurrentChunk();
    }

    @Override
    public byte readByte() throws IOException
    {
        fillScratchBuffer( 1 );
        return scratchBuffer.get();
    }

    @Override
    public short readShort() throws IOException
    {
        fillScratchBuffer( 2 );
        return scratchBuffer.getShort();
    }

    @Override
    public int readInt() throws IOException
    {
        fillScratchBuffer( 4 );
        return scratchBuffer.getInt();
    }

    @Override
    public long readLong() throws IOException
    {
        fillScratchBuffer( 8 );
        return scratchBuffer.getLong();
    }

    @Override
    public double readDouble() throws IOException
    {
        fillScratchBuffer( 8 );
        return scratchBuffer.getDouble();
    }

    @Override
    public PackInput readBytes( byte[] into, int offset, int toRead ) throws IOException
    {
        int left = toRead;
        while ( left > 0 )
        {
            int bufferSize = min( 8, left );
            fillScratchBuffer( bufferSize );
            scratchBuffer.get( into, offset, bufferSize );
            left -= bufferSize;
            offset += bufferSize;
        }
        return this;
    }

    @Override
    public byte peekByte() throws IOException
    {
        state = state.peekByte( this );
        return buffer.get( buffer.position() );
    }

    private boolean hasMoreDataUnreadInCurrentChunk()
    {
        return remainingChunkSize > 0;
    }

    private Runnable onMessageComplete = new Runnable()
    {
        @Override
        public void run()
        {
            // the on message complete should only be called when no data unread from the message buffer
            if ( hasMoreDataUnreadInCurrentChunk() )
            {
                throw new ClientException( "Trying to read message complete ending '00 00' while there are more data " +
                                           "left in the message content unread: buffer [" +
                                           BytePrinter.hexInOneLine( buffer, buffer.position(), buffer.remaining() ) +
                                           "], unread chunk size " + remainingChunkSize );
            }
            try
            {
                // read message boundary
                state.readChunkSize( BufferingChunkedInput.this );
                if ( remainingChunkSize != 0 )
                {
                    throw new ClientException( "Expecting message complete ending '00 00', but got " +
                                               BytePrinter.hex( ByteBuffer.allocate( 2 )
                                                       .putShort( (short) remainingChunkSize ) ) );
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

    /**
     * Fills the scratch buffet with data from the main buffer. If there is not
     * enough data in the buffer more data will be read from the channel.
     *
     * @param bytesToRead The number of bytes to transfer to the scratch buffer.
     * @throws IOException
     */
    private void fillScratchBuffer( int bytesToRead ) throws IOException
    {
        assert (bytesToRead <= scratchBuffer.capacity());
        scratchBuffer.clear();
        scratchBuffer.limit( bytesToRead );
        state = state.read( this );
        scratchBuffer.flip();
    }
}
