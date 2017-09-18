/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import org.neo4j.driver.internal.packstream.PackOutput;
import org.neo4j.driver.v1.exceptions.ClientException;

import static java.lang.Math.max;

public class ChunkedOutput implements PackOutput
{
    public static final short MESSAGE_BOUNDARY = 0;
    public static final int CHUNK_HEADER_SIZE = 2;

    private final ByteBuffer buffer;
    private final WritableByteChannel channel;

    /** The chunk header */
    private int currentChunkHeaderOffset;
    /** Are currently in the middle of writing a chunk? */
    private boolean chunkOpen = false;

    public ChunkedOutput( WritableByteChannel ch )
    {
        this( 8192, ch );
    }

    public ChunkedOutput( int bufferSize, WritableByteChannel ch )
    {
        buffer = ByteBuffer.allocate(  max( 16, bufferSize ) );
        chunkOpen = false;
        channel = ch;
    }

    @Override
    public PackOutput flush() throws IOException
    {
        closeChunkIfOpen();

        buffer.flip();
        channel.write( buffer );
        buffer.clear();

        return this;
    }

    @Override
    public PackOutput writeByte( byte value ) throws IOException
    {
        ensure( 1 );
        buffer.put( value );
        return this;
    }

    @Override
    public PackOutput writeShort( short value ) throws IOException
    {
        ensure( 2 );
        buffer.putShort( value );
        return this;
    }

    @Override
    public PackOutput writeInt( int value ) throws IOException
    {
        ensure( 4 );
        buffer.putInt( value );
        return this;
    }

    @Override
    public PackOutput writeLong( long value ) throws IOException
    {
        ensure( 8 );
        buffer.putLong( value );
        return this;
    }

    @Override
    public PackOutput writeDouble( double value ) throws IOException
    {
        ensure( 8 );
        buffer.putDouble( value );
        return this;
    }

    @Override
    public PackOutput writeBytes( byte[] data ) throws IOException
    {
        int offset = 0;
        int length = data.length;
        while ( offset < length )
        {
            // Ensure there is an open chunk, and that it has at least one byte of space left
            ensure(1);

            // Write as much as we can into the current chunk
            int amountToWrite = Math.min( buffer.remaining(), length - offset );

            buffer.put( data, offset, amountToWrite );
            offset += amountToWrite;
        }
        return this;
    }

    private void closeChunkIfOpen()
    {
        if( chunkOpen )
        {
            int chunkSize = buffer.position() - ( currentChunkHeaderOffset + CHUNK_HEADER_SIZE );
            buffer.putShort( currentChunkHeaderOffset, (short) chunkSize );
            chunkOpen = false;
        }
    }

    private PackOutput ensure( int size ) throws IOException
    {
        int toWriteSize = chunkOpen ? size : size + CHUNK_HEADER_SIZE;
        if ( buffer.remaining() < toWriteSize )
        {
            flush();
        }

        if ( !chunkOpen )
        {
            currentChunkHeaderOffset = buffer.position();
            buffer.position( buffer.position() + CHUNK_HEADER_SIZE );
            chunkOpen = true;
        }

        return this;
    }

    private Runnable onMessageComplete = new Runnable()
    {
        @Override
        public void run()
        {
            try
            {
                closeChunkIfOpen();

                // Ensure there's space to write the message boundary
                if ( buffer.remaining() < CHUNK_HEADER_SIZE )
                {
                    flush();
                }

                // Write message boundary
                buffer.putShort( MESSAGE_BOUNDARY );

                // Mark us as not currently in a chunk
                chunkOpen = false;
            }
            catch ( IOException e )
            {
                throw new ClientException( "Error while sending message complete ending '00 00'.", e );
            }

        }
    };

    @Override
    public Runnable messageBoundaryHook()
    {
        return onMessageComplete;
    }

}
