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
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.packstream.PackOutput;

import static java.lang.Math.max;

public class ChunkedOutput implements PackOutput
{
    public static final short MESSAGE_BOUNDARY = 0;
    public static final int CHUNK_HEADER_SIZE = 2;

    private final ByteBuffer buffer;

    /** The chunk header */
    private int currentChunkHeaderOffset;
    /** Are currently in the middle of writing a chunk? */
    private boolean chunkOpen = false;

    private WritableByteChannel channel;

    public ChunkedOutput(WritableByteChannel ch)
    {
        this( 8192, ch );
    }

    public ChunkedOutput( int bufferSize, WritableByteChannel ch )
    {
        buffer = ByteBuffer.allocateDirect(  max( 16, bufferSize ) );
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
    public PackOutput writeBytes( byte[] data, int offset, int length ) throws IOException
    {
        int index = 0;
        while ( index < length )
        {
            // Ensure there is an open chunk, and that it has at least one byte of space left
            ensure(1);

            // Write as much as we can into the current chunk
            int amountToWrite = Math.min( buffer.remaining(), length - index );

            buffer.put( data, offset, amountToWrite );
            index += amountToWrite;
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

    public Runnable messageBoundaryHook()
    {
        return onMessageComplete;
    }

    public void setOutputStream( OutputStream out )
    {
        this.channel = Channels.newChannel( out );
    }
}
