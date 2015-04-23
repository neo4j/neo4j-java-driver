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

import org.neo4j.packstream.PackOutput;

public class ChunkedOutput implements PackOutput
{
    public static final int CHUNK_HEADER_SIZE = 2;
    private final int bufferSize;
    private ByteBuffer buffer;
    private OutputStream out;
    private int currentChunkHeaderOffset;
    /** Are currently in the middle of writing a chunk? */
    private boolean chunkOpen = false;
    private Runnable onMessageComplete = new Runnable()
    {
        @Override
        public void run()
        {
            try
            {
                closeCurrentChunk();

                // Write message boundary
                ensure( CHUNK_HEADER_SIZE );
                putShort( (short) 0 );

                // Mark us as not currently in a chunk
                chunkOpen = false;
            }
            catch ( IOException e )
            {
                // TODO: Don't use runnable here then, use something that can throw this IOException
                throw new RuntimeException( e );
            }

        }
    };

    public ChunkedOutput()
    {
        this( 8192 );
    }

    public ChunkedOutput( int bufferSize )
    {
        this.bufferSize = bufferSize;
    }

    @Override
    public PackOutput ensure( int size ) throws IOException
    {
        if ( buffer == null )
        {
            newBuffer();
        }
        else if ( buffer.remaining() < size )
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

    @Override
    public PackOutput flush() throws IOException
    {
        if ( chunkOpen )
        {
            closeCurrentChunk();
        }

        byte[] bytes = new byte[buffer.position()];
        buffer.flip();
        buffer.get( bytes, 0, buffer.limit() );
        out.write( bytes );
        newBuffer();

        return this;
    }

    @Override
    public PackOutput put( byte value )
    {
        assert chunkOpen;
        buffer.put( value );
        return this;
    }

    @Override
    public PackOutput putShort( short value )
    {
        assert chunkOpen;
        buffer.putShort( value );
        return this;
    }

    @Override
    public PackOutput putInt( int value )
    {
        assert chunkOpen;
        buffer.putInt( value );
        return this;
    }

    @Override
    public PackOutput putLong( long value )
    {
        assert chunkOpen;
        buffer.putLong( value );
        return this;
    }

    @Override
    public PackOutput putDouble( double value )
    {
        assert chunkOpen;
        buffer.putDouble( value );
        return this;
    }

    @Override
    public PackOutput put( byte[] data, int offset, int length ) throws IOException
    {
        int index = 0;
        while ( index < length )
        {
            int amountToWrite = Math.min( buffer.remaining(), length - index );
            ensure( amountToWrite );

            buffer.put( data, offset, amountToWrite );
            index += amountToWrite;

            if ( buffer.remaining() == 0 )
            {
                flush();
            }
        }
        return this;
    }

    private void closeCurrentChunk()
    {
        int chunkSize = buffer.position() - (currentChunkHeaderOffset + CHUNK_HEADER_SIZE);
        buffer.putShort( currentChunkHeaderOffset, (short) chunkSize );
    }

    private void newBuffer()
    {
        buffer = ByteBuffer.allocate( bufferSize );
        chunkOpen = false;
    }

    public Runnable messageBoundaryHook()
    {
        return onMessageComplete;
    }

    public void setOutputStream( OutputStream out )
    {
        this.out = out;
    }
}
