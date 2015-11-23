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
import java.nio.channels.WritableByteChannel;

public class BufferedChannelOutput implements PackOutput
{
    private final ByteBuffer buffer;
    private WritableByteChannel channel;

    public BufferedChannelOutput( int bufferSize )
    {
        this.buffer = ByteBuffer.allocateDirect( bufferSize ).order( ByteOrder.BIG_ENDIAN );
    }

    public BufferedChannelOutput( WritableByteChannel channel )
    {
        this( channel, 1024 );
    }

    public BufferedChannelOutput( WritableByteChannel channel, int bufferSize )
    {
        this( bufferSize );
        reset( channel );
    }

    public void reset( WritableByteChannel channel )
    {
        this.channel = channel;
    }

    @Override
    public BufferedChannelOutput flush() throws IOException
    {
        buffer.flip();
        do { channel.write( buffer ); } while ( buffer.remaining() > 0 );
        buffer.clear();
        return this;
    }

    @Override
    public PackOutput writeBytes( byte[] data, int offset, int length ) throws IOException
    {
        int index = 0;
        while ( index < length )
        {
            if ( buffer.remaining() == 0 )
            {
                flush();
            }

            int amountToWrite = Math.min( buffer.remaining(), length - index );

            buffer.put( data, offset + index, amountToWrite );
            index += amountToWrite;
        }
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

    private void ensure( int size ) throws IOException
    {
        if ( buffer.remaining() < size )
        {
            flush();
        }
    }
}
