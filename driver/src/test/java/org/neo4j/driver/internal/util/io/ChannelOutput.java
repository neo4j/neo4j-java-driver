/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.driver.internal.util.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import org.neo4j.driver.internal.packstream.PackOutput;

public class ChannelOutput implements PackOutput
{
    private final WritableByteChannel channel;

    public ChannelOutput( WritableByteChannel channel )
    {
        this.channel = channel;
    }

    @Override
    public PackOutput writeBytes( byte[] data ) throws IOException
    {
        channel.write( ByteBuffer.wrap( data ) );
        return this;
    }

    @Override
    public PackOutput writeByte( byte value ) throws IOException
    {
        channel.write( ByteBuffer.wrap( new byte[]{value} ) );
        return this;
    }

    @Override
    public PackOutput writeShort( short value ) throws IOException
    {
        ByteBuffer buffer = ByteBuffer.allocate( Short.BYTES );
        buffer.putShort( value );
        buffer.flip();
        channel.write( buffer );
        return this;
    }

    @Override
    public PackOutput writeInt( int value ) throws IOException
    {
        ByteBuffer buffer = ByteBuffer.allocate( Integer.BYTES );
        buffer.putInt( value );
        buffer.flip();
        channel.write( buffer );
        return this;
    }

    @Override
    public PackOutput writeLong( long value ) throws IOException
    {
        ByteBuffer buffer = ByteBuffer.allocate( Long.BYTES );
        buffer.putLong( value );
        buffer.flip();
        channel.write( buffer );
        return this;
    }

    @Override
    public PackOutput writeDouble( double value ) throws IOException
    {
        ByteBuffer buffer = ByteBuffer.allocate( Double.BYTES );
        buffer.putDouble( value );
        buffer.flip();
        channel.write( buffer );
        return this;
    }
}
