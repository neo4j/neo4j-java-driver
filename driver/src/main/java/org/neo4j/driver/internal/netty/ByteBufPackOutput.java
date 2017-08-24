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
package org.neo4j.driver.internal.netty;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

import org.neo4j.driver.internal.packstream.PackOutput;

public class ByteBufPackOutput implements PackOutput
{
    private ByteBuf buf;

    public void setBuf( ByteBuf buf )
    {
        this.buf = buf;
    }

    @Override
    public PackOutput flush() throws IOException
    {
        return this;
    }

    @Override
    public PackOutput writeByte( byte value ) throws IOException
    {
        buf.writeByte( value );
        return this;
    }

    @Override
    public PackOutput writeBytes( byte[] data, int offset, int amountToWrite ) throws IOException
    {
        buf.writeBytes( data, offset, amountToWrite );
        return this;
    }

    @Override
    public PackOutput writeShort( short value ) throws IOException
    {
        buf.writeShort( value );
        return this;
    }

    @Override
    public PackOutput writeInt( int value ) throws IOException
    {
        buf.writeInt( value );
        return this;
    }

    @Override
    public PackOutput writeLong( long value ) throws IOException
    {
        buf.writeLong( value );
        return this;
    }

    @Override
    public PackOutput writeDouble( double value ) throws IOException
    {
        buf.writeDouble( value );
        return this;
    }
}
