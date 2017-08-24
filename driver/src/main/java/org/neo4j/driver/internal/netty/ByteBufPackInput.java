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

import org.neo4j.driver.internal.packstream.PackInput;

public class ByteBufPackInput implements PackInput
{
    private ByteBuf buf;

    public void setBuf( ByteBuf buf )
    {
        this.buf = buf;
    }

    @Override
    public boolean hasMoreData() throws IOException
    {
        return buf.isReadable();
    }

    @Override
    public byte readByte() throws IOException
    {
        return buf.readByte();
    }

    @Override
    public short readShort() throws IOException
    {
        return buf.readShort();
    }

    @Override
    public int readInt() throws IOException
    {
        return buf.readInt();
    }

    @Override
    public long readLong() throws IOException
    {
        return buf.readLong();
    }

    @Override
    public double readDouble() throws IOException
    {
        return buf.readDouble();
    }

    @Override
    public PackInput readBytes( byte[] into, int offset, int toRead ) throws IOException
    {
        buf.readBytes( into, offset, toRead );
        return this;
    }

    @Override
    public byte peekByte() throws IOException
    {
        return buf.getByte( buf.readerIndex() );
    }
}
