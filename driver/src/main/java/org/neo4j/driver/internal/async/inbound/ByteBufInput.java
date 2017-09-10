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
package org.neo4j.driver.internal.async.inbound;

import io.netty.buffer.ByteBuf;

import org.neo4j.driver.internal.packstream.PackInput;

import static java.util.Objects.requireNonNull;

public class ByteBufInput implements PackInput
{
    private ByteBuf buf;

    public void start( ByteBuf newBuf )
    {
        assertNotStarted();
        buf = requireNonNull( newBuf );
    }

    public void stop()
    {
        buf = null;
    }

    @Override
    public boolean hasMoreData()
    {
        return buf.isReadable();
    }

    @Override
    public byte readByte()
    {
        return buf.readByte();
    }

    @Override
    public short readShort()
    {
        return buf.readShort();
    }

    @Override
    public int readInt()
    {
        return buf.readInt();
    }

    @Override
    public long readLong()
    {
        return buf.readLong();
    }

    @Override
    public double readDouble()
    {
        return buf.readDouble();
    }

    @Override
    public PackInput readBytes( byte[] into, int offset, int toRead )
    {
        buf.readBytes( into, offset, toRead );
        return this;
    }

    @Override
    public byte peekByte()
    {
        return buf.getByte( buf.readerIndex() );
    }

    @Override
    public Runnable messageBoundaryHook()
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
            }
        };
    }

    private void assertNotStarted()
    {
        if ( buf != null )
        {
            throw new IllegalStateException( "Already started" );
        }
    }
}
