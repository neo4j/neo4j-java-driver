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
package org.neo4j.driver.v1.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

public class RecordingByteChannel implements WritableByteChannel, ReadableByteChannel
{
    private final ByteBuffer buffer = ByteBuffer.allocate( 16 * 1024 );
    private int writePosition = 0;
    private int readPosition = 0;
    private boolean eof;

    @Override
    public boolean isOpen()
    {
        return true;
    }

    @Override
    public void close() throws IOException
    {

    }

    @Override
    public int write( ByteBuffer src ) throws IOException
    {
        buffer.position( writePosition );
        int originalPosition = writePosition;

        buffer.put( src );

        writePosition = buffer.position();
        return writePosition - originalPosition;
    }

    @Override
    public int read( ByteBuffer dst ) throws IOException
    {
        if ( readPosition == writePosition )
        {
            return eof ? -1 : 0;
        }
        buffer.position( readPosition );
        int originalPosition = readPosition;
        int originalLimit = buffer.limit();

        buffer.limit( Math.min( buffer.position() + (dst.limit() - dst.position()), writePosition ) );
        dst.put( buffer );

        readPosition = buffer.position();
        buffer.limit( originalLimit );
        return readPosition - originalPosition;
    }

    public byte[] getBytes()
    {
        byte[] bytes = new byte[buffer.position()];
        buffer.position( 0 );
        buffer.get( bytes );
        return bytes;
    }

    /**
     * Mark this buffer as ended. Once whatever is currently unread in it is consumed,
     * it will start yielding -1 responses.
     */
    public void eof()
    {
        eof = true;
    }
}
