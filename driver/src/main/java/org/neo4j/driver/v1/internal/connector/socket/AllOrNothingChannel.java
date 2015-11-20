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
package org.neo4j.driver.v1.internal.connector.socket;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.SocketChannel;

import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.internal.util.BytePrinter;

/**
 * Wraps a regular socket channel such that read and write will not return until the full buffers given have been sent
 * or received, respectively.
 */
public class AllOrNothingChannel implements ByteChannel
{
    private final SocketChannel channel;

    public AllOrNothingChannel( SocketChannel channel ) throws IOException
    {
        this.channel = channel;
        this.channel.configureBlocking( true );
    }

    @Override
    public int read( ByteBuffer buf ) throws IOException
    {
        int toRead = buf.remaining();
        while ( buf.remaining() > 0 )
        {
            int read = channel.read( buf );
            if ( read == -1 )
            {
                throw new ClientException( String.format(
                        "Connection terminated while receiving data. This can happen due to network " +
                        "instabilities, or due to restarts of the database. Expected %s bytes, received %s.",
                        buf.limit(), BytePrinter.hex( buf ) ) );
            }
        }
        return toRead;
    }

    @Override
    public int write( ByteBuffer buf ) throws IOException
    {
        int toWrite = buf.remaining();
        while( buf.remaining() > 0 )
        {
            int write = channel.write( buf );
            if( write == -1 )
            {
                throw new ClientException( String.format(
                        "Connection terminated while sending data. This can happen due to network " +
                        "instabilities, or due to restarts of the database. Expected %s bytes, wrote %s.",
                        buf.limit(), BytePrinter.hex( buf ) ) );
            }
        }
        return toWrite;
    }

    @Override
    public boolean isOpen()
    {
        return channel.isOpen();
    }

    @Override
    public void close() throws IOException
    {
        channel.close();
    }
}
