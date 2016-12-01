/*
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
package org.neo4j.driver.internal.connector.socket;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;

import org.neo4j.driver.internal.util.BytePrinter;
import org.neo4j.driver.v1.exceptions.ClientException;

/**
 * Utility class for common operations.
 */
public final class SocketUtils
{
    private SocketUtils()
    {
        throw new UnsupportedOperationException( "Do not instantiate" );
    }

    public static void blockingRead(ByteChannel channel, ByteBuffer buf) throws IOException
    {
        while(buf.hasRemaining())
        {
            if (channel.read( buf ) < 0)
            {
                try
                {
                    channel.close();
                }
                catch ( IOException e )
                {
                    // best effort
                }
                String bufStr = BytePrinter.hex( buf ).trim();
                throw new ClientException( String.format(
                        "Connection terminated while receiving data. This can happen due to network " +
                        "instabilities, or due to restarts of the database. Expected %s bytes, received %s.",
                        buf.limit(), bufStr.isEmpty() ? "none" : bufStr ) );
            }
        }
    }

    public static void blockingWrite(ByteChannel channel, ByteBuffer buf) throws IOException
    {
        while(buf.hasRemaining())
        {
            if (channel.write( buf ) < 0)
            {
                try
                {
                    channel.close();
                }
                catch ( IOException e )
                {
                    // best effort
                }
                String bufStr = BytePrinter.hex( buf ).trim();
                throw new ClientException( String.format(
                        "Connection terminated while sending data. This can happen due to network " +
                        "instabilities, or due to restarts of the database. Expected %s bytes, wrote %s.",
                        buf.limit(), bufStr.isEmpty() ? "none" :bufStr ) );
            }
        }
    }
}
