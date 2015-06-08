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
package org.neo4j.driver.internal.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import org.neo4j.driver.exceptions.ClientException;

public class InputStreams
{
    /**
     * Reads from the input channel until the 'into' buffer is filled, or throws an exception.
     *
     * @param channel
     * @param into
     */
    public static void readAll( ReadableByteChannel channel, ByteBuffer into ) throws IOException
    {
        while( into.remaining() > 0 )
        {
            int read = channel.read( into );
            if ( read == -1 )
            {
                throw new ClientException(
                        "Connection terminated while receiving data. This can happen due to network " +
                        "instabilities, or due to restarts of the database. Expected " + into.limit() + "bytes, " +
                        "recieved " + BytePrinter.hex( into ) + "." );
            }
        }
    }
}
