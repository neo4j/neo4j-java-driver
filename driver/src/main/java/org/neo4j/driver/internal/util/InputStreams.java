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
