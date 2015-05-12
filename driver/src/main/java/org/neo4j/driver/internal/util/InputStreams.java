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
import java.io.InputStream;

import org.neo4j.driver.exceptions.ClientException;

public class InputStreams
{
    /**
     * Reads from the input stream until the 'into' array is filled, or throws an exception.
     *
     * @param in
     * @param into
     */
    public static void readAll( InputStream in, byte[] into ) throws IOException
    {
        for(int idx = 0, read; idx<into.length; )
        {
            read = in.read( into, idx, into.length - idx );
            if(read == -1)
            {
                throw new ClientException(
                        "Connection terminated while receiving data. This can happen due to network " +
                        "instabilities, or due to restarts of the database. Expected " + into.length + "bytes, " +
                        "recieved " + idx + ".");
            }
            else
            {
                idx += read;
            }
        }
    }
}
