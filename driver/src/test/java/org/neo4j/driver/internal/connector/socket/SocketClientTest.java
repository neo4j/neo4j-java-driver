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
package org.neo4j.driver.internal.connector.socket;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.ServerSocket;

import org.neo4j.driver.exceptions.ClientException;

public class SocketClientTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    // TODO: This is not possible with blocking NIO channels, unless we use inputStreams, but then we can't use
    // off-heap buffers. We need to swap to use selectors, which would allow us to time out.
    @Test
    @Ignore
    public void testNetworkTimeout() throws Throwable
    {
        // Given a server that will never reply
        ServerSocket server = new ServerSocket( 0 );

        // And given we've configured a client with network timeout
        int networkTimeout = 100;
        SocketClient client = new SocketClient( "localhost", server.getLocalPort(),
                networkTimeout );

        // Expect
        exception.expect( ClientException.class );
        exception.expectMessage( "database took longer than network timeout (100ms) to reply." );

        // When
        client.start();
    }
}