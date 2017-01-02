/**
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
package org.neo4j.driver.internal.connector.socket;

import java.net.ServerSocket;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.driver.internal.logging.DevNullLogger;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.exceptions.ClientException;

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
                Config.defaultConfig(), new DevNullLogger() );

        // Expect
        exception.expect( ClientException.class );
        exception.expectMessage( "database took longer than network timeout (100ms) to reply." );

        // When
        client.start();
    }
}
