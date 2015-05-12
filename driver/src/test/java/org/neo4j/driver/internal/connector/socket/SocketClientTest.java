package org.neo4j.driver.internal.connector.socket;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.ServerSocket;

import org.neo4j.driver.exceptions.ClientException;

public class SocketClientTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void testNetworkTimeout() throws Throwable
    {
        // Given a server that will never reply
        ServerSocket server = new ServerSocket( 0 );

        // And given we've configured a client with network timeout
        int networkTimeout = 100;
        SocketClient client = new SocketClient( "localhost", server.getLocalPort(), networkTimeout );

        // Expect
        exception.expect( ClientException.class );
        exception.expectMessage( "database took longer than network timeout (100ms) to reply." );

        // When
        client.start();
    }
}