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

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;

import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.MessageFormat.Reader;
import org.neo4j.driver.internal.messaging.MessageFormat.Writer;

import static java.lang.Integer.getInteger;

public class SocketClient
{
    /**
     * Timeout for network read operations. By default, this is disabled (the database may take as long as it likes to
     * reply). However, on networks that suffer from frequent net-splits, there is a serious issue where a socket may
     * erroneously block for very long periods (up to 10 minutes). If your application suffers from this issue, you
     * should enable the network timeout, by setting it to some value significantly higher than your slowest query.
     */
    private static int defaultNetworkTimeout = getInteger( "neo4j.networkTimeoutMs", 0 );

    private final String host;
    private final int port;
    private final int networkTimeout;

    private SocketProtocol protocol;
    private Reader reader;
    private Writer writer;

    private SocketChannel channel;

    public SocketClient( String host, int port, int networkTimeout )
    {
        this.host = host;
        this.port = port;
        this.networkTimeout = networkTimeout;
    }

    public SocketClient( String host, int port )
    {
        this( host, port, defaultNetworkTimeout );
    }

    public void start()
    {
        try
        {
            channel = SocketChannel.open();
            channel.setOption( StandardSocketOptions.SO_REUSEADDR, true );
            channel.setOption( StandardSocketOptions.SO_KEEPALIVE, true );

            channel.connect( new InetSocketAddress( host, port ) );

            protocol = negotiateProtocol();
            reader = protocol.reader();
            writer = protocol.writer();
        }
        catch ( ConnectException e )
        {
            throw new ClientException( String.format( "Unable to connect to '%s' on port %s, " +
                                                      "ensure the database is running and that there is a working " +
                                                      "network " +
                                                      "connection to it.", host, port ) );
        }
        catch ( SocketTimeoutException e )
        {
            throw new ClientException( String.format( "Unable to connect to '%s' on port %s, " +
                                                      "database took longer than network timeout (%dms) to reply.",
                    host, port, networkTimeout ) );
        }
        catch ( IOException e )
        {
            throw new ClientException( "Unable to process request: " + e.getMessage(), e );
        }
    }

    public void send( List<Message> pendingMessages, SocketResponseHandler handler ) throws IOException
    {
        for ( Message message : pendingMessages )
        {
            writer.write( message );
        }
        writer.flush();

        // Wait until all pending requests have been replied to
        while ( handler.receivedResponses() < pendingMessages.size() )
        {
            reader.read( handler );
        }
    }

    public void stop()
    {
        try
        {
            channel.close();
        }
        catch ( IOException e )
        {
            throw new ClientException( "Unable to close socket connection properly." + e.getMessage(), e );
        }
    }

    private SocketProtocol negotiateProtocol() throws IOException
    {
        // Propose protocol versions
        ByteBuffer buf = ByteBuffer.wrap( new byte[]{
                0, 0, 0, 1,
                0, 0, 0, 0,
                0, 0, 0, 0,
                0, 0, 0, 0} );

        while(buf.remaining() > 0 )
        {
            channel.write( buf );
        }

        // Read back the servers choice
        buf.clear();
        buf.limit( 2 );

        while(buf.remaining() > 0)
        {
            channel.read( buf );
        }

        // Choose protocol, or fail
        buf.flip();
        final int proposal = buf.getInt();

        switch ( proposal )
        {
        case 1: return new SocketProtocolV1( channel );
        case 0: throw new ClientException( "The server does not support any of the protocol versions supported by " +
                                           "this driver. Ensure that you are using driver and server versions that " +
                                           "are compatible with one another." );
        default: throw new ClientException( "Protocol error, server suggested unexpected protocol version: " +
                                            proposal );
        }
    }

    @Override
    public String toString()
    {
        int version = protocol == null ? -1 : protocol.version();
        return "SocketClient[protocolVersion=" + version + "]";
    }
}
