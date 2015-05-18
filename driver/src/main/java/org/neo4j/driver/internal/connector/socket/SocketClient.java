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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.List;

import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.MessageFormat.Reader;
import org.neo4j.driver.internal.messaging.MessageFormat.Writer;
import org.neo4j.driver.internal.spi.Logger;

import static java.lang.Integer.getInteger;
import static org.neo4j.driver.internal.connector.socket.ProtocolChooser.bytes2Int;
import static org.neo4j.driver.internal.connector.socket.ProtocolChooser.chooseVersion;
import static org.neo4j.driver.internal.connector.socket.ProtocolChooser.supportedVersions;

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

    private Socket socket;

    private OutputStream out;
    private InputStream in;

    private SocketProtocol protocol;
    private Reader reader;
    private Writer writer;

    private final Logger logger;

    public SocketClient( String host, int port, Logger logger, int networkTimeout )
    {
        this.host = host;
        this.port = port;
        this.logger = logger;
        this.networkTimeout = networkTimeout;
    }

    public SocketClient( String host, int port, Logger logger )
    {
        this( host, port, logger, defaultNetworkTimeout );
    }

    public void start()
    {
        try
        {
            socket = new Socket();
            if ( networkTimeout > 0 )
            {
                socket.setSoTimeout( networkTimeout );
            }
            socket.connect( new InetSocketAddress( host, port ) );

            out = new MonitoredOutputStream( socket.getOutputStream(), logger );
            in = new MonitoredInputStream( socket.getInputStream(), logger );

            protocol = negotiateProtocol();
            protocol.outputStream( out );
            protocol.inputStream( in );
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
            socket.close();
        }
        catch ( IOException e )
        {
            throw new ClientException( "Unable to close socket connection properly." + e.getMessage(), e );
        }
    }

    private SocketProtocol negotiateProtocol() throws IOException
    {
        logger.debug( "Negotiating protocol..." );
        out.write( supportedVersions() );
        byte[] accepted = new byte[4];
        in.read( accepted ); // TODO check read length
        int version = bytes2Int( accepted );
        logger.debug( "Protocol [" + version + "] chosen." );
        return chooseVersion( version );
    }

    @Override
    public String toString()
    {
        int version = protocol == null ? -1 : protocol.version();
        return "Client using socket protocol version " + version;
    }
}
