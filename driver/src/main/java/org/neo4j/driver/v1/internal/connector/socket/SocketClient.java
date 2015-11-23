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
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.SocketChannel;
import java.security.GeneralSecurityException;
import java.util.List;

import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.internal.messaging.Message;
import org.neo4j.driver.v1.internal.spi.Logger;
import org.neo4j.driver.v1.internal.messaging.MessageFormat;

public class SocketClient
{
    private final String host;
    private final int port;
    private final Logger logger;
    protected final Config config;

    private SocketProtocol protocol;
    private MessageFormat.Reader reader;
    private MessageFormat.Writer writer;

    private ByteChannel channel;

    public SocketClient( String host, int port, Config config, Logger logger )
    {
        this.host = host;
        this.port = port;
        this.config = config;
        this.logger = logger;
        this.channel = null;
    }

    public void start()
    {
        try
        {
            logger.debug( "~~ [CONNECT] %s:%d.", host, port );
            channel = ChannelFactory.create( host, port, config, logger );

            protocol = negotiateProtocol();
            reader = protocol.reader();
            writer = protocol.writer();
        }
        catch ( ConnectException e )
        {
            throw new ClientException( String.format(
                    "Unable to connect to '%s' on port %s, ensure the database is running and that there is a " +
                    "working network connection to it.", host, port ) );
        }
        catch ( IOException e )
        {
            throw new ClientException( "Unable to process request: " + e.getMessage(), e );
        }
        catch ( GeneralSecurityException e )
        {
            throw new ClientException( "Unable to establish ssl connection with server: " + e.getMessage(), e );
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
            channel = null;
            logger.debug( "~~ [CLOSE]" );
        }
        catch ( IOException e )
        {
            if( e.getMessage().equals( "An existing connection was forcibly closed by the remote host" ) )
            {
                // Swallow this exception as it is caused by connection already closed by server
            }
            else
            {
                throw new ClientException("Unable to close socket connection properly." + e.getMessage(), e);
            }
        }
    }

    private SocketProtocol negotiateProtocol() throws IOException
    {
        // TODO make this not so hard-coded
        logger.debug( "~~ [HANDSHAKE] [1, 0, 0, 0]." );
        // Propose protocol versions
        ByteBuffer buf = ByteBuffer.wrap( new byte[]{
                0, 0, 0, 1,
                0, 0, 0, 0,
                0, 0, 0, 0,
                0, 0, 0, 0} );

        channel.write( buf );

        // Read back the servers choice
        buf.clear();
        buf.limit( 4 );

        channel.read( buf );

        // Choose protocol, or fail
        buf.flip();
        final int proposal = buf.getInt();

        switch ( proposal )
        {
        case 1:
            logger.debug( "~~ [HANDSHAKE] 1" );
            return new SocketProtocolV1( channel );
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

    private static class ChannelFactory
    {
        public static ByteChannel create( String host, int port, Config config, Logger logger )
                throws IOException, GeneralSecurityException
        {
            SocketChannel soChannel = SocketChannel.open();
            soChannel.setOption( StandardSocketOptions.SO_REUSEADDR, true );
            soChannel.setOption( StandardSocketOptions.SO_KEEPALIVE, true );
            soChannel.connect( new InetSocketAddress( host, port ) );

            ByteChannel channel = null;

            if( config.isTlsEnabled() )
            {
                channel = new SSLSocketChannel( host, port, soChannel, logger, config.tlsAuthConfig() );
            }
            else
            {
                channel = new AllOrNothingChannel( soChannel );
            }

            if( logger.isTraceEnabled() )
            {
                channel = new LoggingByteChannel( channel, logger );
            }

            return channel;
        }
    }
}
