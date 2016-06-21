/**
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
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.SocketChannel;
import java.security.GeneralSecurityException;
import java.util.Queue;

import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.spi.Logger;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.exceptions.ClientException;

import static java.nio.ByteOrder.BIG_ENDIAN;
import static org.neo4j.driver.internal.connector.socket.SocketUtils.blockingRead;
import static org.neo4j.driver.internal.connector.socket.SocketUtils.blockingWrite;
import static org.neo4j.driver.internal.util.AddressUtil.isLocalHost;

public class SocketClient
{
    private static final int MAGIC_PREAMBLE = 0x6060B017;
    private static final int VERSION1 = 1;
    private static final int HTTP = 1213486160;//== 0x48545450 == "HTTP"
    private static final int NO_VERSION = 0;
    private static final int[] SUPPORTED_VERSIONS = new int[]{VERSION1, NO_VERSION, NO_VERSION, NO_VERSION};

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

    public void send( Queue<Message> messages ) throws IOException
    {
        int messageCount = 0;
        while ( true )
        {
            Message message = messages.poll();
            if ( message == null )
            {
                break;
            }
            else
            {
                logger.debug( "C: %s", message );
                writer.write( message );
                messageCount += 1;
            }
        }
        if ( messageCount > 0 )
        {
            writer.flush();
        }
    }

    public void receiveAll( SocketResponseHandler handler ) throws IOException
    {
        // Wait until all pending requests have been replied to
        while ( handler.collectorsWaiting() > 0 )
        {
            receiveOne( handler );
        }
    }

    public void receiveOne( SocketResponseHandler handler ) throws IOException
    {
        reader.read( handler );

        // TODO: all the errors come from the following trace should result in the termination of this channel
        // https://github.com/neo4j/neo4j/blob/3
        // .0/community/bolt/src/main/java/org/neo4j/bolt/v1/transport/BoltProtocolV1.java#L86
        if ( handler.protocolViolationErrorOccurred() )
        {
            stop();
            throw handler.serverFailure();
        }
    }

    public void stop()
    {
        try
        {
            if ( channel != null )
            {
                logger.debug( "~~ [CLOSE]" );
                channel.close();
                channel = null;
            }
        }
        catch ( IOException e )
        {
            if ( e.getMessage().equals( "An existing connection was forcibly closed by the remote host" ) )
            {
                // Swallow this exception as it is caused by connection already closed by server
            }
            else
            {
                throw new ClientException( "Unable to close socket connection properly." + e.getMessage(), e );
            }
        }
    }

    public boolean isOpen()
    {
        return channel != null && channel.isOpen();
    }

    private SocketProtocol negotiateProtocol() throws IOException
    {
        logger.debug( "~~ [HANDSHAKE] [0x6060B017, 1, 0, 0, 0]." );
        //Propose protocol versions
        ByteBuffer buf = ByteBuffer.allocateDirect( 5 * 4 ).order( BIG_ENDIAN );
        buf.putInt( MAGIC_PREAMBLE );
        for ( int version : SUPPORTED_VERSIONS )
        {
            buf.putInt( version );
        }
        buf.flip();

        //Do a blocking write
       blockingWrite(channel, buf);

        // Read (blocking) back the servers choice
        buf.clear();
        buf.limit( 4 );
        blockingRead(channel, buf);

        // Choose protocol, or fail
        buf.flip();
        final int proposal = buf.getInt();
        switch ( proposal )
        {
        case VERSION1:
            logger.debug( "~~ [HANDSHAKE] 1" );
            return new SocketProtocolV1( channel );
        case NO_VERSION:
            throw new ClientException( "The server does not support any of the protocol versions supported by " +
                                       "this driver. Ensure that you are using driver and server versions that " +
                                       "are compatible with one another." );
        case HTTP:
            throw new ClientException(
                    "Server responded HTTP. Make sure you are not trying to connect to the http endpoint " +
                    "(HTTP defaults to port 7474 whereas BOLT defaults to port 7687)" );
        default:
            throw new ClientException( "Protocol error, server suggested unexpected protocol version: " +
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

            ByteChannel channel;

            switch ( config.encryptionLevel() )
            {
            case REQUIRED:
            {
                channel = new TLSSocketChannel( host, port, soChannel, logger, config.trustStrategy() );
                break;
            }
            case REQUIRED_NON_LOCAL:
            {
                if ( isLocalHost( host ) )
                {
                    channel = soChannel;
                }
                else
                {
                    channel = new TLSSocketChannel( host, port, soChannel, logger, config.trustStrategy() );
                }
                break;
            }
            case NONE:
            {
                channel = soChannel;
                break;
            }
            default:
                throw new ClientException( "Unknown TLS Level: " + config.encryptionLevel() );
            }

            if ( logger.isTraceEnabled() )
            {
                channel = new LoggingByteChannel( channel, logger );
            }

            return channel;
        }
    }
}
