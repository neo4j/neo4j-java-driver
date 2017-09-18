/*
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
package org.neo4j.driver.internal.net;

import java.io.IOException;
import java.net.ConnectException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.util.Queue;

import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.util.BytePrinter;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;

import static java.lang.String.format;
import static java.nio.ByteOrder.BIG_ENDIAN;
import static org.neo4j.driver.internal.util.ServerVersion.v3_2_0;
import static org.neo4j.driver.internal.util.ServerVersion.version;

public class SocketClient
{
    private static final int MAGIC_PREAMBLE = 0x6060B017;
    private static final int VERSION1 = 1;
    private static final int HTTP = 1213486160;//== 0x48545450 == "HTTP"
    private static final int NO_VERSION = 0;
    private static final int[] SUPPORTED_VERSIONS = new int[]{VERSION1, NO_VERSION, NO_VERSION, NO_VERSION};

    private final BoltServerAddress address;
    private final SecurityPlan securityPlan;
    private final int timeoutMillis;
    private final Logger logger;

    private SocketProtocol protocol;
    private MessageFormat.Reader reader;
    private MessageFormat.Writer writer;

    private ByteChannel channel;

    public SocketClient( BoltServerAddress address, SecurityPlan securityPlan, int timeoutMillis, Logger logger )
    {
        this.address = address;
        this.securityPlan = securityPlan;
        this.timeoutMillis = timeoutMillis;
        this.logger = logger;
        this.channel = null;
    }

    void setChannel( ByteChannel channel )
    {
        this.channel = channel;
    }

    void blockingRead( ByteBuffer buf ) throws IOException
    {
        while(buf.hasRemaining())
        {
            if (channel.read( buf ) < 0)
            {
                try
                {
                    channel.close();
                }
                catch ( IOException e )
                {
                    // best effort
                }
                String bufStr = BytePrinter.hex( buf ).trim();
                throw new ServiceUnavailableException( format(
                        "Connection terminated while receiving data. This can happen due to network " +
                        "instabilities, or due to restarts of the database. Expected %s bytes, received %s.",
                        buf.limit(), bufStr.isEmpty() ? "none" : bufStr ) );
            }
        }
    }

    void blockingWrite( ByteBuffer buf ) throws IOException
    {
        while(buf.hasRemaining())
        {
            if (channel.write( buf ) < 0)
            {
                try
                {
                    channel.close();
                }
                catch ( IOException e )
                {
                    // best effort
                }
                String bufStr = BytePrinter.hex( buf ).trim();
                throw new ServiceUnavailableException( format(
                        "Connection terminated while sending data. This can happen due to network " +
                        "instabilities, or due to restarts of the database. Expected %s bytes, wrote %s.",
                        buf.limit(), bufStr.isEmpty() ? "none" :bufStr ) );
            }
        }
    }

    public void start()
    {
        try
        {
            logger.debug( "~~ [CONNECT] %s", address );
            if( channel == null )
            {
                setChannel( ChannelFactory.create( address, securityPlan, timeoutMillis, logger ) );
            }
            setProtocol( negotiateProtocol() );
        }
        catch ( ConnectException e )
        {
            throw new ServiceUnavailableException( format(
                    "Unable to connect to %s, ensure the database is running and that there is a " +
                    "working network connection to it.", address ), e );
        }
        catch ( IOException e )
        {
            throw new ServiceUnavailableException( "Unable to process request: " + e.getMessage(), e );
        }
    }

    public void updateProtocol( String serverVersion )
    {
        if( version( serverVersion ).lessThan( v3_2_0 ) )
        {
            setProtocol( SocketProtocolV1.createWithoutByteArraySupport( channel ) );
        }
    }

    private void setProtocol( SocketProtocol protocol )
    {
        this.protocol = protocol;
        this.reader = protocol.reader();
        this.writer = protocol.writer();
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
        while ( handler.handlersWaiting() > 0 )
        {
            receiveOne( handler );
        }
    }

    public void receiveOne( SocketResponseHandler handler ) throws IOException
    {
        reader.read( handler );

        // Stop immediately if bolt protocol error happened on the server
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
                channel.close();
                setChannel( null );
                logger.debug( "~~ [DISCONNECT]" );
            }
        }
        catch ( IOException e )
        {
            //noinspection StatementWithEmptyBody
            if ( e.getMessage().equals( "An existing connection was forcibly closed by the remote host" ) )
            {
                // Swallow this exception as it is caused by connection already closed by server
            }
            else
            {
                logger.error( "Unable to close socket connection properly", e );
            }
        }
    }

    public boolean isOpen()
    {
        return channel != null && channel.isOpen();
    }

    private SocketProtocol negotiateProtocol() throws IOException
    {
        //Propose protocol versions
        ByteBuffer buf = ByteBuffer.allocate( 5 * 4 ).order( BIG_ENDIAN );
        logger.debug( "C: [HANDSHAKE] 0x6060B017" );
        buf.putInt( MAGIC_PREAMBLE );
        logger.debug( "C: [HANDSHAKE] [1, 0, 0, 0]" );
        for ( int version : SUPPORTED_VERSIONS )
        {
            buf.putInt( version );
        }
        buf.flip();

        blockingWrite( buf );

        // Read (blocking) back the servers choice
        buf.clear();
        buf.limit( 4 );
        try
        {
            blockingRead( buf );
        }
        catch ( ClientException e )
        {
            if ( buf.position() == 0 ) // failed to read any bytes
            {
                throw new ClientException( format(
                        "Failed to establish connection with server. Make sure that you have a server with bolt " +
                        "enabled on %s", address ) );
            }
            else
            {
                throw e;
            }
        }
        // Choose protocol, or fail
        buf.flip();
        final int proposal = buf.getInt();
        switch ( proposal )
        {
        case VERSION1:
            logger.debug( "S: [HANDSHAKE] -> 1" );
            return SocketProtocolV1.create( channel );
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

    public BoltServerAddress address()
    {
        return address;
    }
}
