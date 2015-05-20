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
import java.net.SocketTimeoutException;
import java.util.LinkedList;
import java.util.Map;

import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.logging.DevNullLogger;
import org.neo4j.driver.internal.messaging.AckFailureMessage;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.RunMessage;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.Logger;
import org.neo4j.driver.internal.spi.Logging;
import org.neo4j.driver.internal.spi.StreamCollector;

import static org.neo4j.driver.internal.messaging.DiscardAllMessage.DISCARD_ALL;
import static org.neo4j.driver.internal.messaging.PullAllMessage.PULL_ALL;

public class SocketConnection implements Connection
{
    private final Logging logging;
    private final Logger logger;

    private int requestCounter = 0;
    private final LinkedList<Message> pendingMessages = new LinkedList<>();
    private final SocketResponseHandler responseHandler;

    private final SocketClient socket;

    public SocketConnection( String host, int port, Logging logging )
    {
        this.logging = logging;
        this.logger = logging != null ? logging.getLogging( getClass().getName() ) : new DevNullLogger();
        this.responseHandler = new SocketResponseHandler( logger );
        this.socket = new SocketClient( host, port, logger );
        socket.start();
    }

    @Override
    public void run( String statement, Map<String,Value> parameters, StreamCollector collector )
    {
        int messageId = queueMessage( new RunMessage( statement, parameters ) );
        if ( collector != null )
        {
            responseHandler.registerResultCollector( messageId, collector );
        }
    }

    @Override
    public void discardAll()
    {
        queueMessage( DISCARD_ALL );
    }

    @Override
    public void pullAll( StreamCollector collector )
    {
        int messageId = queueMessage( PULL_ALL );
        responseHandler.registerResultCollector( messageId, collector );
    }

    @Override
    public void sync()
    {
        if ( pendingMessages.size() == 0 )
        {
            return;
        }

        try
        {
            socket.send( pendingMessages, responseHandler );
            requestCounter = 0; // Reset once we've sent all pending request to avoid wrap-around handling
            pendingMessages.clear();
            if ( responseHandler.serverFailureOccurred() )
            {
                // Its enough to simply add the ack message to the outbound queue, it'll get sent
                // off as the first message the next time we need to sync with the database.
                queueMessage( new AckFailureMessage() );
                throw responseHandler.serverFailure();
            }
        }
        catch ( IOException e )
        {
            requestCounter = 0; // Reset once we've sent all pending request to avoid wrap-around handling
            pendingMessages.clear();
            String message = e.getMessage();
            if ( message == null )
            {
                throw new ClientException( "Unable to read response from server: " + e.getClass().getSimpleName(), e );
            }
            else if ( e instanceof SocketTimeoutException )
            {
                throw new ClientException( "Server did not reply within the network timeout limit.", e );
            }
            else
            {
                throw new ClientException( "Unable to read response from server: " + message, e );
            }
        }
        finally
        {
            responseHandler.clear();
        }

    }

    private int queueMessage( Message msg )
    {
        int messageId = nextRequestId();
        pendingMessages.add( msg );
        logger.debug( msg.toString() );
        return messageId;
    }

    @Override
    public void close()
    {
        socket.stop();
    }

    private int nextRequestId()
    {
        return (requestCounter++);
    }
}
