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
import java.net.SocketTimeoutException;
import java.util.LinkedList;
import java.util.Map;

import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.internal.messaging.AckFailureMessage;
import org.neo4j.driver.v1.internal.messaging.InitMessage;
import org.neo4j.driver.v1.internal.messaging.Message;
import org.neo4j.driver.v1.internal.messaging.RunMessage;
import org.neo4j.driver.v1.internal.spi.Connection;
import org.neo4j.driver.v1.internal.spi.Logger;
import org.neo4j.driver.v1.internal.spi.StreamCollector;
import org.neo4j.driver.v1.internal.messaging.PullAllMessage;

import static org.neo4j.driver.v1.internal.messaging.DiscardAllMessage.DISCARD_ALL;

public class SocketConnection implements Connection
{
    private final Logger logger;

    private int requestCounter = 0;
    private final LinkedList<Message> pendingMessages = new LinkedList<>();
    private final SocketResponseHandler responseHandler;

    private final SocketClient socket;

    public SocketConnection( String host, int port, Config config )
    {
        this.logger = config.logging().getLog( getClass().getName() );

        if( logger.isDebugEnabled() )
        {
            this.responseHandler = new LoggingResponseHandler( logger );
        }
        else
        {
            this.responseHandler = new SocketResponseHandler();
        }

        this.socket = new SocketClient( host, port, config, logger );
        socket.start();
    }

    @Override
    public void init( String clientName )
    {
        // No need to sync, this'll get sent once regular communication starts
        queueMessage( new InitMessage( clientName ) );
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
        int messageId = queueMessage( PullAllMessage.PULL_ALL );
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
        logger.debug( "C: %s", msg );
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
