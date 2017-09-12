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
package org.neo4j.driver.internal.async.inbound;

import io.netty.channel.Channel;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import org.neo4j.driver.internal.handlers.AckFailureResponseHandler;
import org.neo4j.driver.internal.messaging.AckFailureMessage;
import org.neo4j.driver.internal.messaging.MessageHandler;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.internal.util.ErrorUtil;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.Value;

import static java.util.Objects.requireNonNull;

public class InboundMessageDispatcher implements MessageHandler
{
    private final Channel channel;
    private final Queue<ResponseHandler> handlers = new LinkedList<>();
    private final Logger log;

    private Throwable currentError;
    private boolean fatalErrorOccurred;

    public InboundMessageDispatcher( Channel channel, Logging logging )
    {
        this.channel = requireNonNull( channel );
        this.log = logging.getLog( getClass().getSimpleName() );
    }

    public void queue( ResponseHandler handler )
    {
        if ( fatalErrorOccurred )
        {
            handler.onFailure( currentError );
        }
        else
        {
            handlers.add( handler );
        }
    }

    public int queuedHandlersCount()
    {
        return handlers.size();
    }

    @Override
    public void handleInitMessage( String clientNameAndVersion, Map<String,Value> authToken )
    {
        throw new UnsupportedOperationException( "Driver is not supposed to receive INIT message. " +
                                                 "Received INIT with client: '" + clientNameAndVersion + "' " +
                                                 "and auth: " + authToken );
    }

    @Override
    public void handleRunMessage( String statement, Map<String,Value> parameters )
    {
        throw new UnsupportedOperationException( "Driver is not supposed to receive RUN message. " +
                                                 "Received RUN with statement: '" + statement + "' " +
                                                 "and params: " + parameters );
    }

    @Override
    public void handlePullAllMessage()
    {
        throw new UnsupportedOperationException( "Driver is not supposed to receive PULL_ALL message." );
    }

    @Override
    public void handleDiscardAllMessage()
    {
        throw new UnsupportedOperationException( "Driver is not supposed to receive DISCARD_ALL message." );
    }

    @Override
    public void handleResetMessage()
    {
        throw new UnsupportedOperationException( "Driver is not supposed to receive RESET message." );
    }

    @Override
    public void handleAckFailureMessage()
    {
        throw new UnsupportedOperationException( "Driver is not supposed to receive ACK_FAILURE message." );
    }

    @Override
    public void handleSuccessMessage( Map<String,Value> meta )
    {
        ResponseHandler handler = handlers.remove();
        log.debug( "Received SUCCESS message with metadata %s for handler %s", meta, handler );
        handler.onSuccess( meta );
    }

    @Override
    public void handleRecordMessage( Value[] fields )
    {
        if ( log.isDebugEnabled() )
        {
            log.debug( "Received RECORD message with metadata %s", Arrays.toString( fields ) );
        }
        ResponseHandler handler = handlers.peek();
        handler.onRecord( fields );
    }

    @Override
    public void handleFailureMessage( String code, String message )
    {
        log.debug( "Received FAILURE message with code '%s' and message '%s'", code, message );
        currentError = ErrorUtil.newNeo4jError( code, message );

        // queue ACK_FAILURE before notifying the next response handler
        queue( new AckFailureResponseHandler( this ) );
        channel.writeAndFlush( AckFailureMessage.ACK_FAILURE );

        ResponseHandler handler = handlers.remove();
        handler.onFailure( currentError );
    }

    @Override
    public void handleIgnoredMessage()
    {
        ResponseHandler handler = handlers.remove();
        log.debug( "Received IGNORED message for handler %s", handler );
        if ( currentError != null )
        {
            handler.onFailure( currentError );
        }
        else
        {
            log.warn( "Received IGNORED message for handler %s but error is missing", handler );
        }
    }

    public void handleFatalError( Throwable error )
    {
        log.warn( "Fatal error occurred", error );

        currentError = error;
        fatalErrorOccurred = true;

        while ( !handlers.isEmpty() )
        {
            ResponseHandler handler = handlers.remove();
            handler.onFailure( currentError );
        }
    }

    public void clearCurrentError()
    {
        currentError = null;
    }

    Throwable currentError()
    {
        return currentError;
    }
}
