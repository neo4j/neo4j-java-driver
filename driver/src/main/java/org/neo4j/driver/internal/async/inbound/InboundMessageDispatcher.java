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
import org.neo4j.driver.internal.logging.ChannelActivityLogger;
import org.neo4j.driver.internal.messaging.MessageHandler;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.internal.util.ErrorUtil;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.Value;

import static java.util.Objects.requireNonNull;
import static org.neo4j.driver.internal.messaging.AckFailureMessage.ACK_FAILURE;

public class InboundMessageDispatcher implements MessageHandler
{
    private final Channel channel;
    private final Queue<ResponseHandler> handlers = new LinkedList<>();
    private final Logger log;

    private Throwable currentError;
    private boolean fatalErrorOccurred;
    private boolean ackFailureMuted;

    public InboundMessageDispatcher( Channel channel, Logging logging )
    {
        this.channel = requireNonNull( channel );
        this.log = new ChannelActivityLogger( channel, logging, getClass() );
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
        log.debug( "S: SUCCESS %s", meta );
        ResponseHandler handler = handlers.remove();
        handler.onSuccess( meta );
    }

    @Override
    public void handleRecordMessage( Value[] fields )
    {
        if ( log.isDebugEnabled() )
        {
            log.debug( "S: RECORD %s", Arrays.toString( fields ) );
        }
        ResponseHandler handler = handlers.peek();
        handler.onRecord( fields );
    }

    @Override
    public void handleFailureMessage( String code, String message )
    {
        log.debug( "S: FAILURE %s \"%s\"", code, message );

        currentError = ErrorUtil.newNeo4jError( code, message );

        if ( ErrorUtil.isFatal( currentError ) )
        {
            // we should not continue using channel after a fatal error
            // fire error event back to the pipeline and avoid sending ACK_FAILURE
            channel.pipeline().fireExceptionCaught( currentError );
            return;
        }

        // try to write ACK_FAILURE before notifying the next response handler
        ackFailureIfNeeded();

        ResponseHandler handler = handlers.remove();
        handler.onFailure( currentError );
    }

    @Override
    public void handleIgnoredMessage()
    {
        log.debug( "S: IGNORED" );

        ResponseHandler handler = handlers.remove();
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

    public Throwable currentError()
    {
        return currentError;
    }

    public boolean fatalErrorOccurred()
    {
        return fatalErrorOccurred;
    }

    /**
     * Makes this message dispatcher not send ACK_FAILURE in response to FAILURE until it's un-muted using
     * {@link #unMuteAckFailure()}. Muting ACK_FAILURE is needed <b>only</b> when sending RESET message. RESET "jumps"
     * over all queued messages on server and makes them fail. Received failures do not need to be acknowledge because
     * RESET moves server's state machine to READY state.
     * <p>
     * <b>This method is not thread-safe</b> and should only be executed by the event loop thread.
     */
    public void muteAckFailure()
    {
        ackFailureMuted = true;
    }

    /**
     * Makes this message dispatcher send ACK_FAILURE in response to FAILURE. Should be used in combination with
     * {@link #muteAckFailure()} when sending RESET message.
     * <p>
     * <b>This method is not thread-safe</b> and should only be executed by the event loop thread.
     *
     * @throws IllegalStateException if ACK_FAILURE is not muted right now.
     */
    public void unMuteAckFailure()
    {
        if ( !ackFailureMuted )
        {
            throw new IllegalStateException( "Can't un-mute ACK_FAILURE because it's not muted" );
        }
        ackFailureMuted = false;
    }

    private void ackFailureIfNeeded()
    {
        if ( !ackFailureMuted )
        {
            queue( new AckFailureResponseHandler( this ) );
            channel.writeAndFlush( ACK_FAILURE, channel.voidPromise() );
        }
    }
}
