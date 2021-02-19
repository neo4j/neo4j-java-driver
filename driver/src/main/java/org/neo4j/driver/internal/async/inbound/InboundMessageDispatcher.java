/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.internal.handlers.ResetResponseHandler;
import org.neo4j.driver.internal.logging.ChannelActivityLogger;
import org.neo4j.driver.internal.messaging.ResponseMessageHandler;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.internal.util.ErrorUtil;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ClientException;

import static java.util.Objects.requireNonNull;
import static org.neo4j.driver.internal.messaging.request.ResetMessage.RESET;
import static org.neo4j.driver.internal.util.ErrorUtil.addSuppressed;

public class InboundMessageDispatcher implements ResponseMessageHandler
{
    private final Channel channel;
    private final Queue<ResponseHandler> handlers = new LinkedList<>();
    private final Logger log;

    private volatile boolean gracefullyClosed;
    private Throwable currentError;
    private boolean fatalErrorOccurred;

    private ResponseHandler autoReadManagingHandler;

    public InboundMessageDispatcher( Channel channel, Logging logging )
    {
        this.channel = requireNonNull( channel );
        this.log = new ChannelActivityLogger( channel, logging, getClass() );
    }

    public void enqueue( ResponseHandler handler )
    {
        if ( fatalErrorOccurred )
        {
            handler.onFailure( currentError );
        }
        else
        {
            handlers.add( handler );
            updateAutoReadManagingHandlerIfNeeded( handler );
        }
    }

    public int queuedHandlersCount()
    {
        return handlers.size();
    }

    @Override
    public void handleSuccessMessage( Map<String,Value> meta )
    {
        log.debug( "S: SUCCESS %s", meta );
        ResponseHandler handler = removeHandler();
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
        if ( handler == null )
        {
            throw new IllegalStateException( "No handler exists to handle RECORD message with fields: " + Arrays.toString( fields ) );
        }
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
            // fire error event back to the pipeline and avoid sending RESET
            channel.pipeline().fireExceptionCaught( currentError );
            return;
        }

        // write a RESET to "acknowledge" the failure
        enqueue( new ResetResponseHandler( this ) );
        channel.writeAndFlush( RESET, channel.voidPromise() );

        ResponseHandler handler = removeHandler();
        handler.onFailure( currentError );
    }

    @Override
    public void handleIgnoredMessage()
    {
        log.debug( "S: IGNORED" );

        ResponseHandler handler = removeHandler();

        Throwable error;
        if ( currentError != null )
        {
            error = currentError;
        }
        else
        {
            log.warn( "Received IGNORED message for handler %s but error is missing and RESET is not in progress. " +
                      "Current handlers %s", handler, handlers );

            error = new ClientException( "Database ignored the request" );
        }
        handler.onFailure( error );
    }

    public void handleChannelInactive( Throwable cause )
    {
        // report issue if the connection has not been terminated as a result of a graceful shutdown request from its
        // parent pool
        if ( !gracefullyClosed )
        {
            handleChannelError( cause );
        }
        else
        {
            channel.close();
        }
    }

    public void handleChannelError( Throwable error )
    {
        if ( currentError != null )
        {
            // we already have an error, this new error probably is caused by the existing one, thus we chain the new error on this current error
            addSuppressed( currentError, error );
        }
        else
        {
            currentError = error;
        }
        fatalErrorOccurred = true;

        while ( !handlers.isEmpty() )
        {
            ResponseHandler handler = removeHandler();
            handler.onFailure( currentError );
        }

        log.debug( "Closing channel because of a failure '%s'", error );
        channel.close();
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

    public void prepareToCloseChannel( )
    {
        this.gracefullyClosed = true;
    }

    /**
     * <b>Visible for testing</b>
     */
    ResponseHandler autoReadManagingHandler()
    {
        return autoReadManagingHandler;
    }

    private ResponseHandler removeHandler()
    {
        ResponseHandler handler = handlers.remove();
        if ( handler == autoReadManagingHandler )
        {
            // the auto-read managing handler is being removed
            // make sure this dispatcher does not hold on to a removed handler
            updateAutoReadManagingHandler( null );
        }
        return handler;
    }

    private void updateAutoReadManagingHandlerIfNeeded( ResponseHandler handler )
    {
        if ( handler.canManageAutoRead() )
        {
            updateAutoReadManagingHandler( handler );
        }
    }

    private void updateAutoReadManagingHandler( ResponseHandler newHandler )
    {
        if ( autoReadManagingHandler != null )
        {
            // there already exists a handler that manages channel's auto-read
            // make it stop because new managing handler is being added and there should only be a single such handler
            autoReadManagingHandler.disableAutoReadManagement();
            // restore the default value of auto-read
            channel.config().setAutoRead( true );
        }
        autoReadManagingHandler = newHandler;
    }
}
