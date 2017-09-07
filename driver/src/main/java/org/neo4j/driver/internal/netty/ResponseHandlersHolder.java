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
package org.neo4j.driver.internal.netty;

import io.netty.channel.Channel;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import org.neo4j.driver.internal.handlers.AckFailureResponseHandler;
import org.neo4j.driver.internal.messaging.AckFailureMessage;
import org.neo4j.driver.internal.messaging.MessageHandler;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.internal.util.ErrorUtil;
import org.neo4j.driver.v1.Value;

public class ResponseHandlersHolder implements MessageHandler
{
    private final Channel channel;
    private final Queue<ResponseHandler> handlers = new LinkedList<>();

    private Throwable currentError;
    private boolean fatalErrorOccurred;

    public ResponseHandlersHolder( Channel channel )
    {
        this.channel = channel;
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

    @Override
    public void handleInitMessage( String clientNameAndVersion, Map<String,Value> authToken ) throws IOException
    {
        throw new UnsupportedOperationException( "Driver is not supposed to receive INIT" );
    }

    @Override
    public void handleRunMessage( String statement, Map<String,Value> parameters ) throws IOException
    {
        throw new UnsupportedOperationException( "Driver is not supposed to receive RUN" );
    }

    @Override
    public void handlePullAllMessage() throws IOException
    {
        throw new UnsupportedOperationException( "Driver is not supposed to receive PULL_ALL" );
    }

    @Override
    public void handleDiscardAllMessage() throws IOException
    {
        throw new UnsupportedOperationException( "Driver is not supposed to receive DISCARD_ALL" );
    }

    @Override
    public void handleResetMessage() throws IOException
    {
        throw new UnsupportedOperationException( "Driver is not supposed to receive RESET" );
    }

    @Override
    public void handleAckFailureMessage() throws IOException
    {
        throw new UnsupportedOperationException( "Driver is not supposed to receive ACK_FAILURE" );
    }

    @Override
    public void handleSuccessMessage( Map<String,Value> meta ) throws IOException
    {
//        System.out.println( "Received SUCCESS " + meta );
        ResponseHandler handler = handlers.remove();
        handler.onSuccess( meta );
    }

    @Override
    public void handleRecordMessage( Value[] fields ) throws IOException
    {
//        System.out.println( "Received RECORD " + Arrays.toString( fields ) );
        ResponseHandler handler = handlers.peek();
        handler.onRecord( fields );
    }

    @Override
    public void handleFailureMessage( String code, String message ) throws IOException
    {
//        System.out.println( "Received FAILURE " + code + " " + message );
        currentError = ErrorUtil.newNeo4jError( code, message );

        // queue ACK_FAILURE before notifying the next response handler
        queue( new AckFailureResponseHandler( this ) );
        channel.writeAndFlush( AckFailureMessage.ACK_FAILURE );

        ResponseHandler handler = handlers.remove();
        handler.onFailure( currentError );
    }

    @Override
    public void handleIgnoredMessage() throws IOException
    {
        ResponseHandler handler = handlers.remove();
        if ( currentError != null )
        {
            handler.onFailure( currentError );
        }
    }

    public void handleFatalError( Throwable error )
    {
        System.out.println( "--- FATAL ERROR" );

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
}
