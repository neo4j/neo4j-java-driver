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

import java.io.IOException;
import java.util.Map;
import java.util.Queue;

import org.neo4j.driver.internal.messaging.MessageHandler;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.Neo4jException;

public class ResponseMessageHandler implements MessageHandler
{
    private final Queue<ResponseHandler> handlers;
    private Throwable lastError;

    public ResponseMessageHandler( Queue<ResponseHandler> handlers )
    {
        this.handlers = handlers;
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
        ResponseHandler handler = handlers.remove();
        handler.onSuccess( meta );
    }

    @Override
    public void handleRecordMessage( Value[] fields ) throws IOException
    {
        ResponseHandler handler = handlers.peek();
        handler.onRecord( fields );
    }

    @Override
    public void handleFailureMessage( String code, String message ) throws IOException
    {
        Neo4jException error = ErrorCreator.create( code, message );
        handleError( error, false );
    }

    @Override
    public void handleIgnoredMessage() throws IOException
    {
        ResponseHandler handler = handlers.poll();
        if ( handler != null && lastError != null )
        {
            handler.onFailure( lastError );
        }
    }

    public void handleFatalError( Throwable error )
    {
        System.out.println( "--- FATAL ERROR" );
        handleError( error, true );
    }

    public void handleError( Throwable error, boolean fatal )
    {
        lastError = error;

        if ( fatal )
        {
            while ( !handlers.isEmpty() )
            {
                ResponseHandler handler = handlers.remove();
                handler.onFailure( lastError );
            }
        }
        else
        {
            ResponseHandler handler = handlers.poll();
            if ( handler != null )
            {
                handler.onFailure( lastError );
            }
        }
    }
}
