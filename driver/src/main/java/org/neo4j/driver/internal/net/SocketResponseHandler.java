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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.neo4j.driver.internal.messaging.MessageHandler;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.AuthenticationException;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.DatabaseException;
import org.neo4j.driver.v1.exceptions.Neo4jException;
import org.neo4j.driver.v1.exceptions.TransientException;

public class SocketResponseHandler implements MessageHandler
{
    private final Queue<ResponseHandler> handlers = new ConcurrentLinkedQueue<>();

    /** If a failure occurs, the error gets stored here */
    private Neo4jException error;

    @Override
    public void handleRecordMessage( Value[] fields )
    {
        ResponseHandler handler = handlers.element();
        handler.onRecord( fields );
    }

    @Override
    public void handleFailureMessage( String code, String message )
    {
        ResponseHandler handler = handlers.remove();
        String[] parts = code.split( "\\." );
        String classification = parts[1];
        switch ( classification )
        {
            case "ClientError":
                if( code.equalsIgnoreCase( "Neo.ClientError.Security.Unauthorized" ) )
                {
                    error = new AuthenticationException( code, message );
                }
                else
                {
                    error = new ClientException( code, message );
                }
                break;
            case "TransientError":
                error = new TransientException( code, message );
                break;
            default:
                error = new DatabaseException( code, message );
                break;
        }
        if ( handler != null )
        {
            handler.onFailure( error );
        }
    }

    @Override
    public void handleSuccessMessage( Map<String,Value> meta )
    {
        ResponseHandler handler = handlers.remove();
        handler.onSuccess( meta );
    }

    @Override
    public void handleIgnoredMessage()
    {
        // todo: this is really fucking strange!
        // todo: IGNORED used to mark handler as completed, which is needed for ISResult
        ResponseHandler handler = handlers.remove();
        handler.onSuccess( Collections.<String,Value>emptyMap() );
    }

    @Override
    public void handleDiscardAllMessage()
    {
    }

    @Override
    public void handleResetMessage()
    {
    }

    @Override
    public void handleAckFailureMessage()
    {
    }

    @Override
    public void handlePullAllMessage()
    {
    }

    @Override
    public void handleInitMessage( String clientNameAndVersion, Map<String,Value> authToken )
    {
    }

    @Override
    public void handleRunMessage( String statement, Map<String,Value> parameters )
    {
    }

    public void appendResponseHandler( ResponseHandler handler )
    {
        Objects.requireNonNull( handler );
        handlers.add( handler );
    }

    public int handlersWaiting()
    {
        return handlers.size();
    }

    public boolean protocolViolationErrorOccurred()
    {
        return error != null && error.code().startsWith( "Neo.ClientError.Request" );
    }

    public boolean serverFailureOccurred()
    {
        return error != null;
    }

    public Neo4jException serverFailure()
    {
        return error;
    }

    public void clearError()
    {
        error = null;
    }
}
