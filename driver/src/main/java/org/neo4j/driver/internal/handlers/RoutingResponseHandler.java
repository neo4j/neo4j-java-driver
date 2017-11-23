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
package org.neo4j.driver.internal.handlers;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionException;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.RoutingErrorHandler;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;
import org.neo4j.driver.v1.exceptions.TransientException;

import static java.lang.String.format;

public class RoutingResponseHandler implements ResponseHandler
{
    private final ResponseHandler delegate;
    private final BoltServerAddress address;
    private final AccessMode accessMode;
    private final RoutingErrorHandler errorHandler;

    public RoutingResponseHandler( ResponseHandler delegate, BoltServerAddress address, AccessMode accessMode,
            RoutingErrorHandler errorHandler )
    {
        this.delegate = delegate;
        this.address = address;
        this.accessMode = accessMode;
        this.errorHandler = errorHandler;
    }

    @Override
    public void onSuccess( Map<String,Value> metadata )
    {
        delegate.onSuccess( metadata );
    }

    @Override
    public void onFailure( Throwable error )
    {
        Throwable newError = handledError( error );
        delegate.onFailure( newError );
    }

    @Override
    public void onRecord( Value[] fields )
    {
        delegate.onRecord( fields );
    }

    private Throwable handledError( Throwable receivedError )
    {
        Throwable error = Futures.completionErrorCause( receivedError );
        if ( error instanceof CompletionException )
        {
            error = error.getCause();
        }

        if ( error instanceof ServiceUnavailableException )
        {
            return handledServiceUnavailableException( ((ServiceUnavailableException) error) );
        }
        else if ( error instanceof ClientException )
        {
            return handledClientException( ((ClientException) error) );
        }
        else if ( error instanceof TransientException )
        {
            return handledTransientException( ((TransientException) error) );
        }
        else
        {
            return error;
        }
    }

    private Throwable handledServiceUnavailableException( ServiceUnavailableException e )
    {
        errorHandler.onConnectionFailure( address );
        return new SessionExpiredException( format( "Server at %s is no longer available", address ), e );
    }

    private Throwable handledTransientException( TransientException e )
    {
        String errorCode = e.code();
        if ( Objects.equals( errorCode, "Neo.TransientError.General.DatabaseUnavailable" ) )
        {
            errorHandler.onConnectionFailure( address );
        }
        return e;
    }

    private Throwable handledClientException( ClientException e )
    {
        if ( isFailureToWrite( e ) )
        {
            // The server is unaware of the session mode, so we have to implement this logic in the driver.
            // In the future, we might be able to move this logic to the server.
            switch ( accessMode )
            {
            case READ:
                return new ClientException( "Write queries cannot be performed in READ access mode." );
            case WRITE:
                errorHandler.onWriteFailure( address );
                return new SessionExpiredException( format( "Server at %s no longer accepts writes", address ) );
            default:
                throw new IllegalArgumentException( accessMode + " not supported." );
            }
        }
        return e;
    }

    private static boolean isFailureToWrite( ClientException e )
    {
        String errorCode = e.code();
        return Objects.equals( errorCode, "Neo.ClientError.Cluster.NotALeader" ) ||
               Objects.equals( errorCode, "Neo.ClientError.General.ForbiddenOnReadOnlyDatabase" );
    }
}
