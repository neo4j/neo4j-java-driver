/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.driver.internal.util;

import io.netty.util.internal.PlatformDependent;

import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import org.neo4j.driver.exceptions.AuthenticationException;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.DatabaseException;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.TransientException;

public final class ErrorUtil
{
    private ErrorUtil()
    {
    }

    public static ServiceUnavailableException newConnectionTerminatedError( String reason )
    {
        if ( reason == null )
        {
            return newConnectionTerminatedError();
        }
        return new ServiceUnavailableException( "Connection to the database terminated. " + reason );
    }

    public static ServiceUnavailableException newConnectionTerminatedError()
    {
        return new ServiceUnavailableException( "Connection to the database terminated. " +
                                                "This can happen due to network instabilities, " +
                                                "or due to restarts of the database" );
    }

    public static Neo4jException newNeo4jError( String code, String message )
    {
        String classification = extractClassification( code );
        switch ( classification )
        {
        case "ClientError":
            if ( code.equalsIgnoreCase( "Neo.ClientError.Security.Unauthorized" ) )
            {
                return new AuthenticationException( code, message );
            }
            else
            {
                return new ClientException( code, message );
            }
        case "TransientError":
            return new TransientException( code, message );
        default:
            return new DatabaseException( code, message );
        }
    }

    public static boolean isFatal( Throwable error )
    {
        if ( error instanceof Neo4jException )
        {
            if ( isProtocolViolationError( ((Neo4jException) error) ) )
            {
                return true;
            }

            if ( isClientOrTransientError( ((Neo4jException) error) ) )
            {
                return false;
            }
        }
        return true;
    }

    public static void rethrowAsyncException( ExecutionException e )
    {
        Throwable error = e.getCause();

        InternalExceptionCause internalCause = new InternalExceptionCause( error.getStackTrace() );
        error.addSuppressed( internalCause );

        StackTraceElement[] currentStackTrace = Stream.of( Thread.currentThread().getStackTrace() )
                .skip( 2 ) // do not include Thread.currentThread() and this method in the stacktrace
                .toArray( StackTraceElement[]::new );
        error.setStackTrace( currentStackTrace );

        PlatformDependent.throwException( error );
    }

    private static boolean isProtocolViolationError( Neo4jException error )
    {
        String errorCode = error.code();
        return errorCode != null && errorCode.startsWith( "Neo.ClientError.Request" );
    }

    private static boolean isClientOrTransientError( Neo4jException error )
    {
        String errorCode = error.code();
        return errorCode != null && (errorCode.contains( "ClientError" ) || errorCode.contains( "TransientError" ));
    }

    private static String extractClassification( String code )
    {
        String[] parts = code.split( "\\." );
        if ( parts.length < 2 )
        {
            return "";
        }
        return parts[1];
    }

    /**
     * Exception which is merely a holder of an async stacktrace, which is not the primary stacktrace users are interested in.
     * Used for blocking API calls that block on async API calls.
     */
    private static class InternalExceptionCause extends RuntimeException
    {
        InternalExceptionCause( StackTraceElement[] stackTrace )
        {
            setStackTrace( stackTrace );
        }

        @Override
        public synchronized Throwable fillInStackTrace()
        {
            // no need to fill in the stack trace
            // this exception just uses the given stack trace
            return this;
        }
    }
}
