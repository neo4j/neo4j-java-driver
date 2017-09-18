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
package org.neo4j.driver.internal.util;

import org.neo4j.driver.v1.exceptions.AuthenticationException;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.DatabaseException;
import org.neo4j.driver.v1.exceptions.Neo4jException;
import org.neo4j.driver.v1.exceptions.TransientException;

public final class ErrorUtil
{
    private ErrorUtil()
    {
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

    public static boolean isRecoverable( Throwable error )
    {
        if ( error instanceof Neo4jException )
        {
            if ( isProtocolViolationError( ((Neo4jException) error) ) )
            {
                return false;
            }

            if ( isClientOrTransientError( ((Neo4jException) error) ) )
            {
                return true;
            }
        }
        return false;
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
        return parts[1];
    }
}
