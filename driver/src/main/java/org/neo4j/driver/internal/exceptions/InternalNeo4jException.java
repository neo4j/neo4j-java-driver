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
package org.neo4j.driver.internal.exceptions;

import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.DatabaseException;
import org.neo4j.driver.v1.exceptions.Neo4jException;
import org.neo4j.driver.v1.exceptions.TransientException;

/**
 * This exception represents a {@link org.neo4j.driver.v1.exceptions.Neo4jException} that is passed from the server.
 *
 * It wraps around the {@link org.neo4j.driver.v1.exceptions.Neo4jException} to make it a checked exception for
 * keeping track of it when it is passed internally.
 * The exception should be mapped back to {@link org.neo4j.driver.v1.exceptions.Neo4jException} before reaching a user.
 */
public class InternalNeo4jException extends InternalException
{
    private final Neo4jException error;

    public InternalNeo4jException( String code, String message )
    {
        String[] parts = code.split( "\\." );
        String classification = parts[1];
        switch ( classification )
        {
        case "ClientError":
            error = new ClientException( code, message );
            break;
        case "TransientError":
            error = new TransientException( code, message );
            break;
        default:
            error = new DatabaseException( code, message );
            break;
        }
    }

    public InternalNeo4jException( Neo4jException error )
    {
        this.error = error;
    }

    @Override
    public Neo4jException publicException()
    {
        return error;
    }

    public boolean isProtocolViolationError()
    {
        return error != null && error.code().startsWith( "Neo.ClientError.Request" );
    }

    private boolean isDatabaseError()
    {
        return error != null && error instanceof DatabaseException;
    }

    @Override
    public boolean isUnrecoverableError()
    {
        return isDatabaseError() || isProtocolViolationError();
    }
}
