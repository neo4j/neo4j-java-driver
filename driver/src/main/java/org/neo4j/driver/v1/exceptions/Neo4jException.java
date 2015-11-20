/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.driver.v1.exceptions;

/**
 * This is the base class for all Neo4j exceptions.
 */
public abstract class Neo4jException extends RuntimeException
{
    private final String code;

    public Neo4jException( String message )
    {
        this( "N/A", message );
    }

    public Neo4jException( String message, Throwable cause )
    {
        this( "N/A", message, cause );
    }

    public Neo4jException( String code, String message )
    {
        this( code, message, null );
    }

    public Neo4jException( String code, String message, Throwable cause )
    {
        super( message, cause );
        this.code = code;
    }

    /**
     * Access the standard Neo4j Status Code for this exception, you can use this to refer to the Neo4j manual for
     * details on what caused the error.
     *
     * @return the Neo4j Status Code for this exception, or 'N/A' if none is available
     */
    public String neo4jErrorCode()
    {
        return code;
    }
}
