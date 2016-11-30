/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.driver.v1.exceptions.Neo4jException;

/**
 * Internal checked exceptions.
 * This exception should never be visible to a driver user in anyway.
 *
 * The internal error should be casted to a public error in {@link org.neo4j.driver.v1.exceptions} with
 * {@link #publicException()} method call before it reaches to a driver end user.
 */
public abstract class InternalException extends Exception
{
    public InternalException()
    {}

    public InternalException( String msg, Throwable cause )
    {
        super( msg, cause );
    }

    public InternalException( String msg )
    {
        super( msg );
    }

    public abstract Neo4jException publicException();

    public boolean isUnrecoverableError()
    {
        return true;
    }
}
