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

import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.Neo4jException;

/**
 * Any error that happens due to mis-use of this driver, such as connecting to a port that does not support Bolt
 * protocol, or connecting to a server whose version is not compatible with this driver.
 *
 * This error normally requires a restart of a driver and change config correctly to recover.
 *
 * <p>
 * This error is very similar to {@link UnsupportedOperationException} which this error is a checked error.
 * </p>
 */
public class InvalidOperationException extends InternalException
{
    public InvalidOperationException( String msg, Throwable cause )
    {
        super( msg, cause );
    }

    public InvalidOperationException( String msg )
    {
        super( msg );
    }

    public InvalidOperationException( String msg, InternalException error )
    {
        super( msg, error.publicException() );
    }

    @Override
    public Neo4jException publicException()
    {
        return new ClientException( this.getMessage(), this.getCause() );
    }
}
