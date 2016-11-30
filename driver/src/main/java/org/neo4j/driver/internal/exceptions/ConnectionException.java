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
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;

/**
 * This error indicates that a socket connection is broken due to errors.
 *
 * When seeing this error, the socket connection should be closed.
 * While the driver should recover from this error, after the network is good for connection again.
 *
 * For a routing client, the client should consider to remove the server from routing table,
 * as the server probably no long available or not ready/authed to serve the client.
 */
public class ConnectionException extends InternalException
{
    public ConnectionException( String msg, Throwable cause )
    {
        super( msg, cause );
    }

    public ConnectionException( String msg, InternalException error )
    {
        super( msg, error.getCause() );
    }

    public ConnectionException( String msg )
    {
        super( msg );
    }

    @Override
    public Neo4jException publicException()
    {
        return new ServiceUnavailableException( getMessage(), getCause() );
    }
}
