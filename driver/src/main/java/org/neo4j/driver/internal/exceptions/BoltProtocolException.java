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
 * Any error that related to some unexpected use of Bolt protocol, including failed to encode/decode between bolt
 * message and binary, unexpected message replied etc.
 */
public class BoltProtocolException extends InternalException
{
    public BoltProtocolException( String msg )
    {
        super( msg );
    }

    public BoltProtocolException( String msg, Throwable error )
    {
        super( msg, error );
    }

    public BoltProtocolException( String msg, InternalException error )
    {
        super( msg, error.publicException() );
    }

    @Override
    public Neo4jException publicException()
    {
        return new ClientException( getMessage(), getCause() );
    }
}
