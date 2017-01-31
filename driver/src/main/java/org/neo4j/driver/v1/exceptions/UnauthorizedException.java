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

package org.neo4j.driver.v1.exceptions;

/**
 * Failed to establish connection between server and driver due to authentication errors.
 * This error could be the error that the driver failed to auth to the server by providing bad credentials or
 * the error that server failed to auth to the client by providing a valid certificate.
 *
 * When the error received is cause by wrong credentials provided by the driver,
 * then to recover from the error, close current driver and recreate a new driver with the correct credentials.
 *
 * When the error received indicating that there is a server using a wrong certificate in the cluster,
 * then contact your cluster administrator and security manager.
 */
public class UnauthorizedException extends Neo4jException
{
    public UnauthorizedException( String message )
    {
        super( message );
    }

    public UnauthorizedException( String code, String message )
    {
        super( code, message );
    }

    public UnauthorizedException( String message, Throwable t )
    {
        super( message, t );
    }
}
