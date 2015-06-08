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
package org.neo4j.driver.internal.connector.socket;

import java.net.URI;
import java.util.Collection;

import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.Version;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.Connector;
import org.neo4j.driver.internal.spi.Logging;

import static java.util.Arrays.asList;

public class SocketConnector implements Connector
{
    public static final String SCHEME = "neo4j";
    public static final int DEFAULT_PORT = 7687;

    private Logging logging;

    @Override
    public boolean supports( String scheme )
    {
        return scheme.equals( SCHEME );
    }

    @Override
    public Connection connect( URI sessionURI ) throws ClientException
    {
        int port = sessionURI.getPort() == -1 ? DEFAULT_PORT : sessionURI.getPort();
        SocketConnection conn = new SocketConnection( sessionURI.getHost(), port, logging );
        conn.initialize( "ndp-java-driver/" + Version.driverVersion() );
        return conn;
    }

    @Override
    public void setLogging( Logging logging )
    {
        this.logging = logging;
    }

    @Override
    public Collection<String> supportedSchemes()
    {
        return asList( SCHEME );
    }
}
