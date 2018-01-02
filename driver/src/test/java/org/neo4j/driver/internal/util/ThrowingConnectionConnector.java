/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.util.List;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.spi.Connector;

public class ThrowingConnectionConnector implements Connector
{
    private final Connector realConnector;
    private final List<ThrowingConnection> connections;

    public ThrowingConnectionConnector( Connector realConnector, List<ThrowingConnection> connections )
    {
        this.realConnector = realConnector;
        this.connections = connections;
    }

    @Override
    public ThrowingConnection connect( BoltServerAddress address )
    {
        ThrowingConnection connection = new ThrowingConnection( realConnector.connect( address ) );
        connections.add( connection );
        return connection;
    }
}
