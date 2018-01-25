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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.driver.internal.ConnectionSettings;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.Connector;
import org.neo4j.driver.v1.Logging;

public class ConnectionTrackingDriverFactory extends DriverFactoryWithClock
{
    private final Set<Connection> connections = Collections.newSetFromMap( new ConcurrentHashMap<Connection,Boolean>() );

    public ConnectionTrackingDriverFactory( Clock clock )
    {
        super( clock );
    }

    @Override
    protected Connector createConnector( ConnectionSettings connectionSettings, SecurityPlan securityPlan, Logging logging )
    {
        Connector connector = super.createConnector( connectionSettings, securityPlan, logging );
        return new ConnectionTrackingConnector( connector, connections );
    }

    public void closeConnections()
    {
        Set<Connection> connectionsSnapshot = new HashSet<>( connections );
        connections.clear();
        for ( Connection connection : connectionsSnapshot )
        {
            connection.close();
        }
    }
}
