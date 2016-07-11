/**
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
package org.neo4j.driver.internal;

import org.neo4j.driver.internal.connector.socket.SocketConnector;
import org.neo4j.driver.internal.pool.InternalConnectionPool;
import org.neo4j.driver.internal.pool.PoolSettings;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.util.BoltServerAddress;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.Neo4jException;

public class DirectDriver implements Driver
{
    private final BoltServerAddress address;
    private final SecurityPlan securityPlan;
    private final Logging logging;
    private final ConnectionPool connections;

    public DirectDriver( BoltServerAddress address, SecurityPlan securityPlan, PoolSettings poolSettings, Logging logging )
    {
        this.address = address;
        this.securityPlan = securityPlan;
        this.logging = logging;
        this.connections = new InternalConnectionPool( new SocketConnector(), Clock.SYSTEM, securityPlan, poolSettings, logging );
    }

    @Override
    public boolean isEncrypted()
    {
        return securityPlan.requiresEncryption();
    }

    /**
     * Establish a session
     * @return a session that could be used to run {@link Session#run(String) a statement} or
     * {@link Session#beginTransaction() a transaction }.
     */
    @Override
    public Session session()
    {
        return new InternalSession( connections.acquire( address ), logging.getLog( "session" ) );
    }

    /**
     * Close all the resources assigned to this driver
     * @throws Neo4jException any error that might happen when releasing all resources
     */
    public void close() throws Neo4jException
    {
        try
        {
            connections.close();
        }
        catch ( Exception e )
        {
            throw new ClientException( "Failed to close driver.", e );
        }
    }
}
