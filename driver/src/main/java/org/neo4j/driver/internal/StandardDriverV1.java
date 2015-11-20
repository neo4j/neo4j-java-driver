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
package org.neo4j.driver.internal;

import java.net.URI;

import org.neo4j.driver.internal.pool.StandardConnectionPool;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;

public class StandardDriverV1 implements Driver, AutoCloseable
{
    private final ConnectionPool connections;
    private final URI url;

    public StandardDriverV1( URI url, Config config )
    {
        this.url = url;
        this.connections = new StandardConnectionPool( config );
    }

    /**
     * Establish a session
     * @return a session that could be used to run {@link Session#run(String) a statement} or
     * {@link Session#newTransaction() a transaction }.
     */
    @Override
    public Session session()
    {
        return new StandardSession( connections.acquire( url ) );
        // TODO a ConnectionPool per URL
        // ConnectionPool connections = new StandardConnectionPool( logging, url );
        // And to get a connection from the pool could be
        // connections.acquire();
    }

    /**
     * Close all the resources assigned to this driver
     * @throws Exception any error that might happen when releasing all resources
     */
    @Override
    public void close() throws Exception
    {
        connections.close();
    }
}
