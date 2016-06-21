/**
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * <p>
 * This file is part of Neo4j.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.neo4j.driver.internal;

import java.net.URI;

import org.neo4j.driver.internal.pool.InternalConnectionPool;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.Neo4jException;

import static org.neo4j.driver.internal.util.AddressUtil.isLocalhost;
import static org.neo4j.driver.v1.Config.EncryptionLevel.REQUIRED;
import static org.neo4j.driver.v1.Config.EncryptionLevel.REQUIRED_NON_LOCAL;

public class InternalDriver implements Driver
{
    private final ConnectionPool connections;
    private final URI url;
    private final Config config;

    public InternalDriver( URI url, AuthToken authToken, Config config )
    {
        this.url = url;
        this.connections = new InternalConnectionPool( config, authToken );
        this.config = config;
    }

    @Override
    public boolean encrypted()
    {

        Config.EncryptionLevel encryptionLevel = config.encryptionLevel();
        return encryptionLevel.equals( REQUIRED ) ||
                ( encryptionLevel.equals( REQUIRED_NON_LOCAL ) && !isLocalhost( url.getHost() ) );
    }

    /**
     * Establish a session
     * @return a session that could be used to run {@link Session#run(String) a statement} or
     * {@link Session#beginTransaction() a transaction }.
     */
    @Override
    public Session session()
    {
        return new InternalSession( connections.acquire( url ), config.logging().getLog( "session" ) );
    }

    /**
     * Close all the resources assigned to this driver
     * @throws Exception any error that might happen when releasing all resources
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
