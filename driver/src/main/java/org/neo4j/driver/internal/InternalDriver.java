/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
