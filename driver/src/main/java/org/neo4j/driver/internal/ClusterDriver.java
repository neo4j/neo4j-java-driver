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

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.net.pooling.PoolSettings;
import org.neo4j.driver.internal.net.pooling.SocketConnectionPool;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.util.Function;

import java.util.LinkedList;
import java.util.List;

import static java.lang.String.format;

public class ClusterDriver extends BaseDriver
{
    private static final String DISCOVER_MEMBERS = "dbms.cluster.discoverMembers";

    private final ConnectionPool connections;

    public ClusterDriver( BoltServerAddress seedAddress, ConnectionSettings connectionSettings, SecurityPlan securityPlan,
                          PoolSettings poolSettings, Logging logging )
    {
        super( seedAddress, securityPlan, logging );
        this.connections = new SocketConnectionPool( connectionSettings, securityPlan, poolSettings, logging );
        discover();
    }

    public void discover()
    {
        final List<BoltServerAddress> newServers = new LinkedList<>(  );
        try
        {
            call( DISCOVER_MEMBERS, new Function<Record, Integer>()
            {
                @Override
                public Integer apply( Record record )
                {
                    newServers.add( new BoltServerAddress( record.get( "address" ).asString() ) );
                    return 0;
                }
            } );
            this.servers.clear();
            this.servers.addAll( newServers );
            log.debug( "~~ [MEMBERS] -> %s", newServers );
        }
        catch ( ClientException ex )
        {
            if ( ex.code().equals( "Neo.ClientError.Procedure.ProcedureNotFound" ) )
            {
                throw new ClientException( "Discovery failed: could not find procedure %s", DISCOVER_MEMBERS );
            }
            else
            {
                throw ex;
            }
        }
    }

    void call( String procedureName, Function<Record, Integer> recorder )
    {
        try ( Session session = new NetworkSession( connections.acquire( randomServer() ), log ) )
        {
            StatementResult records = session.run( format( "CALL %s", procedureName ) );
            while ( records.hasNext() )
            {
                recorder.apply( records.next() );
            }
        }
    }

    @Override
    public Session session()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close()
    {
        try
        {
            connections.close();
        }
        catch ( Exception ex )
        {
            log.error( format( "~~ [ERROR] %s", ex.getMessage() ), ex );
        }
    }

}