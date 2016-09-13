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

import java.util.List;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.net.pooling.PoolSettings;
import org.neo4j.driver.internal.net.pooling.SocketConnectionPool;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.Consumer;
import org.neo4j.driver.internal.util.Supplier;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.SessionMode;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.ConnectionFailureException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;

import static java.lang.String.format;

public class ClusterDriver extends BaseDriver
{
    private static final String DISCOVER_MEMBERS = "dbms.cluster.discoverEndpointAcquisitionServers";
    private static final String ACQUIRE_ENDPOINTS = "dbms.cluster.acquireEndpoints";

    private final Endpoints endpoints = new Endpoints();
    private final ClusterSettings clusterSettings;
    private boolean discoverable = true;

    public ClusterDriver( BoltServerAddress seedAddress, ConnectionSettings connectionSettings,
            ClusterSettings clusterSettings,
            SecurityPlan securityPlan,
            PoolSettings poolSettings, Logging logging )
    {
        super( new SocketConnectionPool( connectionSettings, securityPlan, poolSettings, logging ),seedAddress, securityPlan, logging );
        this.clusterSettings = clusterSettings;
        discover();
    }

    synchronized void discover()
    {
        if (!discoverable)
        {
            return;
        }

        try
        {
            boolean success = false;
            while ( !connections.isEmpty() && !success )
            {
                success = call( DISCOVER_MEMBERS, new Consumer<Record>()
                {
                    @Override
                    public void accept( Record record )
                    {
                        connections.add(new BoltServerAddress( record.get( "address" ).asString() ));
                    }
                } );
            }
            if ( !success )
            {
                throw new ServiceUnavailableException( "Run out of servers" );
            }
        }
        catch ( ClientException ex )
        {
            if ( ex.code().equals( "Neo.ClientError.Procedure.ProcedureNotFound" ) )
            {
                //no procedure there, not much to do, stick with what we've got
                //this may happen because server is running in standalone mode
                log.warn( "Could not find procedure %s", DISCOVER_MEMBERS );
                discoverable = false;
            }
            else
            {
                throw ex;
            }
        }
    }

    //must be called from a synchronized method
    private boolean call( String procedureName, Consumer<Record> recorder )
    {
        Connection acquire = null;
        Session session = null;
        try {
            acquire = connections.acquire();
            session = new NetworkSession( acquire, log );

            StatementResult records = session.run( format( "CALL %s", procedureName ) );
            while ( records.hasNext() )
            {
                recorder.accept( records.next() );
            }
        }
        catch ( ConnectionFailureException e )
        {
            if (acquire != null)
            {
                forget( acquire.address() );
            }
            return false;
        }
        finally
        {
            if (acquire != null)
            {
                acquire.close();
            }
            if (session != null)
            {
                session.close();
            }
        }
        return true;
    }

    //must be called from a synchronized method
    private void callWithRetry(String procedureName, Consumer<Record> recorder )
    {
        while ( !connections.isEmpty() )
        {
            Connection acquire = null;
            Session session = null;
            try {
                acquire = connections.acquire();
                session = new NetworkSession( acquire, log );
                List<Record> list = session.run( format( "CALL %s", procedureName ) ).list();
                for ( Record record : list )
                {
                    recorder.accept( record );
                }
                //we found results give up
                return;
            }
            catch ( ConnectionFailureException e )
            {
                if (acquire != null)
                {
                    forget( acquire.address() );
                }
            }
            finally
            {
                if (acquire != null)
                {
                 acquire.close();
                }
                if (session != null)
                {
                    session.close();
                }
            }
        }

        throw new ServiceUnavailableException( "Failed to communicate with any of the cluster members" );
    }

    private synchronized void forget( BoltServerAddress address )
    {
        connections.purge( address );
    }

    @Override
    public Session session()
    {
        return session( SessionMode.WRITE );
    }

    @Override
    public Session session( final SessionMode mode )
    {
        switch ( mode )
        {
        case READ:
            return new ReadNetworkSession( new Supplier<Connection>()
            {
                @Override
                public Connection get()
                {
                    return acquireConnection( mode );
                }
            }, new Consumer<Connection>()
            {
                @Override
                public void accept( Connection connection )
                {
                    forget( connection.address() );
                }
            }, clusterSettings, log );
        case WRITE:
            return new WriteNetworkSession( acquireConnection( mode ), clusterSettings, log );
        default:
            throw new UnsupportedOperationException();
        }
    }

    private synchronized Connection acquireConnection( SessionMode mode )
    {
        if (!discoverable)
        {
            return connections.acquire();
        }

        //if we are short on servers, find new ones
        if ( connections.addressCount() < clusterSettings.minimumNumberOfServers() )
        {
            discover();
        }

        endpoints.clear();
        try
        {
            callWithRetry( ACQUIRE_ENDPOINTS, new Consumer<Record>()
            {
                @Override
                public void accept( Record record )
                {
                    String serverMode = record.get( "role" ).asString();
                    if ( serverMode.equals( "READ" ) )
                    {
                        endpoints.readServer = new BoltServerAddress( record.get( "address" ).asString() );
                    }
                    else if ( serverMode.equals( "WRITE" ) )
                    {
                        endpoints.writeServer = new BoltServerAddress( record.get( "address" ).asString() );
                    }
                }
            } );
        }
        catch (ClientException e)
        {
            if ( e.code().equals( "Neo.ClientError.Procedure.ProcedureNotFound" ) )
            {
                log.warn( "Could not find procedure %s", ACQUIRE_ENDPOINTS );
                discoverable = false;
                return connections.acquire();
            }
            throw e;
        }

        if ( !endpoints.valid() )
        {
            throw new ServiceUnavailableException("Could not establish any endpoints for the call");
        }


        switch ( mode )
        {
        case READ:
            return connections.acquire( endpoints.readServer );
        case WRITE:
            return connections.acquire( endpoints.writeServer );
        default:
            throw new ClientException( mode + " is not supported for creating new sessions" );
        }
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

    private static class Endpoints
    {
        BoltServerAddress readServer;
        BoltServerAddress writeServer;

        public boolean valid()
        {
            return readServer != null && writeServer != null;
        }

        public void clear()
        {
            readServer = null;
            writeServer = null;
        }
    }

}