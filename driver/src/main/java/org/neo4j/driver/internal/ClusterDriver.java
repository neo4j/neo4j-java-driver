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

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.net.pooling.PoolSettings;
import org.neo4j.driver.internal.net.pooling.SocketConnectionPool;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.util.ConcurrentRingSet;
import org.neo4j.driver.internal.util.Consumer;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.ConnectionFailureException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;

import static java.lang.String.format;

public class ClusterDriver extends BaseDriver
{
    private static final String DISCOVER_MEMBERS = "dbms.cluster.discoverEndpointAcquisitionServers";
    private static final String ACQUIRE_ENDPOINTS = "dbms.cluster.acquireEndpoints";
    private final static Comparator<BoltServerAddress> COMPARATOR = new Comparator<BoltServerAddress>()
    {
        @Override
        public int compare( BoltServerAddress o1, BoltServerAddress o2 )
        {
            int compare = o1.host().compareTo( o2.host() );
            if (compare == 0)
            {
                compare = Integer.compare( o1.port(), o2.port() );
            }

            return compare;
        }
    };

    protected final ConnectionPool connections;

    private final ClusterSettings clusterSettings;
    private final ConcurrentRingSet<BoltServerAddress> discoveryServers = new ConcurrentRingSet<>(COMPARATOR);
    private final ConcurrentRingSet<BoltServerAddress> readServers = new ConcurrentRingSet<>(COMPARATOR);
    private final ConcurrentRingSet<BoltServerAddress> writeServers = new ConcurrentRingSet<>(COMPARATOR);

    public ClusterDriver( BoltServerAddress seedAddress, ConnectionSettings connectionSettings,
            ClusterSettings clusterSettings,
            SecurityPlan securityPlan,
            PoolSettings poolSettings, Logging logging )
    {
        super( securityPlan, logging );
        discoveryServers.add( seedAddress );
        this.connections = new SocketConnectionPool( connectionSettings, securityPlan, poolSettings, logging );
        this.clusterSettings = clusterSettings;
        tryDiscover();
    }

    private void tryDiscover()
    {
        synchronized ( discoveryServers )
        {
            if ( discoveryServers.size() < clusterSettings.minimumNumberOfServers() )
            {
                discover();
            }
        }
    }

    //must be called from a synchronized block
    private void discover()
    {
        BoltServerAddress address = null;
        try
        {
            boolean success = false;
            while ( !discoveryServers.isEmpty() && !success )
            {
                address = discoveryServers.next();
                success = call( address,  DISCOVER_MEMBERS, new Consumer<Record>()
                {
                    @Override
                    public void accept( Record record )
                    {
                        discoveryServers.add( new BoltServerAddress( record.get( "address" ).asString() ) );
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
                this.close();
                throw new ServiceUnavailableException(
                        String.format( "Server %s couldn't perform discovery",
                                address == null ? "`UNKNOWN`" : address.toString()),  ex );
            }
            else
            {
                throw ex;
            }
        }
    }

    //must be called from a synchronized method
    private boolean call( BoltServerAddress address, String procedureName, Consumer<Record> recorder )
    {
        Connection acquire = null;
        Session session = null;
        try
        {
            acquire = connections.acquire(address);
            session = new NetworkSession( acquire, log );

            StatementResult records = session.run( format( "CALL %s", procedureName ) );
            while ( records.hasNext() )
            {
                recorder.accept( records.next() );
            }
        }
        catch ( ConnectionFailureException e )
        {
            forget( address );
            return false;
        }
        finally
        {
            if ( session != null )
            {
                session.close();
            }
            if ( acquire != null )
            {
                acquire.close();
            }

        }
        return true;
    }

    //must be called from a synchronized method
    private void callWithRetry( String procedureName, Consumer<Record> recorder )
    {
        while ( !discoveryServers.isEmpty() )
        {
            BoltServerAddress address = discoveryServers.next();
            Connection acquire = null;
            Session session = null;
            try
            {
                acquire = connections.acquire(address);
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
                if ( acquire != null )
                {
                    forget( acquire.address() );
                }
            }
            finally
            {
                if ( acquire != null )
                {
                    acquire.close();
                }
                if ( session != null )
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
        discoveryServers.remove( address );
        readServers.remove( address );
        writeServers.remove( address );
    }

    @Override
    public Session session()
    {
        return session( AccessMode.WRITE );
    }

    @Override
    public Session session( final AccessMode mode )
    {
        return new ClusteredNetworkSession( acquireConnection( mode ), clusterSettings,
                new Consumer<BoltServerAddress>()
                {
                    @Override
                    public void accept( BoltServerAddress address )
                    {
                        forget( address );
                    }
                }, log );
    }

    private Connection acquireConnection( AccessMode mode )
    {
        //if we are short on servers, find new ones
        if ( discoveryServers.size() < clusterSettings.minimumNumberOfServers() )
        {
            discover();
        }

        switch ( mode )
        {
        case READ:
            if ( readServers.size() < 3 )
            {
                discoverEndpoints();
            }
            return connections.acquire( readServers.next() );
        case WRITE:
            if ( writeServers.size() < 1 )
            {
                discoverEndpoints();
            }
            return connections.acquire( writeServers.next() );
        default:
            throw new ClientException( mode + " is not supported for creating new sessions" );
        }
    }

    private synchronized void discoverEndpoints() throws ServiceUnavailableException
    {
        try
        {
            callWithRetry( ACQUIRE_ENDPOINTS, new Consumer<Record>()
            {
                @Override
                public void accept( Record record )
                {
                    switch ( record.get( "role" ).asString().toUpperCase() )
                    {
                    case "READ":
                        readServers.add( new BoltServerAddress( record.get( "address" ).asString() ) );
                        break;
                    case "WRITE":
                        writeServers.add( new BoltServerAddress( record.get( "address" ).asString() ) );
                        break;
                    }
                }
            } );
        }
        catch ( ClientException e )
        {
            if ( e.code().equals( "Neo.ClientError.Procedure.ProcedureNotFound" ) )
            {
                log.warn( "Could not find procedure %s", ACQUIRE_ENDPOINTS );
                throw new ServiceUnavailableException("Server couldn't perform discovery", e );

            }
            throw e;
        }

        if ( readServers.isEmpty() || writeServers.isEmpty() )
        {
            throw new ServiceUnavailableException( "Could not establish any endpoints for the call" );
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

    //For testing
    Set<BoltServerAddress> discoveryServers()
    {
        return Collections.unmodifiableSet( discoveryServers);
    }


    //For testing
    Set<BoltServerAddress> readServers()
    {
        return Collections.unmodifiableSet(readServers);
    }

    //For testing
    Set<BoltServerAddress> writeServers()
    {
        return Collections.unmodifiableSet( writeServers);
    }

}