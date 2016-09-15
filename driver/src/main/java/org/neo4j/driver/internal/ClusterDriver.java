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
import java.util.Set;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.net.pooling.PoolSettings;
import org.neo4j.driver.internal.net.pooling.SocketConnectionPool;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.util.ConcurrentRoundRobinSet;
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
    private static final String GET_SERVERS = "dbms.cluster.routing.getServers";
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
    private static final int MIN_SERVERS = 2;

    protected final ConnectionPool connections;

    private final ConcurrentRoundRobinSet<BoltServerAddress> routingServers = new ConcurrentRoundRobinSet<>(COMPARATOR);
    private final ConcurrentRoundRobinSet<BoltServerAddress> readServers = new ConcurrentRoundRobinSet<>(COMPARATOR);
    private final ConcurrentRoundRobinSet<BoltServerAddress> writeServers = new ConcurrentRoundRobinSet<>(COMPARATOR);

    public ClusterDriver( BoltServerAddress seedAddress, ConnectionSettings connectionSettings,
            SecurityPlan securityPlan,
            PoolSettings poolSettings, Logging logging )
    {
        super( securityPlan, logging );
        routingServers.add( seedAddress );
        this.connections = new SocketConnectionPool( connectionSettings, securityPlan, poolSettings, logging );
        checkServers();
    }

    private void checkServers()
    {
        synchronized ( routingServers )
        {
            //todo remove setting hardcode to 2
            if ( routingServers.size() < MIN_SERVERS ||
                 readServers.isEmpty() ||
                 writeServers.isEmpty())
            {
                getServers();
            }
        }
    }

    //must be called from a synchronized block
    private void getServers()
    {
        BoltServerAddress address = null;
        try
        {
            boolean success = false;
            while ( !routingServers.isEmpty() && !success )
            {
                address = routingServers.hop();
                success = call( address, GET_SERVERS, new Consumer<Record>()
                {
                    @Override
                    public void accept( Record record )
                    {
                        BoltServerAddress newAddress = new BoltServerAddress( record.get( "address" ).asString() );
                        switch ( record.get( "mode" ).asString().toUpperCase() )
                        {
                        case "READ":
                            readServers.add( newAddress );
                            break;
                        case "WRITE":
                            writeServers.add( newAddress );
                            break;
                        case "ROUTE":
                            routingServers.add( newAddress );
                            break;
                        }
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

    private synchronized void forget( BoltServerAddress address )
    {
        connections.purge( address );
        routingServers.remove( address );
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
        return new ClusteredNetworkSession( acquireConnection( mode ),
                new Consumer<BoltServerAddress>()
                {
                    @Override
                    public void accept( BoltServerAddress address )
                    {
                        forget( address );
                    }
                }, new Consumer<BoltServerAddress>()
        {
            @Override
            public void accept( BoltServerAddress address )
            {
                writeServers.remove( address );
            }
        },
                log );
    }

    private Connection acquireConnection( AccessMode mode )
    {
        //Potentially rediscover servers if we are not happy with our current knowledge
        checkServers();

        switch ( mode )
        {
        case READ:
            return connections.acquire( readServers.hop() );
        case WRITE:
            return connections.acquire( writeServers.hop() );
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

    //For testing
    Set<BoltServerAddress> routingServers()
    {
        return Collections.unmodifiableSet( routingServers );
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

    //For testing
    ConnectionPool connectionPool()
    {
        return connections;
    }

}