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
import java.util.Set;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.ConnectionFailureException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;
import org.neo4j.driver.v1.util.Function;

import static java.lang.String.format;

public class RoutingDriver extends BaseDriver
{
    private static final String GET_SERVERS = "dbms.cluster.routing.getServers";
    private static final long MAX_TTL = Long.MAX_VALUE / 1000L;

    private final ConnectionPool connections;
    private final Function<Connection,Session> sessionProvider;
    private final Clock clock;
    private ClusterView clusterView;


    public RoutingDriver( BoltServerAddress seedAddress,
            ConnectionPool connections,
            SecurityPlan securityPlan,
            Function<Connection,Session> sessionProvider,
            Clock clock,
            Logging logging )
    {
        super( securityPlan, logging );
        this.connections = connections;
        this.sessionProvider = sessionProvider;
        this.clock = clock;
        this.clusterView = new ClusterView( 0L, clock, log );
        this.clusterView.addRouter( seedAddress );
        checkServers();
    }

    private synchronized void checkServers()
    {
        if ( clusterView.isStale() )
        {
            Set<BoltServerAddress> oldAddresses = clusterView.all();
            ClusterView newView = newClusterView();
            Set<BoltServerAddress> newAddresses = newView.all();

            oldAddresses.removeAll( newAddresses );
            for ( BoltServerAddress boltServerAddress : oldAddresses )
            {
                connections.purge( boltServerAddress );
            }

            this.clusterView = newView;
        }
    }

    private long calculateNewExpiry( Record record )
    {
        long ttl = record.get( "ttl" ).asLong();
        long nextExpiry = clock.millis() + 1000L * ttl;
        if ( ttl < 0 || ttl >= MAX_TTL || nextExpiry < 0 )
        {
            return Long.MAX_VALUE;
        }
        else
        {
            return nextExpiry;
        }
    }

    private ClusterView newClusterView()
    {
        BoltServerAddress address = null;
        for ( int i = 0; i < clusterView.numberOfRouters(); i++ )
        {
            address = clusterView.nextRouter();
            ClusterView newClusterView;
            try
            {
                newClusterView = call( address, GET_SERVERS, new Function<Record,ClusterView>()

                {
                    @Override
                    public ClusterView apply( Record record )
                    {
                        long expire = calculateNewExpiry( record );
                        ClusterView newClusterView = new ClusterView( expire, clock, log );
                        List<ServerInfo> servers = servers( record );
                        for ( ServerInfo server : servers )
                        {
                            switch ( server.role() )
                            {
                            case "READ":
                                newClusterView.addReaders( server.addresses() );
                                break;
                            case "WRITE":
                                newClusterView.addWriters( server.addresses() );
                                break;
                            case "ROUTE":
                                newClusterView.addRouters( server.addresses() );
                                break;
                            }
                        }
                        return newClusterView;
                    }
                } );
            }
            catch ( Throwable t )
            {
                forget( address );
                continue;
            }

            if ( newClusterView.numberOfRouters() != 0 )
            {
                return newClusterView;
            }
        }


        //discovery failed, not much to do, stick with what we've got
        //this may happen because server is running in standalone mode
        this.close();
        throw new ServiceUnavailableException(
                String.format( "Server %s couldn't perform discovery",
                        address == null ? "`UNKNOWN`" : address.toString() ) );

    }

    private static class ServerInfo
    {
        private final List<BoltServerAddress> addresses;
        private final String role;

        public ServerInfo( List<BoltServerAddress> addresses, String role )
        {
            this.addresses = addresses;
            this.role = role;
        }

        public String role()
        {
            return role;
        }

        List<BoltServerAddress> addresses()
        {
            return addresses;
        }
    }

    private List<ServerInfo> servers( Record record )
    {
        return record.get( "servers" ).asList( new Function<Value,ServerInfo>()
        {
            @Override
            public ServerInfo apply( Value value )
            {
                return new ServerInfo( value.get( "addresses" ).asList( new Function<Value,BoltServerAddress>()
                {
                    @Override
                    public BoltServerAddress apply( Value value )
                    {
                        return new BoltServerAddress( value.asString() );
                    }
                } ), value.get( "role" ).asString() );
            }
        } );
    }

    //must be called from a synchronized method
    private <T> T call( BoltServerAddress address, String procedureName, Function<Record, T> recorder )
    {
        Connection acquire;
        Session session = null;
        try
        {
            acquire = connections.acquire( address );
            session = sessionProvider.apply( acquire );

            StatementResult records = session.run( format( "CALL %s", procedureName ) );
            //got a result but was empty
            if ( !records.hasNext() )
            {
                forget( address );
                throw new IllegalStateException("Server responded with empty result");
            }
            //consume the results
            return recorder.apply( records.single() );
        }
        finally
        {
            if ( session != null )
            {
                session.close();
            }
        }
    }

    private synchronized void forget( BoltServerAddress address )
    {
        connections.purge( address );
        clusterView.remove(address);
    }

    @Override
    public Session session()
    {
        return session( AccessMode.WRITE );
    }

    @Override
    public Session session( final AccessMode mode )
    {
        Connection connection = acquireConnection( mode );
        return new RoutingNetworkSession( new NetworkSession( connection ), mode, connection.address(),
                new RoutingErrorHandler()
                {
                    @Override
                    public void onConnectionFailure( BoltServerAddress address )
                    {
                        forget( address );
                    }

                    @Override
                    public void onWriteFailure( BoltServerAddress address )
                    {
                        clusterView.removeWriter( address );
                    }
                } );
    }

    private Connection acquireConnection( AccessMode role )
    {
        //Potentially rediscover servers if we are not happy with our current knowledge
        checkServers();

        switch ( role )
        {
        case READ:
            return acquireReadConnection();
        case WRITE:
            return acquireWriteConnection();
        default:
            throw new ClientException( role + " is not supported for creating new sessions" );
        }
    }

    private Connection acquireReadConnection()
    {
        int numberOfServers = clusterView.numberOfReaders();
        for ( int i = 0; i < numberOfServers; i++ )
        {
            BoltServerAddress address = clusterView.nextReader();
            try
            {
                return connections.acquire( address );
            }
            catch ( ConnectionFailureException e )
            {
                forget( address );
            }
        }

        throw new SessionExpiredException( "Failed to connect to any read server" );
    }

    private Connection acquireWriteConnection()
    {
        int numberOfServers = clusterView.numberOfWriters();
        for ( int i = 0; i < numberOfServers; i++ )
        {
            BoltServerAddress address = clusterView.nextWriter();
            try
            {
                return connections.acquire( address );
            }
            catch ( ConnectionFailureException e )
            {
                forget( address );
            }
        }

        throw new SessionExpiredException( "Failed to connect to any write server" );
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
    public Set<BoltServerAddress> routingServers()
    {
        return clusterView.routingServers();
    }

    //For testing
    public Set<BoltServerAddress> readServers()
    {
        return  clusterView.readServers();
    }

    //For testing
    public Set<BoltServerAddress> writeServers()
    {
        return clusterView.writeServers( );
    }

    //For testing
    public ConnectionPool connectionPool()
    {
        return connections;
    }

}
