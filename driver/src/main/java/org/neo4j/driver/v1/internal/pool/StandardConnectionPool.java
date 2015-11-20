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
package org.neo4j.driver.v1.internal.pool;

import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.internal.connector.socket.SocketConnector;
import org.neo4j.driver.v1.internal.spi.Connection;
import org.neo4j.driver.v1.internal.spi.ConnectionPool;
import org.neo4j.driver.v1.internal.spi.Connector;
import org.neo4j.driver.v1.internal.util.Clock;
import org.neo4j.driver.v1.internal.util.Consumer;

/**
 * A basic connection pool that optimizes for threads being long-lived, acquiring/releasing many connections.
 * It uses a global queue as a fallback pool, but tries to avoid coordination by storing connections in a ThreadLocal.
 * <p>
 * Safety is achieved by tracking thread locals getting garbage collected, returning connections to the global pool
 * when this happens.
 * <p>
 * If threads are long-lived, this pool will achieve linearly scalable performance with overhead equivalent to a
 * hash-map lookup per acquire.
 * <p>
 * If threads are short-lived, this pool is not ideal.
 */
public class StandardConnectionPool implements ConnectionPool
{
    /**
     * Map of scheme -> connector, this is what we use to establish new connections.
     */
    private final ConcurrentHashMap<String,Connector> connectors = new ConcurrentHashMap<>();

    /**
     * Pools, organized by URL.
     */
    private final ConcurrentHashMap<URI,ThreadCachingPool<PooledConnection>> pools = new ConcurrentHashMap<>();

    /**
     * Connections that fail this criteria will be disposed of.
     */
    private final ValidationStrategy<PooledConnection> connectionValidation;

    private final Clock clock;
    private final Config config;

    public StandardConnectionPool( Config config )
    {
        this( loadConnectors(), Clock.SYSTEM, config );
    }

    public StandardConnectionPool( Collection<Connector> conns, Clock clock, Config config )
    {
        this.config = config;
        this.clock = clock;
        this.connectionValidation = new PooledConnectionValidator( config.idleTimeBeforeConnectionTest() );
        for ( Connector connector : conns )
        {
            for ( String s : connector.supportedSchemes() )
            {
                this.connectors.put( s, connector );
            }
        }
    }

    @Override
    public Connection acquire( URI sessionURI )
    {
        try
        {
            PooledConnection conn = pool( sessionURI ).acquire( 30, TimeUnit.SECONDS );
            if( conn == null )
            {
                throw new ClientException(
                        "Failed to acquire a session with Neo4j " +
                        "as all the connections in the connection pool are already occupied by other sessions. "+
                        "Please close unused session and retry. " +
                        "Current Pool size: " + config.connectionPoolSize() +
                        ". If your application requires running more sessions concurrently than the current pool " +
                        "size, you should create a driver with a larger connection pool size." );
            }
            return conn;
        }
        catch ( InterruptedException e )
        {
            throw new ClientException( "Interrupted while waiting for a connection to Neo4j." );
        }
    }

    private ThreadCachingPool<PooledConnection> pool( URI sessionURI )
    {
        ThreadCachingPool<PooledConnection> pool = pools.get( sessionURI );
        if ( pool == null )
        {
            pool = newPool( sessionURI );
            if ( pools.putIfAbsent( sessionURI, pool ) != null )
            {
                // We lost a race to create the pool, dispose of the one we created, and recurse
                pool.close();
                return pool( sessionURI );
            }
        }
        return pool;
    }

    private static Collection<Connector> loadConnectors()
    {
        List<Connector> connectors = new LinkedList<>();

        // Hard code socket connector
        Connector conn = new SocketConnector();
        connectors.add( conn );

        // Load custom loadConnectors via JSL
        ServiceLoader<Connector> load = ServiceLoader.load( Connector.class );
        for ( Connector connector : load )
        {
            connectors.add( connector );
        }
        return connectors;
    }

    @Override
    public void close() throws Exception
    {
        for ( ThreadCachingPool<PooledConnection> pool : pools.values() )
        {
            pool.close();
        }
        pools.clear();
    }

    private String connectorSchemes()
    {
        return Arrays.toString( connectors.keySet().toArray( new String[connectors.keySet().size()] ) );
    }

    private ThreadCachingPool<PooledConnection> newPool( final URI uri )
    {

        return new ThreadCachingPool<>( config.connectionPoolSize(), new Allocator<PooledConnection>()
        {
            @Override
            public PooledConnection allocate( Consumer<PooledConnection> release )
            {
                Connector connector = connectors.get( uri.getScheme() );
                if ( connector == null )
                {
                    throw new ClientException(
                            "'" + uri.getScheme() + "' is not a supported transport (in '" +
                            uri + "', available transports are: " + connectorSchemes() + "." );
                }
                Connection conn = connector.connect( uri, config );
                return new PooledConnection( conn, release );
            }

            @Override
            public void onDispose( PooledConnection pooledConnection )
            {
                pooledConnection.dispose();
            }

            @Override
            public void onAcquire( PooledConnection pooledConnection )
            {

            }
        }, connectionValidation, clock );
    }
}
