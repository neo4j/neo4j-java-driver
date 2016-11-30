/*
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
package org.neo4j.driver.internal.net.pooling;


import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.driver.internal.exceptions.BoltProtocolException;
import org.neo4j.driver.internal.exceptions.ConnectionException;
import org.neo4j.driver.internal.exceptions.InvalidOperationException;
import org.neo4j.driver.internal.exceptions.ServerNeo4jException;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.spi.Connector;
import org.neo4j.driver.internal.spi.PooledConnection;
import org.neo4j.driver.v1.Logging;

/**
 * The pool is designed to buffer certain amount of free sessions into session pool. When closing a session, we first
 * try to return the session into the session pool, however if we failed to return it back, either because the pool
 * is full or the pool is being cleaned on driver.close, then we directly close the connection attached with the
 * session.
 * <p>
 * The session is NOT meant to be thread safe, each thread should have an independent session and close it (return to
 * pool) when the work with the session has been done.
 * <p>
 * The driver is thread safe. Each thread could try to get a session from the pool and then return it to the pool
 * at the same time.
 */
public class SocketConnectionPool implements ConnectionPool
{
    /**
     * Pools, organized by server address.
     */

    private final AtomicBoolean closed = new AtomicBoolean();
    private final ConcurrentHashMap<BoltServerAddress,BlockingPooledConnectionQueue> pools = new ConcurrentHashMap<>();

    private final PoolSettings poolSettings;
    private final Connector connector;
    private final Logging logging;

    public SocketConnectionPool( PoolSettings poolSettings, Connector connector, Logging logging )
    {
        this.poolSettings = poolSettings;
        this.connector = connector;
        this.logging = logging;
    }

    @Override
    public PooledConnection acquire( final BoltServerAddress address )
            throws ConnectionException, InvalidOperationException, ServerNeo4jException, BoltProtocolException
    {
        assertNotClosed();

        final BlockingPooledConnectionQueue connections = pool( address );
        PooledConnectionFactory pooledConnectionFactory = new PooledConnectionFactory()
        {
            @Override
            public PooledConnection newInstance()
                    throws ConnectionException, ServerNeo4jException, InvalidOperationException, BoltProtocolException
            {
                PooledConnectionReleaser releaseManager =
                        new SocketConnectionPoolPooledConnectionReleaseManager( connections );
                return new PooledSocketConnection( connector.connect( address ), releaseManager );
            }
        };
        PooledConnection conn = connections.acquire( pooledConnectionFactory );

        if ( closed.get() )
        {
            connections.terminate();
            throw poolClosedException();
        }

        return conn;
    }

    @Override
    public void purge( BoltServerAddress address ) throws InvalidOperationException
    {
        BlockingPooledConnectionQueue connections = pools.remove( address );
        if ( connections != null )
        {
            connections.terminate();
        }
    }

    @Override
    public boolean hasAddress( BoltServerAddress address )
    {
        return pools.containsKey( address );
    }

    @Override
    public void close() throws InvalidOperationException
    {
        if ( closed.compareAndSet( false, true ) )
        {
            for ( BlockingPooledConnectionQueue pool : pools.values() )
            {
                pool.terminate();
            }

            pools.clear();
        }
    }

    private void assertNotClosed() throws InvalidOperationException
    {
        if ( closed.get() )
        {
            throw poolClosedException();
        }
    }

    private static InvalidOperationException poolClosedException()
    {
        return new InvalidOperationException( "Pool closed" );
    }

    private BlockingPooledConnectionQueue pool( BoltServerAddress address )
    {
        BlockingPooledConnectionQueue pool = pools.get( address );
        if ( pool == null )
        {
            pool = new BlockingPooledConnectionQueue( address, poolSettings.maxIdleConnectionPoolSize(), logging );

            if ( pools.putIfAbsent( address, pool ) != null )
            {
                // We lost a race to create the pool, dispose of the one we created, and recurse
                return pool( address );
            }
        }
        return pool;
    }

    private class SocketConnectionPoolPooledConnectionReleaseManager extends PooledConnectionReleaseManager
    {
        SocketConnectionPoolPooledConnectionReleaseManager( BlockingPooledConnectionQueue connections )
        {
            super( connections, new PooledConnectionValidator( SocketConnectionPool.this ) );
        }
    }
}
