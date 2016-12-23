/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.spi.ConnectionValidator;
import org.neo4j.driver.internal.spi.Connector;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.Supplier;
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
    private final ConcurrentMap<BoltServerAddress,BlockingPooledConnectionQueue> pools = new ConcurrentHashMap<>();

    private final AtomicBoolean closed = new AtomicBoolean();

    private final PoolSettings poolSettings;
    private final Connector connector;
    private final ConnectionValidator<PooledConnection> connectionValidator;
    private final Clock clock;
    private final Logging logging;

    public SocketConnectionPool( PoolSettings poolSettings, Connector connector, Clock clock, Logging logging )
    {
        this.poolSettings = poolSettings;
        this.connector = connector;
        this.connectionValidator = new PooledConnectionValidator( this );
        this.clock = clock;
        this.logging = logging;
    }

    @Override
    public Connection acquire( final BoltServerAddress address )
    {
        assertNotClosed();
        BlockingPooledConnectionQueue connectionQueue = pool( address );
        PooledConnection connection = acquireConnection( address, connectionQueue );
        assertNotClosed( address, connectionQueue );

        return connection;
    }

    @Override
    public void purge( BoltServerAddress address )
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
    public void close()
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

    private PooledConnection acquireConnection( final BoltServerAddress address,
            final BlockingPooledConnectionQueue connectionQueue )
    {
        Supplier<PooledConnection> supplier = newPooledConnectionSupplier( address, connectionQueue );

        int acquisitionAttempt = 1;
        PooledConnection connection = connectionQueue.acquire( supplier );
        while ( !canBeAcquired( connection, acquisitionAttempt ) )
        {
            connection = connectionQueue.acquire( supplier );
            acquisitionAttempt++;
        }
        return connection;
    }

    private Supplier<PooledConnection> newPooledConnectionSupplier( final BoltServerAddress address,
            final BlockingPooledConnectionQueue connectionQueue )
    {
        return new Supplier<PooledConnection>()
        {
            @Override
            public PooledConnection get()
            {
                PooledConnectionReleaseConsumer releaseConsumer =
                        new PooledConnectionReleaseConsumer( connectionQueue, connectionValidator );
                return new PooledConnection( connector.connect( address ), releaseConsumer, clock );
            }
        };
    }

    private boolean canBeAcquired( PooledConnection connection, int acquisitionAttempt )
    {
        if ( poolSettings.idleTimeBeforeConnectionTestConfigured() )
        {
            if ( acquisitionAttempt > poolSettings.maxIdleConnectionPoolSize() )
            {
                return true;
            }

            if ( hasBeenIdleForTooLong( connection ) )
            {
                return connectionValidator.isConnected( connection );
            }
        }
        return true;
    }

    private boolean hasBeenIdleForTooLong( PooledConnection connection )
    {
        long idleTime = clock.millis() - connection.lastUsedTimestamp();
        return idleTime > poolSettings.idleTimeBeforeConnectionTest();
    }

    private void assertNotClosed( BoltServerAddress address, BlockingPooledConnectionQueue connections )
    {
        if ( closed.get() )
        {
            connections.terminate();
            pools.remove( address );
            assertNotClosed();
        }
    }

    private void assertNotClosed()
    {
        if ( closed.get() )
        {
            throw new IllegalStateException( "Pool closed" );
        }
    }
}
