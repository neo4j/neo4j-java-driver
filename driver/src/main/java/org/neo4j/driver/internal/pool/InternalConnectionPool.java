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
package org.neo4j.driver.internal.pool;

import java.net.URI;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.spi.Connector;
import org.neo4j.driver.internal.util.BoltServerAddress;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.exceptions.Neo4jException;

/**
 * The pool is designed to buffer certain amount of free sessions into session pool. When closing a session, we first
 * try to return the session into the session pool, however if we failed to return it back, either because the pool
 * is full or the pool is being cleaned on driver.close, then we directly close the connection attached with the
 * session.
 *
 * The session is NOT meant to be thread safe, each thread should have an independent session and close it (return to
 * pool) when the work with the session has been done.
 *
 * The driver is thread safe. Each thread could try to get a session from the pool and then return it to the pool
 * at the same time.
 */
public class InternalConnectionPool implements ConnectionPool
{

    /**
     * Pools, organized by URL.
     */
    private final ConcurrentHashMap<BoltServerAddress,BlockingQueue<PooledConnection>> pools = new ConcurrentHashMap<>();

    private final Connector connector;
    private final SecurityPlan securityPlan;
    private final Clock clock;
    private final PoolSettings poolSettings;
    private final Logging logging;

    /** Shutdown flag */
    private final AtomicBoolean stopped = new AtomicBoolean( false );

    public InternalConnectionPool( Connector connector, Clock clock, SecurityPlan securityPlan,
                                   PoolSettings poolSettings, Logging logging )
    {
        this.securityPlan = securityPlan;
        this.clock = clock;
        this.poolSettings = poolSettings;
        this.logging = logging;
        this.connector = connector;
    }

    @Override
    public Connection acquire( BoltServerAddress address )
    {
        if ( stopped.get() )
        {
            throw new IllegalStateException( "Pool has been closed, cannot acquire new values." );
        }
        BlockingQueue<PooledConnection> connections = pool( address );
        PooledConnection conn = connections.poll();
        if ( conn == null )
        {
            conn = new PooledConnection(connector.connect( address, securityPlan, logging ), new
                    PooledConnectionReleaseConsumer( connections, stopped, poolSettings ), clock);
        }
        conn.updateUsageTimestamp();
        return conn;
    }

    private BlockingQueue<PooledConnection> pool( BoltServerAddress address )
    {
        BlockingQueue<PooledConnection> pool = pools.get( address );
        if ( pool == null )
        {
            pool = new LinkedBlockingQueue<>(poolSettings.maxIdleConnectionPoolSize());
            if ( pools.putIfAbsent( address, pool ) != null )
            {
                // We lost a race to create the pool, dispose of the one we created, and recurse
                return pool( address );
            }
        }
        return pool;
    }

    @Override
    public void close() throws Neo4jException
    {
        if( !stopped.compareAndSet( false, true ) )
        {
            // already closed or some other thread already started close
            return;
        }

        for ( BlockingQueue<PooledConnection> pool : pools.values() )
        {
            while ( !pool.isEmpty() )
            {
                PooledConnection conn = pool.poll();
                if ( conn != null )
                {
                    //close the underlying connection without adding it back to the queue
                    conn.dispose();
                }
            }
        }

        pools.clear();
    }

}
