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
package org.neo4j.driver.internal.net.pooling;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.driver.internal.ConnectionSettings;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.net.ConcurrencyGuardingConnection;
import org.neo4j.driver.internal.net.SocketConnection;
import org.neo4j.driver.internal.security.InternalAuthToken;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;

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
public class SocketConnectionPool implements ConnectionPool
{

    /**
     * Pools, organized by server address.
     */
    private final ConcurrentSkipListMap<BoltServerAddress,BlockingQueue<PooledConnection>> pools = new ConcurrentSkipListMap<>(

            new Comparator<BoltServerAddress>()
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
            } );

    private final Clock clock = Clock.SYSTEM;

    private final ConnectionSettings connectionSettings;
    private final SecurityPlan securityPlan;
    private final PoolSettings poolSettings;
    private final Logging logging;

    private BoltServerAddress current = null;

    /** Shutdown flag */
    private final AtomicBoolean stopped = new AtomicBoolean( false );

    public SocketConnectionPool( ConnectionSettings connectionSettings, SecurityPlan securityPlan,
                                 PoolSettings poolSettings, Logging logging )
    {
        this.connectionSettings = connectionSettings;
        this.securityPlan = securityPlan;
        this.poolSettings = poolSettings;
        this.logging = logging;
    }

    private Connection connect( BoltServerAddress address ) throws ClientException
    {
        Connection conn = new SocketConnection( address, securityPlan, logging );

        // Because SocketConnection is not thread safe, wrap it in this guard
        // to ensure concurrent access leads causes application errors
        conn = new ConcurrencyGuardingConnection( conn );
        conn.init( connectionSettings.userAgent(), tokenAsMap( connectionSettings.authToken() ) );
        return conn;
    }

    private static Map<String,Value> tokenAsMap( AuthToken token )
    {
        if( token instanceof InternalAuthToken )
        {
            return ((InternalAuthToken) token).toMap();
        }
        else
        {
            throw new ClientException( "Unknown authentication token, `" + token + "`. Please use one of the supported " +
                    "tokens from `" + AuthTokens.class.getSimpleName() + "`." );
        }
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
            conn = new PooledConnection( connect( address ), new
                    PooledConnectionReleaseConsumer( connections, stopped, poolSettings ), clock );
        }
        conn.updateUsageTimestamp();
        return conn;
    }

    @Override
    public Connection acquire()
    {
        if ( current == null )
        {
            current = pools.firstKey();
        }
        else
        {
            current = pools.higherKey( current );
            //We've gone through all connections, start over
            if (current == null)
            {
                current = pools.firstKey();
            }
        }

        if ( current == null )
        {
            throw new IllegalStateException( "Cannot acquire connection from an empty pool" );
        }

        return acquire( current );
    }

    @Override
    public boolean isEmpty()
    {
        return pools.isEmpty();
    }

    @Override
    public int addressCount()
    {
        return pools.size();
    }

    @Override
    public void add( BoltServerAddress address )
    {
        pools.putIfAbsent( address, new LinkedBlockingQueue<PooledConnection>(  ) );
    }

    @Override
    public Set<BoltServerAddress> addresses()
    {
        return pools.keySet();
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
    public void purge( BoltServerAddress address )
    {
        BlockingQueue<PooledConnection> connections = pools.remove( address );
        if ( connections == null )
        {
            return;
        }
        while (!connections.isEmpty())
        {
            PooledConnection connection = connections.poll();
            if ( connection != null)
            {
                connection.dispose();
            }
        }
    }

    @Override
    public void close()
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
