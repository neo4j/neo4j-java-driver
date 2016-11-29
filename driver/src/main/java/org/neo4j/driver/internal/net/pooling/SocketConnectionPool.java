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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.driver.internal.ConnectionSettings;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.net.ConcurrencyGuardingConnection;
import org.neo4j.driver.internal.net.SocketConnection;
import org.neo4j.driver.internal.security.InternalAuthToken;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.Supplier;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;

import static java.util.Collections.emptyList;

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
    private final ConcurrentHashMap<BoltServerAddress,BlockingPooledConnectionQueue> pools =
            new ConcurrentHashMap<>();

    private final Clock clock = Clock.SYSTEM;

    private final ConnectionSettings connectionSettings;
    private final SecurityPlan securityPlan;
    private final PoolSettings poolSettings;
    private final Logging logging;

    /** Shutdown flag */

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
        if ( token instanceof InternalAuthToken )
        {
            return ((InternalAuthToken) token).toMap();
        }
        else
        {
            throw new ClientException(
                    "Unknown authentication token, `" + token + "`. Please use one of the supported " +
                    "tokens from `" + AuthTokens.class.getSimpleName() + "`." );
        }
    }

    @Override
    public Connection acquire( final BoltServerAddress address )
    {
        final BlockingPooledConnectionQueue connections = pool( address );
        Supplier<PooledConnection> supplier = new Supplier<PooledConnection>()
        {
            @Override
            public PooledConnection get()
            {
                PooledConnectionValidator connectionValidator =
                        new PooledConnectionValidator( SocketConnectionPool.this );
                PooledConnectionReleaseConsumer releaseConsumer =
                        new PooledConnectionReleaseConsumer( connections, connectionValidator );
                return new PooledConnection( connect( address ), releaseConsumer, clock );
            }
        };
        PooledConnection conn = connections.acquire( supplier );
        conn.updateTimestamp();
        return conn;
    }

    private BlockingPooledConnectionQueue pool( BoltServerAddress address )
    {
        BlockingPooledConnectionQueue pool = pools.get( address );
        if ( pool == null )
        {
            pool = new BlockingPooledConnectionQueue( poolSettings.maxIdleConnectionPoolSize() );

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
        BlockingPooledConnectionQueue connections = pools.remove( address );
        if ( connections == null )
        {
            return;
        }

        connections.terminate();
    }

    @Override
    public boolean hasAddress( BoltServerAddress address )
    {
        return pools.containsKey( address );
    }

    @Override
    public void close()
    {
        for ( BlockingPooledConnectionQueue pool : pools.values() )
        {
            pool.terminate();
        }

        pools.clear();
    }


    //for testing
    public List<PooledConnection> connectionsForAddress( BoltServerAddress address )
    {
        BlockingPooledConnectionQueue pooledConnections = pools.get( address );
        if ( pooledConnections == null )
        {
            return emptyList();
        }
        else
        {
            return pooledConnections.toList();
        }
    }


}
