/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.driver.internal.metrics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.metrics.spi.ConnectionMetrics;
import org.neo4j.driver.internal.metrics.spi.ConnectionPoolMetrics;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.exceptions.ClientException;

import static java.lang.String.format;

public class InternalDriverMetrics extends InternalAbstractDriverMetrics
{
    private final Map<String,ConnectionPoolMetrics> connectionPoolMetrics;
    private final Map<String,ConnectionMetrics> connectionMetrics;
    private final Config config;

    public InternalDriverMetrics( Config config )
    {
        this.config = config;
        this.connectionPoolMetrics = new ConcurrentHashMap<>();
        this.connectionMetrics = new ConcurrentHashMap<>();
    }

    @Override
    public void addPoolMetrics( BoltServerAddress serverAddress, ConnectionPool pool )
    {
        this.connectionPoolMetrics.put( serverAddressToUniqueName( serverAddress ),
                new InternalConnectionPoolMetrics( serverAddress, pool, config.connectionAcquisitionTimeoutMillis() ) );
    }

    @Override
    public void beforeCreating( BoltServerAddress serverAddress )
    {
        InternalConnectionPoolMetrics poolMetrics = getPoolMetrics( serverAddress );
        poolMetrics.beforeCreating();
    }

    @Override
    public void afterCreatedSuccessfully( BoltServerAddress serverAddress )
    {
        InternalConnectionPoolMetrics poolMetrics = getPoolMetrics( serverAddress );
        poolMetrics.afterCreatedSuccessfully();
    }

    @Override
    public void afterFailedToCreate( BoltServerAddress serverAddress )
    {
        InternalConnectionPoolMetrics poolMetrics = getPoolMetrics( serverAddress );
        poolMetrics.afterFailedToCreate();
    }

    @Override
    public void afterClosed( BoltServerAddress serverAddress )
    {
        InternalConnectionPoolMetrics poolMetrics = getPoolMetrics( serverAddress );
        poolMetrics.afterClosed();
    }

    @Override
    public void beforeAcquiring( BoltServerAddress serverAddress, ListenerEvent listenerEvent )
    {
        InternalConnectionPoolMetrics poolMetrics = getPoolMetrics( serverAddress );
        poolMetrics.beforeAcquire( listenerEvent );
    }

    @Override
    public void afterAcquired( BoltServerAddress serverAddress, ListenerEvent listenerEvent )
    {
        InternalConnectionPoolMetrics poolMetrics = getPoolMetrics( serverAddress );
        poolMetrics.afterAcquire( listenerEvent );
    }

    @Override
    public Map<String,ConnectionPoolMetrics> connectionPoolMetrics()
    {
        return this.connectionPoolMetrics;
    }

    @Override
    public Map<String,ConnectionMetrics> connectionMetrics()
    {
        return this.connectionMetrics;
    }

    @Override
    public String toString()
    {
        return format( "PoolMetrics=%s, ConnMetrics=%s", connectionPoolMetrics, connectionMetrics );
    }

    public static String serverAddressToUniqueName( BoltServerAddress serverAddress )
    {
        return serverAddress.toString();
    }

    private InternalConnectionPoolMetrics getPoolMetrics( BoltServerAddress serverAddress )
    {
        InternalConnectionPoolMetrics poolMetrics =
                (InternalConnectionPoolMetrics) this.connectionPoolMetrics.get( serverAddressToUniqueName( serverAddress ) );
        if ( poolMetrics == null )
        {
            throw new ClientException( format( "Failed to find pool metrics for server %s in pool %s", serverAddress, this.connectionPoolMetrics ) );
        }
        return poolMetrics;
    }
}
