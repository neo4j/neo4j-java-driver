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
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.async.pool.ConnectionPoolImpl;
import org.neo4j.driver.internal.metrics.ListenerEvent.ConnectionListenerEvent;
import org.neo4j.driver.internal.metrics.ListenerEvent.PoolListenerEvent;
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
        Objects.requireNonNull( config );
        this.config = config;
        this.connectionPoolMetrics = new ConcurrentHashMap<>();
        this.connectionMetrics = new ConcurrentHashMap<>();
    }

    @Override
    public void addMetrics( BoltServerAddress serverAddress, ConnectionPoolImpl pool )
    {
        addPoolMetrics( serverAddress, pool );
        addConnectionMetrics( serverAddress );
    }

    @Override
    public void beforeCreating( BoltServerAddress serverAddress, ConnectionListenerEvent creatingEvent )
    {
        poolMetrics( serverAddress ).beforeCreating();
        connectionMetrics( serverAddress ).beforeCreating( creatingEvent );
    }

    @Override
    public void afterCreating( BoltServerAddress serverAddress, ConnectionListenerEvent creatingEvent )
    {
        connectionMetrics( serverAddress ).afterCreating( creatingEvent );
    }

    @Override
    public void afterCreated( BoltServerAddress serverAddress )
    {
        poolMetrics( serverAddress ).afterCreated();
    }

    @Override
    public void afterFailedToCreate( BoltServerAddress serverAddress )
    {
        poolMetrics( serverAddress ).afterFailedToCreate();
    }

    @Override
    public void afterClosed( BoltServerAddress serverAddress )
    {
        poolMetrics( serverAddress ).afterClosed();
    }

    @Override
    public void beforeAcquiringOrCreating( BoltServerAddress serverAddress, PoolListenerEvent listenerEvent )
    {
        poolMetrics( serverAddress ).beforeAcquiringOrCreating( listenerEvent );
    }

    @Override
    public void afterAcquiringOrCreating( BoltServerAddress serverAddress, PoolListenerEvent listenerEvent )
    {
        poolMetrics( serverAddress ).afterAcquiringOrCreating( listenerEvent );
    }

    @Override
    public void afterAcquiredOrCreated( BoltServerAddress serverAddress, ConnectionListenerEvent inUseEvent )
    {
        connectionMetrics( serverAddress ).acquiredOrCreated( inUseEvent );
    }

    @Override
    public void afterReleased( BoltServerAddress serverAddress, ConnectionListenerEvent inUseEvent )
    {
        connectionMetrics( serverAddress ).released( inUseEvent );
    }

    @Override
    public ConnectionListenerEvent createConnectionListenerEvent()
    {
        return new NanoTimeBasedListenerEvent();
    }

    @Override
    public PoolListenerEvent createPoolListenerEvent()
    {
        return new NanoTimeBasedListenerEvent();
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

    static String serverAddressToUniqueName( BoltServerAddress serverAddress )
    {
        return serverAddress.toString();
    }

    private ConnectionPoolMetricsListener poolMetrics( BoltServerAddress serverAddress )
    {
        InternalConnectionPoolMetrics poolMetrics =
                (InternalConnectionPoolMetrics) this.connectionPoolMetrics.get( serverAddressToUniqueName( serverAddress ) );
        if ( poolMetrics == null )
        {
            throw new ClientException( format( "Failed to find pool metrics for server `%s` in %s", serverAddress, this.connectionPoolMetrics ) );
        }
        return poolMetrics;
    }

    private ConnectionMetricsListener connectionMetrics( BoltServerAddress serverAddress )
    {
        InternalConnectionMetrics connMetrics = (InternalConnectionMetrics) this.connectionMetrics.get( serverAddressToUniqueName( serverAddress ) );
        if ( connMetrics == null )
        {
            throw new ClientException( format( "Failed to find connection metrics for server `%s` in %s", serverAddress, this.connectionMetrics ) );
        }
        return connMetrics;
    }

    private void addPoolMetrics( BoltServerAddress serverAddress, ConnectionPool pool )
    {
        this.connectionPoolMetrics.put( serverAddressToUniqueName( serverAddress ),
                new InternalConnectionPoolMetrics( serverAddress, pool, config.connectionAcquisitionTimeoutMillis() ) );
    }

    private void addConnectionMetrics( BoltServerAddress serverAddress )
    {
        this.connectionMetrics.put( serverAddressToUniqueName( serverAddress ),
                new InternalConnectionMetrics( serverAddress, config.connectionTimeoutMillis() ) );
    }
}
