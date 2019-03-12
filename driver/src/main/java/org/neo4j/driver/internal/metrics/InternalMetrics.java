/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import org.neo4j.driver.ConnectionPoolMetrics;
import org.neo4j.driver.Metrics;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.exceptions.ClientException;

import static java.lang.String.format;
import static java.util.Collections.unmodifiableMap;

public class InternalMetrics extends InternalAbstractMetrics
{
    private final Map<String,ConnectionPoolMetrics> connectionPoolMetrics;
    private final Clock clock;

    public InternalMetrics( Clock clock )
    {
        Objects.requireNonNull( clock );
        this.connectionPoolMetrics = new ConcurrentHashMap<>();
        this.clock = clock;
    }

    @Override
    public void putPoolMetrics( BoltServerAddress serverAddress, ConnectionPoolImpl pool )
    {
        this.connectionPoolMetrics.put( serverAddressToUniqueName( serverAddress ),
                new InternalConnectionPoolMetrics( serverAddress, pool ) );
    }

    @Override
    public void beforeCreating( BoltServerAddress serverAddress, ListenerEvent creatingEvent )
    {
        poolMetrics( serverAddress ).beforeCreating( creatingEvent );
    }

    @Override
    public void afterCreated( BoltServerAddress serverAddress, ListenerEvent creatingEvent )
    {
        poolMetrics( serverAddress ).afterCreated( creatingEvent );
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
    public void beforeAcquiringOrCreating( BoltServerAddress serverAddress, ListenerEvent acquireEvent )
    {
        poolMetrics( serverAddress ).beforeAcquiringOrCreating( acquireEvent );
    }

    @Override
    public void afterAcquiringOrCreating( BoltServerAddress serverAddress )
    {
        poolMetrics( serverAddress ).afterAcquiringOrCreating();
    }

    @Override
    public void afterAcquiredOrCreated( BoltServerAddress serverAddress, ListenerEvent acquireEvent )
    {
        poolMetrics( serverAddress ).afterAcquiredOrCreated( acquireEvent );
    }

    @Override
    public void afterConnectionCreated( BoltServerAddress serverAddress, ListenerEvent inUseEvent )
    {
        poolMetrics( serverAddress ).acquired( inUseEvent );
    }

    @Override
    public void afterConnectionReleased( BoltServerAddress serverAddress, ListenerEvent inUseEvent )
    {
        poolMetrics( serverAddress ).released( inUseEvent );
    }

    @Override
    public void afterTimedOutToAcquireOrCreate( BoltServerAddress serverAddress )
    {
        poolMetrics( serverAddress ).afterTimedOutToAcquireOrCreate();
    }

    @Override
    public ListenerEvent createListenerEvent()
    {
        return new TimeRecorderListenerEvent( clock );
    }

    @Override
    public Map<String,ConnectionPoolMetrics> connectionPoolMetrics()
    {
        return unmodifiableMap( this.connectionPoolMetrics );
    }

    @Override
    public Metrics snapshot()
    {
        return new SnapshotMetrics( this );
    }

    @Override
    public String toString()
    {
        return format( "PoolMetrics=%s", connectionPoolMetrics );
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
}
