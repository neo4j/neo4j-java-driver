/*
 * Copyright (c) "Neo4j"
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

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.driver.ConnectionPoolMetrics;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.async.pool.ConnectionPoolImpl;
import org.neo4j.driver.internal.util.Clock;

import static java.lang.String.format;
import static java.util.Collections.unmodifiableCollection;
import static org.neo4j.driver.internal.metrics.ConnectionPoolMetricsListener.DEV_NULL_POOL_METRICS_LISTENER;

public class InternalMetrics extends InternalAbstractMetrics
{
    private final Map<String,ConnectionPoolMetrics> connectionPoolMetrics;
    private final Clock clock;
    private final Logger log;

    InternalMetrics( Clock clock, Logging logging )
    {
        Objects.requireNonNull( clock );
        this.connectionPoolMetrics = new ConcurrentHashMap<>();
        this.clock = clock;
        this.log = logging.getLog( getClass().getSimpleName() );
    }

    @Override
    public void putPoolMetrics( String poolId, BoltServerAddress serverAddress, ConnectionPoolImpl pool )
    {
        this.connectionPoolMetrics.put( poolId, new InternalConnectionPoolMetrics( poolId, serverAddress, pool ) );
    }

    @Override
    public void removePoolMetrics( String id )
    {
        this.connectionPoolMetrics.remove( id );
    }

    @Override
    public void beforeCreating( String poolId, ListenerEvent creatingEvent )
    {
        poolMetrics( poolId ).beforeCreating( creatingEvent );
    }

    @Override
    public void afterCreated( String poolId, ListenerEvent creatingEvent )
    {
        poolMetrics( poolId ).afterCreated( creatingEvent );
    }

    @Override
    public void afterFailedToCreate( String poolId )
    {
        poolMetrics( poolId ).afterFailedToCreate();
    }

    @Override
    public void afterClosed( String poolId )
    {
        poolMetrics( poolId ).afterClosed();
    }

    @Override
    public void beforeAcquiringOrCreating( String poolId, ListenerEvent acquireEvent )
    {
        poolMetrics( poolId ).beforeAcquiringOrCreating( acquireEvent );
    }

    @Override
    public void afterAcquiringOrCreating( String poolId )
    {
        poolMetrics( poolId ).afterAcquiringOrCreating();
    }

    @Override
    public void afterAcquiredOrCreated( String poolId, ListenerEvent acquireEvent )
    {
        poolMetrics( poolId ).afterAcquiredOrCreated( acquireEvent );
    }

    @Override
    public void afterConnectionCreated( String poolId, ListenerEvent inUseEvent )
    {
        poolMetrics( poolId ).acquired( inUseEvent );
    }

    @Override
    public void afterConnectionReleased( String poolId, ListenerEvent inUseEvent )
    {
        poolMetrics( poolId ).released( inUseEvent );
    }

    @Override
    public void afterTimedOutToAcquireOrCreate( String poolId )
    {
        poolMetrics( poolId ).afterTimedOutToAcquireOrCreate();
    }

    @Override
    public ListenerEvent createListenerEvent()
    {
        return new TimeRecorderListenerEvent( clock );
    }

    @Override
    public Collection<ConnectionPoolMetrics> connectionPoolMetrics()
    {
        return unmodifiableCollection( this.connectionPoolMetrics.values() );
    }

    @Override
    public String toString()
    {
        return format( "PoolMetrics=%s", connectionPoolMetrics );
    }

    private ConnectionPoolMetricsListener poolMetrics( String poolId )
    {
        InternalConnectionPoolMetrics poolMetrics = (InternalConnectionPoolMetrics) this.connectionPoolMetrics.get( poolId );
        if ( poolMetrics == null )
        {
            log.warn( format( "Failed to find pool metrics with id `%s` in %s.", poolId, this.connectionPoolMetrics ) );
            return DEV_NULL_POOL_METRICS_LISTENER;
        }
        return poolMetrics;
    }
}
