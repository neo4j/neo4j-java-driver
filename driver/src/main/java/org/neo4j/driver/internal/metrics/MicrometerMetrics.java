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

import io.micrometer.core.instrument.MeterRegistry;
import org.neo4j.driver.ConnectionPoolMetrics;
import org.neo4j.driver.Metrics;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.async.pool.ConnectionPoolImpl;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MicrometerMetrics implements Metrics, MetricsListener {

    private final MeterRegistry meterRegistry;
    private final Map<String,ConnectionPoolMetrics> connectionPoolMetrics;

    public MicrometerMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        this.connectionPoolMetrics = new ConcurrentHashMap<>();
    }

    @Override
    public Collection<ConnectionPoolMetrics> connectionPoolMetrics() {
        return Collections.unmodifiableCollection(this.connectionPoolMetrics.values());
    }

    @Override
    public void beforeCreating(String poolId, ListenerEvent creatingEvent) {
        poolMetrics(poolId).beforeCreating(creatingEvent);
    }

    @Override
    public void afterCreated(String poolId, ListenerEvent creatingEvent) {
        poolMetrics(poolId).afterCreated(creatingEvent);
    }

    @Override
    public void afterFailedToCreate(String poolId) {
        poolMetrics(poolId).afterFailedToCreate();
    }

    @Override
    public void afterClosed(String poolId) {
        poolMetrics(poolId).afterClosed();
    }

    @Override
    public void beforeAcquiringOrCreating(String poolId, ListenerEvent acquireEvent) {
        poolMetrics(poolId).beforeAcquiringOrCreating(acquireEvent);
    }

    @Override
    public void afterAcquiringOrCreating(String poolId) {
        poolMetrics(poolId).afterAcquiringOrCreating();
    }

    @Override
    public void afterAcquiredOrCreated(String poolId, ListenerEvent acquireEvent) {
        poolMetrics(poolId).afterAcquiredOrCreated(acquireEvent);
    }

    @Override
    public void afterTimedOutToAcquireOrCreate(String poolId) {
        poolMetrics(poolId).afterTimedOutToAcquireOrCreate();
    }

    @Override
    public void afterConnectionCreated(String poolId, ListenerEvent inUseEvent) {
        poolMetrics(poolId).acquired(inUseEvent);
    }

    @Override
    public void afterConnectionReleased(String poolId, ListenerEvent inUseEvent) {
        poolMetrics(poolId).released(inUseEvent);
    }

    @Override
    public ListenerEvent createListenerEvent() {
        return new MicrometerTimerListenerEvent(this.meterRegistry);
    }

    @Override
    public void putPoolMetrics(String poolId, BoltServerAddress address, ConnectionPoolImpl connectionPool) {
        this.connectionPoolMetrics.put(poolId, new MicrometerConnectionPoolMetrics(poolId, address, connectionPool, this.meterRegistry));
    }

    @Override
    public void removePoolMetrics(String poolId) {
        // TODO should we unregister metrics registered for this poolId?
        this.connectionPoolMetrics.remove(poolId);
    }

    private ConnectionPoolMetricsListener poolMetrics( String poolId )
    {
        InternalConnectionPoolMetrics poolMetrics = (InternalConnectionPoolMetrics) this.connectionPoolMetrics.get( poolId );
        if ( poolMetrics == null )
        {
            return DEV_NULL_POOL_METRICS_LISTENER;
        }
        return poolMetrics;
    }

    ConnectionPoolMetricsListener DEV_NULL_POOL_METRICS_LISTENER = new ConnectionPoolMetricsListener()
    {
        @Override
        public void beforeCreating( ListenerEvent listenerEvent )
        {
        }

        @Override
        public void afterCreated( ListenerEvent listenerEvent )
        {
        }

        @Override
        public void afterFailedToCreate()
        {
        }

        @Override
        public void afterClosed()
        {
        }

        @Override
        public void beforeAcquiringOrCreating( ListenerEvent acquireEvent )
        {
        }

        @Override
        public void afterAcquiringOrCreating()
        {
        }

        @Override
        public void afterAcquiredOrCreated( ListenerEvent acquireEvent )
        {
        }

        @Override
        public void afterTimedOutToAcquireOrCreate()
        {
        }

        @Override
        public void acquired( ListenerEvent inUseEvent )
        {
        }

        @Override
        public void released( ListenerEvent inUseEvent )
        {
        }
    };
}
