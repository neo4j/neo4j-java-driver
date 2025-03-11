/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntSupplier;
import org.neo4j.bolt.connection.BoltServerAddress;
import org.neo4j.bolt.connection.ListenerEvent;
import org.neo4j.bolt.connection.MetricsListener;
import org.neo4j.driver.ConnectionPoolMetrics;
import org.neo4j.driver.Metrics;

final class MicrometerMetrics implements Metrics, MetricsListener {
    private final MeterRegistry meterRegistry;
    private final Map<String, ConnectionPoolMetrics> connectionPoolMetrics;

    public MicrometerMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        this.connectionPoolMetrics = new ConcurrentHashMap<>();
    }

    @Override
    public Collection<ConnectionPoolMetrics> connectionPoolMetrics() {
        return Collections.unmodifiableCollection(this.connectionPoolMetrics.values());
    }

    @Override
    public void beforeCreating(String poolId, ListenerEvent<?> creatingEvent) {
        poolMetricsListener(poolId).beforeCreating(creatingEvent);
    }

    @Override
    public void afterCreated(String poolId, ListenerEvent<?> creatingEvent) {
        poolMetricsListener(poolId).afterCreated(creatingEvent);
    }

    @Override
    public void afterFailedToCreate(String poolId) {
        poolMetricsListener(poolId).afterFailedToCreate();
    }

    @Override
    public void afterClosed(String poolId) {
        poolMetricsListener(poolId).afterClosed();
    }

    @Override
    public void beforeAcquiringOrCreating(String poolId, ListenerEvent<?> acquireEvent) {
        poolMetricsListener(poolId).beforeAcquiringOrCreating(acquireEvent);
    }

    @Override
    public void afterAcquiringOrCreating(String poolId) {
        poolMetricsListener(poolId).afterAcquiringOrCreating();
    }

    @Override
    public void afterAcquiredOrCreated(String poolId, ListenerEvent<?> acquireEvent) {
        poolMetricsListener(poolId).afterAcquiredOrCreated(acquireEvent);
    }

    @Override
    public void afterTimedOutToAcquireOrCreate(String poolId) {
        poolMetricsListener(poolId).afterTimedOutToAcquireOrCreate();
    }

    @Override
    public void afterConnectionCreated(String poolId, ListenerEvent<?> inUseEvent) {
        poolMetricsListener(poolId).acquired(inUseEvent);
    }

    @Override
    public void afterConnectionReleased(String poolId, ListenerEvent<?> inUseEvent) {
        poolMetricsListener(poolId).released(inUseEvent);
    }

    @Override
    public ListenerEvent<?> createListenerEvent() {
        return new MicrometerTimerListenerEvent(this.meterRegistry);
    }

    @Override
    public void registerPoolMetrics(
            String poolId, BoltServerAddress address, IntSupplier inUseSupplier, IntSupplier idleSupplier) {
        this.connectionPoolMetrics.put(
                poolId,
                new MicrometerConnectionPoolMetrics(poolId, address, inUseSupplier, idleSupplier, this.meterRegistry));
    }

    // For testing purposes only
    void putPoolMetrics(String poolId, ConnectionPoolMetrics poolMetrics) {
        this.connectionPoolMetrics.put(poolId, poolMetrics);
    }

    @Override
    public void removePoolMetrics(String poolId) {
        this.connectionPoolMetrics.remove(poolId);
    }

    private ConnectionPoolMetricsListener poolMetricsListener(String poolId) {
        var poolMetrics = (ConnectionPoolMetricsListener) this.connectionPoolMetrics.get(poolId);
        if (poolMetrics == null) {
            return DevNullPoolMetricsListener.INSTANCE;
        }
        return poolMetrics;
    }
}
