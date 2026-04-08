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
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.neo4j.driver.ConnectionPoolMetrics;
import org.neo4j.driver.Metrics;

final class MicrometerMetrics implements Metrics {
    private final MeterRegistry meterRegistry;
    private final Map<String, MicrometerConnectionPoolMetrics> connectionPoolMetrics;

    public MicrometerMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        this.connectionPoolMetrics = new ConcurrentHashMap<>();
    }

    @Override
    public Collection<ConnectionPoolMetrics> connectionPoolMetrics() {
        return Collections.unmodifiableCollection(this.connectionPoolMetrics.values());
    }

    public void registerPoolMetrics(String poolId, URI uri) {
        this.connectionPoolMetrics.put(poolId, new MicrometerConnectionPoolMetrics(poolId, uri, this.meterRegistry));
    }

    // For testing purposes only
    void putPoolMetrics(String poolId, MicrometerConnectionPoolMetrics poolMetrics) {
        this.connectionPoolMetrics.put(poolId, poolMetrics);
    }

    public void deregisterPoolMetrics(String poolId) {
        this.connectionPoolMetrics.remove(poolId);
    }

    MicrometerConnectionPoolMetrics getConnectionPoolMetrics(String poolId) {
        return this.connectionPoolMetrics.get(poolId);
    }
}
