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
import org.neo4j.driver.Metrics;
import org.neo4j.driver.internal.observation.DriverObservationProvider;
import org.neo4j.driver.internal.observation.NoopObservation;
import org.neo4j.driver.internal.observation.Observation;

public final class MicrometerMetricsObservationProvider implements DriverObservationProvider {
    private final MeterRegistry registry;
    private final MicrometerMetrics metrics;

    public MicrometerMetricsObservationProvider() {
        this.registry = io.micrometer.core.instrument.Metrics.globalRegistry;
        this.metrics = new MicrometerMetrics(registry);
    }

    public Metrics metrics() {
        return metrics;
    }

    @Override
    public Observation connectionPoolCreate(String id, URI uri, int maxSize) {
        return new MicrometerPoolCreateObservation(metrics, id, uri);
    }

    @Override
    public Observation connectionPoolClose(String id, URI uri) {
        return new MicrometerPoolCloseObservation(metrics, id);
    }

    @Override
    public Observation pooledConnectionCreate(String id, URI uri) {
        var poolMetrics = metrics.getConnectionPoolMetrics(id);
        return new MicrometerPoolConnectionCreateObservation(poolMetrics, registry);
    }

    @Override
    public Observation pooledConnectionClose(String id, URI uri) {
        var poolMetrics = metrics.getConnectionPoolMetrics(id);
        return new MicrometerPoolConnectionCloseObservation(poolMetrics);
    }

    @Override
    public Observation pooledConnectionAcquire(String id, URI uri) {
        var poolMetrics = metrics.getConnectionPoolMetrics(id);
        return new MicrometerPoolConnectionAcquireObservation(poolMetrics, registry);
    }

    @Override
    public Observation pooledConnectionInUse(String id, URI uri) {
        var poolMetrics = metrics.getConnectionPoolMetrics(id);
        return new MicrometerPoolConnectionInUseObservation(poolMetrics, registry);
    }

    @Override
    public Observation scopedObservation() {
        return NoopObservation.getInstance();
    }
}
