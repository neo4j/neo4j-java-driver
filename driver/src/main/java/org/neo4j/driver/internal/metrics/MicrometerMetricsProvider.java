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
import org.neo4j.bolt.connection.MetricsListener;
import org.neo4j.driver.Metrics;

/**
 * An adapter to bridge between driver metrics and Micrometer {@link MeterRegistry meter registry}.
 */
public final class MicrometerMetricsProvider implements MetricsProvider {
    private final MicrometerMetrics metrics;

    public static MetricsProvider forGlobalRegistry() {
        return of(io.micrometer.core.instrument.Metrics.globalRegistry);
    }

    public static MetricsProvider of(MeterRegistry meterRegistry) {
        return new MicrometerMetricsProvider(meterRegistry);
    }

    private MicrometerMetricsProvider(MeterRegistry meterRegistry) {
        this.metrics = new MicrometerMetrics(meterRegistry);
    }

    @Override
    public Metrics metrics() {
        return this.metrics;
    }

    @Override
    public MetricsListener metricsListener() {
        return this.metrics;
    }
}
