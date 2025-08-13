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
package org.neo4j.driver.observation.metrics;

import java.time.Clock;
import org.neo4j.driver.Config;
import org.neo4j.driver.observation.ObservationProvider;
import org.neo4j.driver.observation.metrics.internal.DriverMetricsObservationProvider;
import org.neo4j.driver.util.Preview;

/**
 * An {@link ObservationProvider} implementation that provides {@link Metrics} implementation.
 * @since 6.0.0
 */
@Preview(name = "Observability")
public sealed interface MetricsObservationProvider extends ObservationProvider
        permits DriverMetricsObservationProvider {
    /**
     * Creates a new {@link MetricsObservationProvider} instance.
     * <p>
     * To enable metrics, register the returned instance using {@link org.neo4j.driver.Config.ConfigBuilder#withObservationProvider(ObservationProvider)}.
     * @return the new {@link MetricsObservationProvider} instance
     */
    static MetricsObservationProvider newInstance() {
        return new DriverMetricsObservationProvider(Clock.systemDefaultZone());
    }

    /**
     * Creates a new {@link MetricsObservationProvider} instance and registers it with the provided {@link org.neo4j.driver.Config.ConfigBuilder}.
     * @param configBuilder the config builder
     * @return the new {@link MetricsObservationProvider} instance
     */
    static MetricsObservationProvider newInstance(Config.ConfigBuilder configBuilder) {
        var provider = newInstance();
        configBuilder.withObservationProvider(provider);
        return provider;
    }

    /**
     * Returns {@link Metrics} instance managed by this provider.
     * @return the {@link Metrics} instance
     */
    Metrics metrics();
}
