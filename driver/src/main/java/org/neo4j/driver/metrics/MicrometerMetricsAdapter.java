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
package org.neo4j.driver.metrics;

import io.micrometer.core.instrument.MeterRegistry;

import org.neo4j.driver.Metrics;
import org.neo4j.driver.MetricsAdapter;

/**
 * An adapter to bridge between the driver metrics and a Micrometer {@link MeterRegistry meter registry}.
 */
public final class MicrometerMetricsAdapter implements MetricsAdapter
{
    private final MicrometerMetrics metrics;

    public MicrometerMetricsAdapter( MeterRegistry meterRegistry )
    {
        this.metrics = new MicrometerMetrics( meterRegistry );
    }

    @Override
    public Metrics metrics()
    {
        return this.metrics;
    }

    @Override
    public MetricsListener metricsListener()
    {
        return this.metrics;
    }
}