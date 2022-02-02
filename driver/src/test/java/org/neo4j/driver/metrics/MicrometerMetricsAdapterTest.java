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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.driver.Metrics;

import static org.junit.jupiter.api.Assertions.assertTrue;

class MicrometerMetricsAdapterTest
{
    MicrometerMetricsAdapter provider;
    MeterRegistry registry;

    @BeforeEach
    void beforeEach()
    {
        provider = new MicrometerMetricsAdapter( registry );
    }

    @Test
    void shouldReturnMicrometerMetricsOnMetrics()
    {
        // GIVEN & WHEN
        Metrics metrics = provider.metrics();

        // THEN
        assertTrue( metrics instanceof MicrometerMetrics );
    }

    @Test
    void shouldReturnMicrometerMetricsOnMetricsListener()
    {
        // GIVEN & WHEN
        MetricsListener listener = provider.metricsListener();

        // THEN
        assertTrue( listener instanceof MicrometerMetrics );
    }
}