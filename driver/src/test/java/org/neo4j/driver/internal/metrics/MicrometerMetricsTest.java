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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.net.URI;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MicrometerMetricsTest {
    static final String ID = "id";

    MicrometerMetrics metrics;
    MeterRegistry registry;
    MicrometerConnectionPoolMetrics poolMetrics;

    @BeforeEach
    void beforeEach() {
        registry = new SimpleMeterRegistry();
        metrics = new MicrometerMetrics(registry);
        poolMetrics = mock(MicrometerConnectionPoolMetrics.class);
    }

    @Test
    void shouldReturnEmptyConnectionPoolMetrics() {
        // GIVEN & WHEN
        var collection = metrics.connectionPoolMetrics();

        // THEN
        assertTrue(collection.isEmpty());
    }

    @Test
    void shouldRegisterPoolMetrics() {
        // GIVEN
        var size = metrics.connectionPoolMetrics().size();

        // WHEN
        metrics.registerPoolMetrics(ID, URI.create("bolt://localhost:7687"));

        // THEN
        assertEquals(size + 1, metrics.connectionPoolMetrics().size());
    }

    @Test
    void shouldDeregisterPoolMetrics() {
        // GIVEN
        metrics.putPoolMetrics(ID, poolMetrics);
        var size = metrics.connectionPoolMetrics().size();

        // WHEN
        metrics.deregisterPoolMetrics(ID);

        // THEN
        assertEquals(size - 1, metrics.connectionPoolMetrics().size());
    }
}
