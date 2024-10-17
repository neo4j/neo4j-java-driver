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
package org.neo4j.driver.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.MetricsAdapter;
import org.neo4j.driver.testutil.DatabaseExtension;
import org.neo4j.driver.testutil.ParallelizableIT;

@ParallelizableIT
class MetricsIT {
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    private final MeterRegistry meterRegistry = Metrics.globalRegistry;
    private final long fetchSize = 5;

    private Driver driver;

    @BeforeEach
    void createDriver() {
        Metrics.addRegistry(new SimpleMeterRegistry());
        var config = Config.builder()
                .withFetchSize(fetchSize)
                .withMetricsAdapter(MetricsAdapter.MICROMETER)
                .build();
        driver = neo4j.customDriver(config);
    }

    @AfterEach
    void closeDriver() {
        driver.close();
    }

    @Test
    void driverMetricsUpdatedWithDriverUse() {
        try (var session = driver.session()) {
            var result = session.run("UNWIND range(1, $limit) AS x RETURN x", Map.of("limit", fetchSize + 1));
            // assert in use
            var acquisitionTimer =
                    meterRegistry.get("neo4j.driver.connections.acquisition").timer();
            var creationTimer =
                    meterRegistry.get("neo4j.driver.connections.creation").timer();
            var usageTimer = meterRegistry.get("neo4j.driver.connections.usage").timer();
            assertEquals(1, acquisitionTimer.count());
            assertEquals(1, creationTimer.count());
            assertEquals(0, usageTimer.count());

            result.consume();
            // todo chain close futures to fix this
            Thread.sleep(1000);
            // assert released
            assertEquals(1, acquisitionTimer.count());
            assertEquals(1, creationTimer.count());
            assertEquals(1, usageTimer.count());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
