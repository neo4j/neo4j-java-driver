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
package org.neo4j.driver.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.driver.Values.parameters;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.MetricsAdapter;
import org.neo4j.driver.QueryRunner;
import org.neo4j.driver.Result;
import org.neo4j.driver.util.DatabaseExtension;
import org.neo4j.driver.util.ParallelizableIT;

@ParallelizableIT
class MetricsIT {
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    private Driver driver;
    private MeterRegistry meterRegistry = new SimpleMeterRegistry();

    @BeforeEach
    void createDriver() {
        driver = GraphDatabase.driver(
                neo4j.uri(),
                neo4j.authToken(),
                Config.builder().withMetricsAdapter(MetricsAdapter.MICROMETER).build());
    }

    @AfterEach
    void closeDriver() {
        driver.close();
    }

    @Test
    void driverMetricsUpdatedWithDriverUse() {
        Result result = createNodesInNewSession(12);
        // assert in use
        Timer acquisitionTimer =
                meterRegistry.get("neo4j.driver.connections.acquisition").timer();
        Timer creationTimer =
                meterRegistry.get("neo4j.driver.connections.creation").timer();
        Timer usageTimer = meterRegistry.get("neo4j.driver.connections.usage").timer();
        assertEquals(1, acquisitionTimer.count());
        assertEquals(1, creationTimer.count());
        assertEquals(0, usageTimer.count());

        result.consume();
        // assert released
        assertEquals(1, acquisitionTimer.count());
        assertEquals(1, creationTimer.count());
        assertEquals(1, usageTimer.count());
    }

    private Result createNodesInNewSession(int nodesToCreate) {
        return createNodes(nodesToCreate, driver.session());
    }

    private Result createNodes(int nodesToCreate, QueryRunner queryRunner) {
        return queryRunner.run(
                "UNWIND range(1, $nodesToCreate) AS i CREATE (n {index: i}) RETURN n",
                parameters("nodesToCreate", nodesToCreate));
    }
}
