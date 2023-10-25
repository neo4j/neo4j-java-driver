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
package org.neo4j.driver.tck.reactive;

import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.testcontainers.containers.Neo4jContainer;
import org.testng.SkipException;

public class Neo4jManager {
    @SuppressWarnings("resource")
    private final Neo4jContainer<?> NEO4J = new Neo4jContainer<>("neo4j:4.4").withAdminPassword(null);

    public void start() {
        NEO4J.start();
    }

    public void stop() {
        NEO4J.stop();
    }

    public Driver getDriver() {
        return GraphDatabase.driver(NEO4J.getBoltUrl());
    }

    public void skipIfDockerTestsSkipped() {
        var skip = System.getProperty("skipDockerTests");
        if (skip != null && skip.equals(Boolean.TRUE.toString())) {
            throw new SkipException("Docker is unavailable");
        }
    }
}
