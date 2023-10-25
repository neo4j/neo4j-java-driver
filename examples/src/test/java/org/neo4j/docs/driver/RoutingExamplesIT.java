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
package org.neo4j.docs.driver;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.internal.util.EnabledOnNeo4jWith;
import org.neo4j.driver.internal.util.Neo4jFeature;
import org.neo4j.driver.net.ServerAddress;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers(disabledWithoutDocker = true)
@DisabledIfSystemProperty(named = "skipDockerTests", matches = "^true$")
class RoutingExamplesIT {
    private static final String NEO4J_VERSION =
            Optional.ofNullable(System.getenv("NEO4J_VERSION")).orElse("4.4");

    @Container
    @SuppressWarnings("resource")
    private static final Neo4jContainer<?> NEO4J_CONTAINER = new Neo4jContainer<>(
                    String.format("neo4j:%s-enterprise", NEO4J_VERSION))
            .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
            // in this testing deployment the server runs inside a container and its Bolt port is exposed to the test(s)
            // on a random port that might not match the port in the routing table
            // this setting leads to the server echoing back the routing context address supplied by the driver
            // the test(s) may define the routing context address via the URI
            .withNeo4jConfig("dbms.routing.default_router", "SERVER")
            .withAdminPassword(null);

    @Test
    @EnabledOnNeo4jWith(Neo4jFeature.SERVER_SIDE_ROUTING_ENABLED_BY_DEFAULT)
    void testShouldRunConfigCustomResolverExample() {
        // Given
        var boltUri = URI.create(NEO4J_CONTAINER.getBoltUrl());
        var id = UUID.randomUUID().toString();
        var example = new ConfigCustomResolverExample();
        var neo4j = String.format("neo4j://%s:%d", boltUri.getHost(), boltUri.getPort());

        // When
        example.addNode(neo4j, AuthTokens.none(), Set.of(ServerAddress.of(boltUri.getHost(), boltUri.getPort())), id);

        // Then
        try(var driver = GraphDatabase.driver(boltUri, AuthTokens.none())) {
            var num = driver.executableQuery("MATCH (n{id: $id}) RETURN count(n)")
                    .withParameters(Map.of("id", id))
                    .execute();
            assertEquals(1, num.records().get(0).get(0).asInt());
        }
    }
}
