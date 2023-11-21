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

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.net.ServerAddress;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class ConfigCustomResolverExample {
    @SuppressWarnings("unused")
    // tag::config-custom-resolver[]
    public void addExampleNode() {
        var addresses = Set.of(
                ServerAddress.of("a.example.com", 7676),
                ServerAddress.of("b.example.com", 8787),
                ServerAddress.of("c.example.com", 9898)
        );
        addNode("neo4j://x.example.com", AuthTokens.basic("neo4j", "some password"), addresses, UUID.randomUUID().toString());
    }

    public void addNode(String virtualUri, AuthToken authToken, Set<ServerAddress> addresses, String id) {
        var config = Config.builder()
                .withResolver(address -> addresses)
                .build();
        try (var driver = GraphDatabase.driver(virtualUri, authToken, config)) {
            driver.executableQuery("CREATE ({id: $id})")
                    .withParameters(Map.of("id", id))
                    .execute();
        }
    }
    // end::config-custom-resolver[]
}
