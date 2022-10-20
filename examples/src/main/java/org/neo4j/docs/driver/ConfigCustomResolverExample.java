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
package org.neo4j.docs.driver;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.net.ServerAddress;

import java.util.Arrays;
import java.util.HashSet;

import static org.neo4j.driver.SessionConfig.builder;
import static org.neo4j.driver.Values.parameters;

public class ConfigCustomResolverExample implements AutoCloseable {
    private final Driver driver;

    public ConfigCustomResolverExample(String virtualUri, AuthToken authToken, ServerAddress... addresses) {
        var config = Config.builder()
                .withResolver(address -> new HashSet<>(Arrays.asList(addresses)))
                .build();

        driver = GraphDatabase.driver(virtualUri, authToken, config);
    }

    // tag::config-custom-resolver[]
    private Driver createDriver(String virtualUri, String user, String password, ServerAddress... addresses) {
        var config = Config.builder()
                .withResolver(address -> new HashSet<>(Arrays.asList(addresses)))
                .build();

        return GraphDatabase.driver(virtualUri, AuthTokens.basic(user, password), config);
    }

    private void addPerson(String name) {
        var username = "neo4j";
        var password = "some password";

        try (var driver = createDriver(
                "neo4j://x.example.com",
                username,
                password,
                ServerAddress.of("a.example.com", 7676),
                ServerAddress.of("b.example.com", 8787),
                ServerAddress.of("c.example.com", 9898))) {
            try (var session = driver.session(builder().withDefaultAccessMode(AccessMode.WRITE).build())) {
                session.run("CREATE (a:Person {name: $name})", parameters("name", name));
            }
        }
    }
    // end::config-custom-resolver[]

    @Override
    public void close() throws RuntimeException {
        driver.close();
    }

    public boolean canConnect() {
        var result = driver.session(builder().withDefaultAccessMode(AccessMode.READ).build()).run("RETURN 1");
        return result.single().get(0).asInt() == 1;
    }
}
