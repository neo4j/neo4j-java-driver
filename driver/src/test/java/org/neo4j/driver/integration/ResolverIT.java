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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.Logging.none;

import org.junit.jupiter.api.Test;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.net.ServerAddress;
import org.neo4j.driver.net.ServerAddressResolver;

class ResolverIT {
    @Test
    void shouldFailInitialDiscoveryWhenConfiguredResolverThrows() {
        ServerAddressResolver resolver = mock(ServerAddressResolver.class);
        when(resolver.resolve(any(ServerAddress.class))).thenThrow(new RuntimeException("Resolution failure!"));

        Config config = Config.builder()
                .withoutEncryption()
                .withLogging(none())
                .withResolver(resolver)
                .build();
        final Driver driver = GraphDatabase.driver("neo4j://my.server.com:9001", config);

        RuntimeException error = assertThrows(RuntimeException.class, driver::verifyConnectivity);
        assertEquals("Resolution failure!", error.getMessage());
        verify(resolver).resolve(ServerAddress.of("my.server.com", 9001));
    }
}
