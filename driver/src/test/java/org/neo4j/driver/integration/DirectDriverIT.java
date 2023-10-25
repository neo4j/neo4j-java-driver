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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.driver.internal.util.Matchers.directDriverWithAddress;

import java.net.URI;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.testutil.DatabaseExtension;
import org.neo4j.driver.testutil.ParallelizableIT;

@ParallelizableIT
class DirectDriverIT {
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    private Driver driver;

    @AfterEach
    void closeDriver() {
        if (driver != null) {
            driver.close();
        }
    }

    @Test
    void shouldAllowIPv6Address() {
        // Given
        var uri = URI.create("bolt://[::1]:" + neo4j.boltPort());
        var address = new BoltServerAddress(uri);

        // When
        driver = GraphDatabase.driver(uri, neo4j.authTokenManager());

        // Then
        assertThat(driver, is(directDriverWithAddress(address)));
    }

    @Test
    void shouldRejectInvalidAddress() {
        // Given
        var uri = URI.create("*");

        // When & Then
        @SuppressWarnings("resource")
        var e = assertThrows(IllegalArgumentException.class, () -> GraphDatabase.driver(uri, neo4j.authTokenManager()));
        assertThat(e.getMessage(), equalTo("Scheme must not be null"));
    }

    @Test
    void shouldRegisterSingleServer() {
        // Given
        var uri = neo4j.uri();
        var address = new BoltServerAddress(uri);

        // When
        driver = GraphDatabase.driver(uri, neo4j.authTokenManager());

        // Then
        assertThat(driver, is(directDriverWithAddress(address)));
    }
}
