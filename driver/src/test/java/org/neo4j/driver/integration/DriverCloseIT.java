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
import static org.neo4j.driver.SessionConfig.builder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.util.DatabaseExtension;
import org.neo4j.driver.util.ParallelizableIT;

@ParallelizableIT
class DriverCloseIT {
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    @Test
    void isEncryptedThrowsForClosedDriver() {
        Driver driver = createDriver();

        driver.close();

        assertThrows(IllegalStateException.class, driver::isEncrypted);
    }

    @Test
    void sessionThrowsForClosedDriver() {
        Driver driver = createDriver();

        driver.close();

        assertThrows(IllegalStateException.class, driver::session);
    }

    @Test
    void sessionWithModeThrowsForClosedDriver() {
        Driver driver = createDriver();

        driver.close();

        assertThrows(
                IllegalStateException.class,
                () -> driver.session(
                        builder().withDefaultAccessMode(AccessMode.WRITE).build()));
    }

    @Test
    void closeClosedDriver() {
        Driver driver = createDriver();

        driver.close();
        driver.close();
        driver.close();
    }

    @Test
    void useSessionAfterDriverIsClosed() {
        Driver driver = createDriver();
        Session session = driver.session();

        driver.close();

        assertThrows(IllegalStateException.class, () -> session.run("CREATE ()"));
    }

    @Test
    void shouldInterruptStreamConsumptionAndEndRetriesOnDriverClosure() {
        int fetchSize = 5;
        Config config = Config.builder().withFetchSize(fetchSize).build();
        Driver driver = GraphDatabase.driver(neo4j.uri(), neo4j.authToken(), config);
        Session session = driver.session();

        IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> session.readTransaction(tx -> {
                    Map<String, Object> parameters = new HashMap<>();
                    parameters.put("limit", fetchSize * 3);
                    Result result = tx.run("UNWIND range(0, $limit) AS x RETURN x", parameters);
                    CompletableFuture.runAsync(driver::close);
                    return result.list();
                }));
        assertEquals(ConnectionPool.CONNECTION_POOL_CLOSED_ERROR_MESSAGE, exception.getMessage());
    }

    private static Driver createDriver() {
        return GraphDatabase.driver(neo4j.uri(), neo4j.authToken());
    }
}
