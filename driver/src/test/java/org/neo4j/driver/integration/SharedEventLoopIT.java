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

import static org.junit.jupiter.api.Assertions.fail;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.internal.DriverFactory;
import org.neo4j.driver.internal.security.SecurityPlanImpl;
import org.neo4j.driver.testutil.DatabaseExtension;
import org.neo4j.driver.testutil.ParallelizableIT;

@ParallelizableIT
class SharedEventLoopIT {
    private final DriverFactory driverFactory = new DriverFactory();

    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    @Test
    void testDriverShouldNotCloseSharedEventLoop() {
        var eventLoopGroup = new NioEventLoopGroup(1);

        try {
            var driver1 = createDriver(eventLoopGroup);
            var driver2 = createDriver(eventLoopGroup);

            testConnection(driver1);
            testConnection(driver2);

            driver1.close();

            testConnection(driver2);
            driver2.close();
        } finally {
            eventLoopGroup.shutdownGracefully(100, 100, TimeUnit.MILLISECONDS);
        }
    }

    @Test
    void testDriverShouldUseSharedEventLoop() {
        var eventLoopGroup = new NioEventLoopGroup(1);

        var driver = createDriver(eventLoopGroup);
        testConnection(driver);

        eventLoopGroup.shutdownGracefully(100, 100, TimeUnit.MILLISECONDS);

        // the driver should fail if it really uses the provided event loop
        // if the call succeeds, it meas that the driver created its own event loop
        try {
            testConnection(driver);
            fail("Exception expected");
        } catch (Exception e) {
            // ignored
        }
    }

    private Driver createDriver(EventLoopGroup eventLoopGroup) {
        return driverFactory.newInstance(
                neo4j.uri(),
                neo4j.authTokenManager(),
                Config.defaultConfig(),
                SecurityPlanImpl.insecure(),
                eventLoopGroup,
                null);
    }

    private void testConnection(Driver driver) {
        try (var session = driver.session()) {
            session.run("RETURN 1");
        }
    }
}
