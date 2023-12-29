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
package org.neo4j.driver.internal.bolt.api;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.internal.DriverFactory;
import org.neo4j.driver.internal.InternalDriver;
import org.neo4j.driver.internal.SessionFactory;
import org.neo4j.driver.internal.metrics.MetricsProvider;
import org.neo4j.driver.internal.security.StaticAuthTokenManager;

class CustomSecurityPlanTest {
    @Test
    @SuppressWarnings("resource")
    void testCustomSecurityPlanUsed() {
        var driverFactory = new SecurityPlanCapturingDriverFactory();

        var securityPlan = Mockito.mock(SecurityPlan.class);

        driverFactory.newInstance(
                URI.create("neo4j://somewhere:1234"),
                new StaticAuthTokenManager(AuthTokens.none()),
                Config.defaultConfig(),
                securityPlan,
                null);

        assertFalse(driverFactory.capturedSecurityPlans.isEmpty());
        assertTrue(driverFactory.capturedSecurityPlans.stream().allMatch(capturePlan -> capturePlan == securityPlan));
    }

    private static class SecurityPlanCapturingDriverFactory extends DriverFactory {
        final List<SecurityPlan> capturedSecurityPlans = new ArrayList<>();

        @Override
        protected InternalDriver createDriver(
                SecurityPlan securityPlan,
                SessionFactory sessionFactory,
                MetricsProvider metricsProvider,
                Config config) {
            capturedSecurityPlans.add(securityPlan);
            return super.createDriver(securityPlan, sessionFactory, metricsProvider, config);
        }
    }
}
