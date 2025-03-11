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
package org.neo4j.driver.internal;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.BDDMockito.given;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import javax.net.ssl.SSLContext;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.internal.metrics.MetricsProvider;
import org.neo4j.driver.internal.security.BoltSecurityPlanManager;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.security.StaticAuthTokenManager;

class CustomSecurityPlanTest {
    @Test
    @SuppressWarnings("resource")
    void testCustomSecurityPlanUsed() {
        var driverFactory = new SecurityPlanCapturingDriverFactory();

        var securityPlan = Mockito.mock(SecurityPlan.class);
        given(securityPlan.requiresEncryption()).willReturn(true);
        given(securityPlan.requiresClientAuth()).willReturn(true);
        var sslContext = Mockito.mock(SSLContext.class);
        given(securityPlan.sslContext()).willReturn(CompletableFuture.completedStage(sslContext));
        given(securityPlan.requiresHostnameVerification()).willReturn(true);

        driverFactory.newInstance(
                URI.create("neo4j://somewhere:1234"),
                new StaticAuthTokenManager(AuthTokens.none()),
                null,
                Config.defaultConfig(),
                securityPlan,
                null,
                null);

        assertFalse(driverFactory.capturedSecurityPlans.isEmpty());
        assertTrue(driverFactory.capturedSecurityPlans.stream()
                .allMatch(capturePlan -> capturePlan.requiresEncryption()
                        && capturePlan.requiresClientAuth()
                        && capturePlan.sslContext() == sslContext
                        && capturePlan.requiresHostnameVerification()));
    }

    private static class SecurityPlanCapturingDriverFactory extends DriverFactory {
        final List<org.neo4j.bolt.connection.SecurityPlan> capturedSecurityPlans = new ArrayList<>();

        @Override
        protected InternalDriver createDriver(
                BoltSecurityPlanManager securityPlanManager,
                SessionFactory sessionFactory,
                MetricsProvider metricsProvider,
                Supplier<CompletionStage<Void>> shutdownSupplier,
                Config config) {
            capturedSecurityPlans.add(
                    securityPlanManager.plan().toCompletableFuture().join());
            return super.createDriver(securityPlanManager, sessionFactory, metricsProvider, shutdownSupplier, config);
        }
    }
}
