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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.Config.defaultConfig;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;

import java.net.URI;
import java.time.Clock;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.driver.AuthTokenManager;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Logging;
import org.neo4j.driver.MetricsAdapter;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.internal.adaptedbolt.DriverBoltConnectionSource;
import org.neo4j.driver.internal.async.LeakLoggingNetworkSession;
import org.neo4j.driver.internal.async.NetworkSession;
import org.neo4j.driver.internal.homedb.HomeDatabaseCache;
import org.neo4j.driver.internal.metrics.DriverMetricsObservationProvider;
import org.neo4j.driver.internal.metrics.MicrometerMetricsObservationProvider;
import org.neo4j.driver.internal.observation.NoopObservationProvider;
import org.neo4j.driver.internal.retry.RetryLogic;
import org.neo4j.driver.internal.security.BoltSecurityPlanManager;
import org.neo4j.driver.internal.security.StaticAuthTokenManager;

class DriverFactoryTest {
    private static Stream<String> testUris() {
        return Stream.of("bolt://localhost:7687", "neo4j://localhost:7687");
    }

    @ParameterizedTest
    @MethodSource("testUris")
    @SuppressWarnings("resource")
    void usesStandardSessionFactoryWhenNothingConfigured(String uri) {
        var config = defaultConfig();
        var factory = new SessionFactoryCapturingDriverFactory();

        createDriver(uri, factory, config);

        var capturedFactory = factory.capturedSessionFactory;
        assertThat(
                capturedFactory.newInstance(SessionConfig.defaultConfig(), null, true),
                instanceOf(NetworkSession.class));
    }

    @ParameterizedTest
    @MethodSource("testUris")
    @SuppressWarnings("resource")
    void usesLeakLoggingSessionFactoryWhenConfigured(String uri) {
        var config = Config.builder().withLeakedSessionsLogging().build();
        var factory = new SessionFactoryCapturingDriverFactory();

        createDriver(uri, factory, config);

        var capturedFactory = factory.capturedSessionFactory;
        assertThat(
                capturedFactory.newInstance(SessionConfig.defaultConfig(), null, true),
                instanceOf(LeakLoggingNetworkSession.class));
    }

    @ParameterizedTest
    @MethodSource("testUris")
    void shouldNotVerifyConnectivity(String uri) {
        var sessionFactory = mock(SessionFactory.class);
        when(sessionFactory.verifyConnectivity()).thenReturn(completedWithNull());
        when(sessionFactory.close()).thenReturn(completedWithNull());
        var driverFactory = new DriverFactoryWithSessions(sessionFactory);

        try (var driver = createDriver(uri, driverFactory)) {
            assertNotNull(driver);
            verify(sessionFactory, never()).verifyConnectivity();
        }
    }

    @Test
    void shouldNotCreateDriverMetrics() {
        // Given
        var config = Config.builder().withoutDriverMetrics().build();
        // When
        var provider = DriverFactory.getOrCreateObservationProvider(config, Clock.systemUTC());
        // Then
        assertThat(provider, is(equalTo(NoopObservationProvider.getInstance())));
    }

    @Test
    void shouldCreateDriverMetricsIfMonitoringEnabled() {
        // Given
        var config =
                Config.builder().withDriverMetrics().withLogging(Logging.none()).build();
        // When
        var provider = DriverFactory.getOrCreateObservationProvider(config, Clock.systemUTC());
        // Then
        assertThat(provider instanceof DriverMetricsObservationProvider, is(true));
    }

    @Test
    void shouldCreateMicrometerDriverMetricsIfMonitoringEnabled() {
        // Given
        var config = Config.builder()
                .withDriverMetrics()
                .withMetricsAdapter(MetricsAdapter.MICROMETER)
                .withLogging(Logging.none())
                .build();
        // When
        var provider = DriverFactory.getOrCreateObservationProvider(config, Clock.systemUTC());
        // Then
        assertThat(provider instanceof MicrometerMetricsObservationProvider, is(true));
    }

    private Driver createDriver(String uri, DriverFactory driverFactory) {
        return createDriver(uri, driverFactory, defaultConfig());
    }

    private Driver createDriver(String uri, DriverFactory driverFactory, Config config) {
        var auth = AuthTokens.none();
        return driverFactory.newInstance(URI.create(uri), new StaticAuthTokenManager(auth), null, config);
    }

    private static class SessionFactoryCapturingDriverFactory extends DriverFactory {
        SessionFactory capturedSessionFactory;

        @Override
        protected SessionFactory createSessionFactory(
                BoltSecurityPlanManager securityPlanManager,
                DriverBoltConnectionSource connectionProvider,
                RetryLogic retryLogic,
                Config config,
                AuthTokenManager authTokenManager,
                HomeDatabaseCache homeDatabaseCache) {
            var sessionFactory = super.createSessionFactory(
                    securityPlanManager, connectionProvider, retryLogic, config, authTokenManager, homeDatabaseCache);
            capturedSessionFactory = sessionFactory;
            return sessionFactory;
        }
    }

    private static class DriverFactoryWithSessions extends DriverFactory {
        final SessionFactory sessionFactory;

        DriverFactoryWithSessions(SessionFactory sessionFactory) {
            this.sessionFactory = sessionFactory;
        }

        @Override
        protected SessionFactory createSessionFactory(
                BoltSecurityPlanManager securityPlanManager,
                DriverBoltConnectionSource connectionProvider,
                RetryLogic retryLogic,
                Config config,
                AuthTokenManager authTokenManager,
                HomeDatabaseCache homeDatabaseCache) {
            return sessionFactory;
        }
    }
}
