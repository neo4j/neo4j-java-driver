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
package org.neo4j.driver.internal;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.Config.defaultConfig;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.internal.util.Futures.failedFuture;
import static org.neo4j.driver.internal.util.Matchers.clusterDriver;
import static org.neo4j.driver.internal.util.Matchers.directDriver;

import io.netty.bootstrap.Bootstrap;
import io.netty.util.concurrent.EventExecutorGroup;
import java.net.URI;
import java.time.Clock;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokenManager;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Logging;
import org.neo4j.driver.MetricsAdapter;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.internal.async.LeakLoggingNetworkSession;
import org.neo4j.driver.internal.async.NetworkSession;
import org.neo4j.driver.internal.async.connection.BootstrapFactory;
import org.neo4j.driver.internal.cluster.HomeDatabaseCache;
import org.neo4j.driver.internal.cluster.Rediscovery;
import org.neo4j.driver.internal.cluster.RediscoveryImpl;
import org.neo4j.driver.internal.cluster.RoutingContext;
import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.cluster.loadbalancing.LoadBalancer;
import org.neo4j.driver.internal.metrics.DevNullMetricsProvider;
import org.neo4j.driver.internal.metrics.InternalMetricsProvider;
import org.neo4j.driver.internal.metrics.MetricsProvider;
import org.neo4j.driver.internal.metrics.MicrometerMetricsProvider;
import org.neo4j.driver.internal.retry.RetryLogic;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.security.StaticAuthTokenManager;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.spi.ConnectionProvider;

class DriverFactoryTest {
    private static Stream<String> testUris() {
        return Stream.of("bolt://localhost:7687", "neo4j://localhost:7687");
    }

    @ParameterizedTest
    @MethodSource("testUris")
    void connectionPoolClosedWhenDriverCreationFails(String uri) {
        ConnectionPool connectionPool = connectionPoolMock();
        DriverFactory factory = new ThrowingDriverFactory(connectionPool);

        assertThrows(UnsupportedOperationException.class, () -> createDriver(uri, factory));
        verify(connectionPool).close();
    }

    @ParameterizedTest
    @MethodSource("testUris")
    void connectionPoolCloseExceptionIsSuppressedWhenDriverCreationFails(String uri) {
        ConnectionPool connectionPool = connectionPoolMock();
        RuntimeException poolCloseError = new RuntimeException("Pool close error");
        when(connectionPool.close()).thenReturn(failedFuture(poolCloseError));

        DriverFactory factory = new ThrowingDriverFactory(connectionPool);

        UnsupportedOperationException e =
                assertThrows(UnsupportedOperationException.class, () -> createDriver(uri, factory));
        assertArrayEquals(new Throwable[] {poolCloseError}, e.getSuppressed());
        verify(connectionPool).close();
    }

    @ParameterizedTest
    @MethodSource("testUris")
    void usesStandardSessionFactoryWhenNothingConfigured(String uri) {
        Config config = defaultConfig();
        SessionFactoryCapturingDriverFactory factory = new SessionFactoryCapturingDriverFactory();

        createDriver(uri, factory, config);

        SessionFactory capturedFactory = factory.capturedSessionFactory;
        assertThat(capturedFactory.newInstance(SessionConfig.defaultConfig(), null), instanceOf(NetworkSession.class));
    }

    @ParameterizedTest
    @MethodSource("testUris")
    void usesLeakLoggingSessionFactoryWhenConfigured(String uri) {
        Config config = Config.builder().withLeakedSessionsLogging().build();
        SessionFactoryCapturingDriverFactory factory = new SessionFactoryCapturingDriverFactory();

        createDriver(uri, factory, config);

        SessionFactory capturedFactory = factory.capturedSessionFactory;
        assertThat(
                capturedFactory.newInstance(SessionConfig.defaultConfig(), null),
                instanceOf(LeakLoggingNetworkSession.class));
    }

    @ParameterizedTest
    @MethodSource("testUris")
    void shouldNotVerifyConnectivity(String uri) {
        SessionFactory sessionFactory = mock(SessionFactory.class);
        when(sessionFactory.verifyConnectivity()).thenReturn(completedWithNull());
        when(sessionFactory.close()).thenReturn(completedWithNull());
        DriverFactoryWithSessions driverFactory = new DriverFactoryWithSessions(sessionFactory);

        try (Driver driver = createDriver(uri, driverFactory)) {
            assertNotNull(driver);
            verify(sessionFactory, never()).verifyConnectivity();
        }
    }

    @Test
    void shouldNotCreateDriverMetrics() {
        // Given
        Config config = Config.builder().withoutDriverMetrics().build();
        // When
        MetricsProvider provider = DriverFactory.getOrCreateMetricsProvider(config, Clock.systemUTC());
        // Then
        assertThat(provider, is(equalTo(DevNullMetricsProvider.INSTANCE)));
    }

    @Test
    void shouldCreateDriverMetricsIfMonitoringEnabled() {
        // Given
        Config config =
                Config.builder().withDriverMetrics().withLogging(Logging.none()).build();
        // When
        MetricsProvider provider = DriverFactory.getOrCreateMetricsProvider(config, Clock.systemUTC());
        // Then
        assertThat(provider instanceof InternalMetricsProvider, is(true));
    }

    @Test
    void shouldCreateMicrometerDriverMetricsIfMonitoringEnabled() {
        // Given
        Config config = Config.builder()
                .withDriverMetrics()
                .withMetricsAdapter(MetricsAdapter.MICROMETER)
                .withLogging(Logging.none())
                .build();
        // When
        MetricsProvider provider = DriverFactory.getOrCreateMetricsProvider(config, Clock.systemUTC());
        // Then
        assertThat(provider instanceof MicrometerMetricsProvider, is(true));
    }

    @ParameterizedTest
    @MethodSource("testUris")
    void shouldCreateAppropriateDriverType(String uri) {
        DriverFactory driverFactory = new DriverFactory();
        Driver driver = createDriver(uri, driverFactory);

        if (uri.startsWith("bolt://")) {
            assertThat(driver, is(directDriver()));
        } else if (uri.startsWith("neo4j://")) {
            assertThat(driver, is(clusterDriver()));
        } else {
            fail("Unexpected scheme provided in argument");
        }
    }

    @Test
    void shouldUseBuiltInRediscoveryByDefault() {
        // GIVEN
        var driverFactory = new DriverFactory();

        // WHEN
        var driver = driverFactory.newInstance(
                URI.create("neo4j://localhost:7687"),
                new StaticAuthTokenManager(AuthTokens.none()),
                Config.defaultConfig(),
                null,
                null,
                null);

        // THEN
        var sessionFactory = ((InternalDriver) driver).getSessionFactory();
        var connectionProvider = ((SessionFactoryImpl) sessionFactory).getConnectionProvider();
        var rediscovery = ((LoadBalancer) connectionProvider).getRediscovery();
        assertTrue(rediscovery instanceof RediscoveryImpl);
    }

    @Test
    void shouldUseSuppliedRediscovery() {
        // GIVEN
        var driverFactory = new DriverFactory();
        @SuppressWarnings("unchecked")
        Supplier<Rediscovery> rediscoverySupplier = mock(Supplier.class);
        var rediscovery = mock(Rediscovery.class);
        given(rediscoverySupplier.get()).willReturn(rediscovery);

        // WHEN
        var driver = driverFactory.newInstance(
                URI.create("neo4j://localhost:7687"),
                new StaticAuthTokenManager(AuthTokens.none()),
                Config.defaultConfig(),
                null,
                null,
                rediscoverySupplier);

        // THEN
        var sessionFactory = ((InternalDriver) driver).getSessionFactory();
        var connectionProvider = ((SessionFactoryImpl) sessionFactory).getConnectionProvider();
        var actualRediscovery = ((LoadBalancer) connectionProvider).getRediscovery();
        then(rediscoverySupplier).should().get();
        assertEquals(rediscovery, actualRediscovery);
    }

    private Driver createDriver(String uri, DriverFactory driverFactory) {
        return createDriver(uri, driverFactory, defaultConfig());
    }

    private Driver createDriver(String uri, DriverFactory driverFactory, Config config) {
        AuthToken auth = AuthTokens.none();
        return driverFactory.newInstance(URI.create(uri), new StaticAuthTokenManager(auth), config);
    }

    private static ConnectionPool connectionPoolMock() {
        ConnectionPool pool = mock(ConnectionPool.class);
        Connection connection = mock(Connection.class);
        when(pool.acquire(any(BoltServerAddress.class), any(AuthToken.class))).thenReturn(completedFuture(connection));
        when(pool.close()).thenReturn(completedWithNull());
        return pool;
    }

    private static class ThrowingDriverFactory extends DriverFactory {
        final ConnectionPool connectionPool;

        ThrowingDriverFactory(ConnectionPool connectionPool) {
            this.connectionPool = connectionPool;
        }

        @Override
        protected InternalDriver createDriver(
                SecurityPlan securityPlan,
                SessionFactory sessionFactory,
                MetricsProvider metricsProvider,
                Runnable homeDatabaseCachePurgeRunnable,
                Config config) {
            throw new UnsupportedOperationException("Can't create direct driver");
        }

        @Override
        protected InternalDriver createRoutingDriver(
                SecurityPlan securityPlan,
                BoltServerAddress address,
                ConnectionPool connectionPool,
                EventExecutorGroup eventExecutorGroup,
                RoutingSettings routingSettings,
                RetryLogic retryLogic,
                MetricsProvider metricsProvider,
                Supplier<Rediscovery> rediscoverySupplier,
                Config config) {
            throw new UnsupportedOperationException("Can't create routing driver");
        }

        @Override
        protected ConnectionPool createConnectionPool(
                AuthTokenManager authTokenManager,
                SecurityPlan securityPlan,
                Bootstrap bootstrap,
                MetricsProvider metricsProvider,
                Config config,
                boolean ownsEventLoopGroup,
                RoutingContext routingContext) {
            return connectionPool;
        }
    }

    private static class SessionFactoryCapturingDriverFactory extends DriverFactory {
        SessionFactory capturedSessionFactory;

        @Override
        protected InternalDriver createDriver(
                SecurityPlan securityPlan,
                SessionFactory sessionFactory,
                MetricsProvider metricsProvider,
                Runnable homeDatabaseCachePurgeRunnable,
                Config config) {
            InternalDriver driver = mock(InternalDriver.class);
            when(driver.verifyConnectivityAsync()).thenReturn(completedWithNull());
            return driver;
        }

        @Override
        protected LoadBalancer createLoadBalancer(
                BoltServerAddress address,
                ConnectionPool connectionPool,
                EventExecutorGroup eventExecutorGroup,
                Config config,
                RoutingSettings routingSettings,
                Supplier<Rediscovery> rediscoverySupplier,
                HomeDatabaseCache homeDatabaseCache) {
            return null;
        }

        @Override
        protected SessionFactory createSessionFactory(
                ConnectionProvider connectionProvider, RetryLogic retryLogic, Config config) {
            SessionFactory sessionFactory = super.createSessionFactory(connectionProvider, retryLogic, config);
            capturedSessionFactory = sessionFactory;
            return sessionFactory;
        }

        @Override
        protected ConnectionPool createConnectionPool(
                AuthTokenManager authTokenManager,
                SecurityPlan securityPlan,
                Bootstrap bootstrap,
                MetricsProvider metricsProvider,
                Config config,
                boolean ownsEventLoopGroup,
                RoutingContext routingContext) {
            return connectionPoolMock();
        }
    }

    private static class DriverFactoryWithSessions extends DriverFactory {
        final SessionFactory sessionFactory;

        DriverFactoryWithSessions(SessionFactory sessionFactory) {
            this.sessionFactory = sessionFactory;
        }

        @Override
        protected Bootstrap createBootstrap(int ignored) {
            return BootstrapFactory.newBootstrap(1);
        }

        @Override
        protected ConnectionPool createConnectionPool(
                AuthTokenManager authTokenManager,
                SecurityPlan securityPlan,
                Bootstrap bootstrap,
                MetricsProvider metricsProvider,
                Config config,
                boolean ownsEventLoopGroup,
                RoutingContext routingContext) {
            return connectionPoolMock();
        }

        @Override
        protected SessionFactory createSessionFactory(
                ConnectionProvider connectionProvider, RetryLogic retryLogic, Config config) {
            return sessionFactory;
        }
    }
}
