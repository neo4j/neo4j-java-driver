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

import static java.util.Objects.requireNonNull;
import static org.neo4j.driver.internal.IdentityResolver.IDENTITY_RESOLVER;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import java.net.URI;
import java.time.Clock;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.neo4j.driver.AuthTokenManager;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Logging;
import org.neo4j.driver.MetricsAdapter;
import org.neo4j.driver.internal.bolt.api.BoltConnectionProvider;
import org.neo4j.driver.internal.bolt.api.BoltServerAddress;
import org.neo4j.driver.internal.bolt.api.DefaultDomainNameResolver;
import org.neo4j.driver.internal.bolt.api.DomainNameResolver;
import org.neo4j.driver.internal.bolt.api.RoutingContext;
import org.neo4j.driver.internal.bolt.api.SecurityPlan;
import org.neo4j.driver.internal.bolt.basicimpl.NettyBoltConnectionProvider;
import org.neo4j.driver.internal.bolt.basicimpl.async.connection.BootstrapFactory;
import org.neo4j.driver.internal.bolt.pooledimpl.PooledBoltConnectionProvider;
import org.neo4j.driver.internal.bolt.routedimpl.RoutedBoltConnectionProvider;
import org.neo4j.driver.internal.metrics.DevNullMetricsProvider;
import org.neo4j.driver.internal.metrics.InternalMetricsProvider;
import org.neo4j.driver.internal.metrics.MetricsProvider;
import org.neo4j.driver.internal.metrics.MicrometerMetricsProvider;
import org.neo4j.driver.internal.retry.ExponentialBackoffRetryLogic;
import org.neo4j.driver.internal.retry.RetryLogic;
import org.neo4j.driver.internal.security.SecurityPlans;
import org.neo4j.driver.internal.util.DriverInfoUtil;
import org.neo4j.driver.net.ServerAddress;
import org.neo4j.driver.net.ServerAddressResolver;

public class DriverFactory {
    public static final String NO_ROUTING_CONTEXT_ERROR_MESSAGE =
            "Routing parameters are not supported with scheme 'bolt'. Given URI: ";

    public final Driver newInstance(URI uri, AuthTokenManager authTokenManager, Config config) {
        return newInstance(uri, authTokenManager, config, null, null);
    }

    @SuppressWarnings("deprecation")
    public final Driver newInstance(
            URI uri,
            AuthTokenManager authTokenManager,
            Config config,
            SecurityPlan securityPlan,
            EventLoopGroup eventLoopGroup) {
        requireNonNull(authTokenManager, "authTokenProvider must not be null");

        Bootstrap bootstrap;
        boolean ownsEventLoopGroup;
        if (eventLoopGroup == null) {
            bootstrap = createBootstrap(config.eventLoopThreads());
            ownsEventLoopGroup = true;
        } else {
            bootstrap = createBootstrap(eventLoopGroup);
            ownsEventLoopGroup = false;
        }

        if (securityPlan == null) {
            var settings = new SecuritySettings(config.encrypted(), config.trustStrategy());
            securityPlan = SecurityPlans.createSecurityPlan(settings, uri.getScheme());
        }

        var address = new InternalServerAddress(uri);
        var routingSettings = new RoutingSettings(config.routingTablePurgeDelayMillis(), new RoutingContext(uri));

        EventExecutorGroup eventExecutorGroup = bootstrap.config().group();
        var retryLogic = createRetryLogic(config.maxTransactionRetryTimeMillis(), eventExecutorGroup, config.logging());

        var metricsProvider = getOrCreateMetricsProvider(config, createClock());

        return createDriver(
                uri,
                securityPlan,
                address,
                eventExecutorGroup,
                bootstrap.group(),
                routingSettings,
                retryLogic,
                metricsProvider,
                config,
                authTokenManager);
    }

    protected static MetricsProvider getOrCreateMetricsProvider(Config config, Clock clock) {
        var metricsAdapter = config.metricsAdapter();
        // This can actually only happen when someone mocks the config
        if (metricsAdapter == null) {
            metricsAdapter = config.isMetricsEnabled() ? MetricsAdapter.DEFAULT : MetricsAdapter.DEV_NULL;
        }
        return switch (metricsAdapter) {
            case DEV_NULL -> DevNullMetricsProvider.INSTANCE;
            case DEFAULT -> new InternalMetricsProvider(clock, config.logging());
            case MICROMETER -> MicrometerMetricsProvider.forGlobalRegistry();
        };
    }

    private InternalDriver createDriver(
            URI uri,
            SecurityPlan securityPlan,
            ServerAddress address,
            EventExecutorGroup eventExecutorGroup,
            EventLoopGroup eventLoopGroup,
            RoutingSettings routingSettings,
            RetryLogic retryLogic,
            MetricsProvider metricsProvider,
            Config config,
            AuthTokenManager authTokenManager) {
        BoltConnectionProvider boltConnectionProvider = null;
        try {
            if (uri.getScheme().startsWith("bolt")) {
                var routingContext = new RoutingContext(uri);
                if (routingContext.isDefined()) {
                    throw new IllegalArgumentException(NO_ROUTING_CONTEXT_ERROR_MESSAGE + "'" + uri + "'");
                }
                boltConnectionProvider = new PooledBoltConnectionProvider(
                        new NettyBoltConnectionProvider(
                                eventLoopGroup,
                                createClock(),
                                getDomainNameResolver(),
                                new BoltLoggingProvider(config.logging())),
                        config.maxConnectionPoolSize(),
                        config.connectionAcquisitionTimeoutMillis(),
                        config.maxConnectionLifetimeMillis(),
                        config.idleTimeBeforeConnectionTest(),
                        createClock(),
                        new BoltLoggingProvider(config.logging()));
            } else {
                var serverAddressResolver = config.resolver() != null ? config.resolver() : IDENTITY_RESOLVER;
                Function<BoltServerAddress, Set<BoltServerAddress>> boltAddressResolver =
                        (boltAddress) -> serverAddressResolver.resolve(address).stream()
                                .map(serverAddress -> new BoltServerAddress(serverAddress.host(), serverAddress.port()))
                                .collect(Collectors.toCollection(LinkedHashSet::new));
                var routingContext = new RoutingContext(uri);
                routingContext.toMap();
                boltConnectionProvider = new RoutedBoltConnectionProvider(
                        () -> new PooledBoltConnectionProvider(
                                new NettyBoltConnectionProvider(
                                        eventLoopGroup,
                                        createClock(),
                                        getDomainNameResolver(),
                                        new BoltLoggingProvider(config.logging())),
                                config.maxConnectionPoolSize(),
                                config.connectionAcquisitionTimeoutMillis(),
                                config.maxConnectionLifetimeMillis(),
                                config.idleTimeBeforeConnectionTest(),
                                createClock(),
                                new BoltLoggingProvider(config.logging())),
                        boltAddressResolver,
                        getDomainNameResolver(),
                        createClock(),
                        new BoltLoggingProvider(config.logging()));
            }
            var boltAgent = DriverInfoUtil.boltAgent();
            boltConnectionProvider.init(
                    new BoltServerAddress(address.host(), address.port()),
                    securityPlan,
                    new RoutingContext(uri),
                    boltAgent,
                    config.userAgent(),
                    config.connectionTimeoutMillis(),
                    metricsProvider.metricsListener());
            // todo assertNoRoutingContext(uri, routingSettings);
            var sessionFactory = createSessionFactory(boltConnectionProvider, retryLogic, config, authTokenManager);
            var driver = createDriver(securityPlan, sessionFactory, metricsProvider, config);
            var log = config.logging().getLog(getClass());
            log.info("Routing driver instance %s created for server address %s", driver.hashCode(), address);
            return driver;
        } catch (Throwable driverError) {
            if (boltConnectionProvider != null) {
                boltConnectionProvider.close().toCompletableFuture().join();
            }
            throw driverError;
        }
    }

    /**
     * Creates new {@link Driver}.
     * <p>
     * <b>This method is protected only for testing</b>
     */
    protected InternalDriver createDriver(
            SecurityPlan securityPlan, SessionFactory sessionFactory, MetricsProvider metricsProvider, Config config) {
        return new InternalDriver(
                securityPlan,
                sessionFactory,
                metricsProvider,
                config.isTelemetryDisabled(),
                config.notificationConfig(),
                config.logging());
    }

    private static ServerAddressResolver createResolver(Config config) {
        var configuredResolver = config.resolver();
        return configuredResolver != null ? configuredResolver : IDENTITY_RESOLVER;
    }

    /**
     * Creates new {@link Clock}.
     */
    protected Clock createClock() {
        return Clock.systemUTC();
    }

    /**
     * Creates new {@link SessionFactory}.
     * <p>
     * <b>This method is protected only for testing</b>
     */
    protected SessionFactory createSessionFactory(
            BoltConnectionProvider connectionProvider,
            RetryLogic retryLogic,
            Config config,
            AuthTokenManager authTokenManager) {
        return new SessionFactoryImpl(connectionProvider, retryLogic, config, authTokenManager);
    }

    /**
     * Creates new {@link RetryLogic}.
     * <p>
     * <b>This method is protected only for testing</b>
     */
    protected RetryLogic createRetryLogic(
            long maxTransactionRetryTime, EventExecutorGroup eventExecutorGroup, Logging logging) {
        return new ExponentialBackoffRetryLogic(maxTransactionRetryTime, eventExecutorGroup, createClock(), logging);
    }

    /**
     * Creates new {@link Bootstrap}.
     * <p>
     * <b>This method is protected only for testing</b>
     */
    protected Bootstrap createBootstrap(int size) {
        return BootstrapFactory.newBootstrap(size);
    }

    /**
     * Creates new {@link Bootstrap}.
     * <p>
     * <b>This method is protected only for testing</b>
     */
    protected Bootstrap createBootstrap(EventLoopGroup eventLoopGroup) {
        return BootstrapFactory.newBootstrap(eventLoopGroup);
    }

    /**
     * Provides an instance of {@link DomainNameResolver} that is used for domain name resolution.
     * <p>
     * <b>This method is protected only for testing</b>
     *
     * @return the instance of {@link DomainNameResolver}.
     */
    protected DomainNameResolver getDomainNameResolver() {
        return DefaultDomainNameResolver.getInstance();
    }

    private static void assertNoRoutingContext(URI uri, RoutingSettings routingSettings) {
        var routingContext = routingSettings.routingContext();
        if (routingContext.isDefined()) {
            throw new IllegalArgumentException(NO_ROUTING_CONTEXT_ERROR_MESSAGE + "'" + uri + "'");
        }
    }
}
