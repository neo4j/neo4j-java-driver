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
import io.netty.channel.local.LocalAddress;
import io.netty.util.concurrent.EventExecutorGroup;
import java.net.URI;
import java.time.Clock;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.neo4j.driver.AuthTokenManager;
import org.neo4j.driver.ClientCertificateManager;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Logging;
import org.neo4j.driver.MetricsAdapter;
import org.neo4j.driver.internal.bolt.api.BoltConnectionProvider;
import org.neo4j.driver.internal.bolt.api.BoltServerAddress;
import org.neo4j.driver.internal.bolt.api.DefaultDomainNameResolver;
import org.neo4j.driver.internal.bolt.api.DomainNameResolver;
import org.neo4j.driver.internal.bolt.api.LoggingProvider;
import org.neo4j.driver.internal.bolt.api.RoutingContext;
import org.neo4j.driver.internal.bolt.basicimpl.NettyBoltConnectionProvider;
import org.neo4j.driver.internal.bolt.basicimpl.async.connection.BootstrapFactory;
import org.neo4j.driver.internal.bolt.pooledimpl.PooledBoltConnectionProvider;
import org.neo4j.driver.internal.bolt.routedimpl.RoutedBoltConnectionProvider;
import org.neo4j.driver.internal.bolt.routedimpl.cluster.Rediscovery;
import org.neo4j.driver.internal.metrics.DevNullMetricsProvider;
import org.neo4j.driver.internal.metrics.InternalMetricsProvider;
import org.neo4j.driver.internal.metrics.MetricsProvider;
import org.neo4j.driver.internal.metrics.MicrometerMetricsProvider;
import org.neo4j.driver.internal.retry.ExponentialBackoffRetryLogic;
import org.neo4j.driver.internal.retry.RetryLogic;
import org.neo4j.driver.internal.security.BoltSecurityPlanManager;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.security.SecurityPlans;
import org.neo4j.driver.internal.util.DriverInfoUtil;
import org.neo4j.driver.net.ServerAddress;

public class DriverFactory {
    public static final String NO_ROUTING_CONTEXT_ERROR_MESSAGE =
            "Routing parameters are not supported with scheme 'bolt'. Given URI: ";

    public final Driver newInstance(
            URI uri,
            AuthTokenManager authTokenManager,
            ClientCertificateManager clientCertificateManager,
            Config config) {
        return newInstance(uri, authTokenManager, clientCertificateManager, config, null, null, null);
    }

    public final Driver newInstance(
            URI uri,
            AuthTokenManager authTokenManager,
            ClientCertificateManager clientCertificateManager,
            Config config,
            SecurityPlan securityPlan,
            EventLoopGroup eventLoopGroup,
            Supplier<Rediscovery> rediscoverySupplier) {
        if (securityPlan == null) {
            var settings = new SecuritySettings(config.encrypted(), config.trustStrategy());
            securityPlan = SecurityPlans.createSecurityPlan(
                    settings, uri.getScheme(), clientCertificateManager, config.logging());
        }
        var securityPlanManager = BoltSecurityPlanManager.from(securityPlan);
        return newInstance(uri, authTokenManager, config, securityPlanManager, eventLoopGroup, rediscoverySupplier);
    }

    @SuppressWarnings("deprecation")
    public final Driver newInstance(
            URI uri,
            AuthTokenManager authTokenManager,
            Config config,
            BoltSecurityPlanManager securityPlanManager,
            EventLoopGroup eventLoopGroup,
            Supplier<Rediscovery> rediscoverySupplier) {
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

        var address = new InternalServerAddress(uri);
        var routingSettings = new RoutingSettings(config.routingTablePurgeDelayMillis(), new RoutingContext(uri));

        EventExecutorGroup eventExecutorGroup = bootstrap.config().group();
        var retryLogic = createRetryLogic(config.maxTransactionRetryTimeMillis(), eventExecutorGroup, config.logging());

        var metricsProvider = getOrCreateMetricsProvider(config, createClock());

        return createDriver(
                uri,
                securityPlanManager,
                address,
                bootstrap.group(),
                routingSettings,
                retryLogic,
                metricsProvider,
                config,
                authTokenManager,
                ownsEventLoopGroup,
                rediscoverySupplier);
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
            BoltSecurityPlanManager securityPlanManager,
            ServerAddress address,
            EventLoopGroup eventLoopGroup,
            RoutingSettings routingSettings,
            RetryLogic retryLogic,
            MetricsProvider metricsProvider,
            Config config,
            AuthTokenManager authTokenManager,
            boolean ownsEventLoopGroup,
            Supplier<Rediscovery> rediscoverySupplier) {
        BoltConnectionProvider boltConnectionProvider = null;
        try {
            boltConnectionProvider =
                    createBoltConnectionProvider(uri, config, eventLoopGroup, routingSettings, rediscoverySupplier);
            boltConnectionProvider.init(
                    new BoltServerAddress(address.host(), address.port()),
                    new RoutingContext(uri),
                    DriverInfoUtil.boltAgent(),
                    config.userAgent(),
                    config.connectionTimeoutMillis(),
                    metricsProvider.metricsListener());
            var sessionFactory = createSessionFactory(
                    securityPlanManager, boltConnectionProvider, retryLogic, config, authTokenManager);
            Supplier<CompletionStage<Void>> shutdownSupplier = ownsEventLoopGroup
                    ? () -> {
                        var closeFuture = new CompletableFuture<Void>();
                        eventLoopGroup
                                .shutdownGracefully(200, 15_000, TimeUnit.MILLISECONDS)
                                .addListener(future -> closeFuture.complete(null));
                        return closeFuture;
                    }
                    : () -> CompletableFuture.completedStage(null);
            var driver = createDriver(securityPlanManager, sessionFactory, metricsProvider, shutdownSupplier, config);
            var log = config.logging().getLog(getClass());
            if (uri.getScheme().startsWith("bolt")) {
                log.info("Direct driver instance %s created for server address %s", driver.hashCode(), address);
            } else {
                log.info("Routing driver instance %s created for server address %s", driver.hashCode(), address);
            }
            return driver;
        } catch (Throwable driverError) {
            if (boltConnectionProvider != null) {
                boltConnectionProvider.close().toCompletableFuture().join();
            }
            throw driverError;
        }
    }

    private Function<BoltServerAddress, Set<BoltServerAddress>> createBoltServerAddressResolver(Config config) {
        var serverAddressResolver = config.resolver() != null ? config.resolver() : IDENTITY_RESOLVER;
        return (boltAddress) ->
                serverAddressResolver.resolve(ServerAddress.of(boltAddress.host(), boltAddress.port())).stream()
                        .map(serverAddress -> new BoltServerAddress(serverAddress.host(), serverAddress.port()))
                        .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private BoltConnectionProvider createBoltConnectionProvider(
            URI uri,
            Config config,
            EventLoopGroup eventLoopGroup,
            RoutingSettings routingSettings,
            Supplier<Rediscovery> rediscoverySupplier) {
        BoltConnectionProvider boltConnectionProvider;
        var clock = createClock();
        var loggingProvider = new BoltLoggingProvider(config.logging());
        Supplier<BoltConnectionProvider> pooledBoltConnectionProviderSupplier =
                () -> createPooledBoltConnectionProvider(config, eventLoopGroup, clock, loggingProvider);
        if (uri.getScheme().startsWith("bolt")) {
            assertNoRoutingContext(uri, routingSettings);
            boltConnectionProvider = pooledBoltConnectionProviderSupplier.get();
        } else {
            boltConnectionProvider = createRoutedBoltConnectionProvider(
                    config,
                    pooledBoltConnectionProviderSupplier,
                    routingSettings,
                    rediscoverySupplier,
                    clock,
                    loggingProvider);
        }
        return boltConnectionProvider;
    }

    private BoltConnectionProvider createRoutedBoltConnectionProvider(
            Config config,
            Supplier<BoltConnectionProvider> pooledBoltConnectionProviderSupplier,
            RoutingSettings routingSettings,
            Supplier<Rediscovery> rediscoverySupplier,
            Clock clock,
            LoggingProvider loggingProvider) {
        var boltServerAddressResolver = createBoltServerAddressResolver(config);
        var rediscovery = rediscoverySupplier != null ? rediscoverySupplier.get() : null;
        return new RoutedBoltConnectionProvider(
                pooledBoltConnectionProviderSupplier,
                boltServerAddressResolver,
                getDomainNameResolver(),
                routingSettings.routingTablePurgeDelayMs(),
                rediscovery,
                clock,
                loggingProvider);
    }

    private BoltConnectionProvider createPooledBoltConnectionProvider(
            Config config, EventLoopGroup eventLoopGroup, Clock clock, LoggingProvider loggingProvider) {
        var nettyBoltConnectionProvider = createNettyBoltConnectionProvider(eventLoopGroup, clock, loggingProvider);
        return new PooledBoltConnectionProvider(
                nettyBoltConnectionProvider,
                config.maxConnectionPoolSize(),
                config.connectionAcquisitionTimeoutMillis(),
                config.maxConnectionLifetimeMillis(),
                config.idleTimeBeforeConnectionTest(),
                clock,
                loggingProvider);
    }

    private BoltConnectionProvider createNettyBoltConnectionProvider(
            EventLoopGroup eventLoopGroup, Clock clock, LoggingProvider loggingProvider) {
        return new NettyBoltConnectionProvider(
                eventLoopGroup, clock, getDomainNameResolver(), localAddress(), loggingProvider);
    }

    @SuppressWarnings("SameReturnValue")
    protected LocalAddress localAddress() {
        return null;
    }

    /**
     * Creates new {@link Driver}.
     * <p>
     * <b>This method is protected only for testing</b>
     */
    protected InternalDriver createDriver(
            BoltSecurityPlanManager securityPlanManager,
            SessionFactory sessionFactory,
            MetricsProvider metricsProvider,
            Supplier<CompletionStage<Void>> shutdownSupplier,
            Config config) {
        return new InternalDriver(
                securityPlanManager,
                sessionFactory,
                metricsProvider,
                config.isTelemetryDisabled(),
                config.notificationConfig(),
                shutdownSupplier,
                config.logging());
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
            BoltSecurityPlanManager securityPlanManager,
            BoltConnectionProvider connectionProvider,
            RetryLogic retryLogic,
            Config config,
            AuthTokenManager authTokenManager) {
        return new SessionFactoryImpl(securityPlanManager, connectionProvider, retryLogic, config, authTokenManager);
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
