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

import java.net.SocketAddress;
import java.net.URI;
import java.time.Clock;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.neo4j.bolt.connection.BoltAgent;
import org.neo4j.bolt.connection.BoltConnectionProvider;
import org.neo4j.bolt.connection.BoltConnectionProviderFactory;
import org.neo4j.bolt.connection.BoltConnectionSource;
import org.neo4j.bolt.connection.BoltServerAddress;
import org.neo4j.bolt.connection.DefaultDomainNameResolver;
import org.neo4j.bolt.connection.DomainNameResolver;
import org.neo4j.bolt.connection.LoggingProvider;
import org.neo4j.bolt.connection.MetricsListener;
import org.neo4j.bolt.connection.NotificationConfig;
import org.neo4j.bolt.connection.RoutedBoltConnectionParameters;
import org.neo4j.bolt.connection.pooled.PooledBoltConnectionSource;
import org.neo4j.bolt.connection.pooled.SecurityPlanSupplier;
import org.neo4j.bolt.connection.routed.BoltConnectionSourceFactory;
import org.neo4j.bolt.connection.routed.Rediscovery;
import org.neo4j.bolt.connection.routed.RoutedBoltConnectionSource;
import org.neo4j.driver.AuthTokenManager;
import org.neo4j.driver.ClientCertificateManager;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Logging;
import org.neo4j.driver.MetricsAdapter;
import org.neo4j.driver.exceptions.AuthTokenManagerExecutionException;
import org.neo4j.driver.internal.adaptedbolt.AdaptingDriverBoltConnectionSource;
import org.neo4j.driver.internal.adaptedbolt.BoltAuthTokenManager;
import org.neo4j.driver.internal.adaptedbolt.BoltConnectionProviderFactoryLoader;
import org.neo4j.driver.internal.adaptedbolt.DriverBoltConnectionSource;
import org.neo4j.driver.internal.adaptedbolt.ErrorMapper;
import org.neo4j.driver.internal.adaptedbolt.SingleRoutedBoltConnectionSource;
import org.neo4j.driver.internal.boltlistener.BoltConnectionListener;
import org.neo4j.driver.internal.homedb.HomeDatabaseCache;
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
import org.neo4j.driver.internal.value.BoltValueFactory;
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

    @SuppressWarnings("deprecation")
    public final Driver newInstance(
            URI uri,
            AuthTokenManager authTokenManager,
            ClientCertificateManager clientCertificateManager,
            Config config,
            SecurityPlan securityPlan,
            ScheduledExecutorService eventLoopGroup,
            Supplier<Rediscovery> rediscoverySupplier) {
        var boltConnectionProviderFactoryLoader = new BoltConnectionProviderFactoryLoader(config.logging(), uri);
        var boltConnectionProviderFactory = boltConnectionProviderFactoryLoader
                .providerFactory()
                .orElseThrow(() -> new IllegalArgumentException(
                        "Unsupported scheme: " + boltConnectionProviderFactoryLoader.scheme()));
        if (securityPlan == null) {
            var settings = new SecuritySettings(config.encrypted(), config.trustStrategy());
            securityPlan = SecurityPlans.createSecurityPlan(
                    settings, uri.getScheme(), clientCertificateManager, config.logging());
        }
        var securityPlanManager = BoltSecurityPlanManager.from(securityPlan);
        return newInstance(
                uri,
                authTokenManager,
                config,
                securityPlanManager,
                eventLoopGroup,
                rediscoverySupplier,
                boltConnectionProviderFactory);
    }

    public final Driver newInstance(
            URI uri,
            AuthTokenManager authTokenManager,
            Config config,
            BoltSecurityPlanManager securityPlanManager,
            ScheduledExecutorService eventLoopGroup,
            Supplier<Rediscovery> rediscoverySupplier,
            BoltConnectionProviderFactory boltConnectionProviderFactory) {
        requireNonNull(authTokenManager, "authTokenProvider must not be null");

        var retryExecutor = eventLoopGroup != null ? eventLoopGroup : Executors.newSingleThreadScheduledExecutor();
        @SuppressWarnings("deprecation")
        var retryLogic = createRetryLogic(config.maxTransactionRetryTimeMillis(), retryExecutor, config.logging());

        var metricsProvider = getOrCreateMetricsProvider(config, createClock());

        return createDriver(
                uri,
                securityPlanManager,
                eventLoopGroup,
                retryLogic,
                metricsProvider,
                config,
                authTokenManager,
                rediscoverySupplier,
                boltConnectionProviderFactory);
    }

    @SuppressWarnings("deprecation")
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

    @SuppressWarnings("deprecation")
    private InternalDriver createDriver(
            URI uri,
            BoltSecurityPlanManager securityPlanManager,
            ScheduledExecutorService eventLoopGroup,
            RetryLogic retryLogic,
            MetricsProvider metricsProvider,
            Config config,
            AuthTokenManager authTokenManager,
            Supplier<Rediscovery> rediscoverySupplier,
            BoltConnectionProviderFactory boltConnectionProviderFactory) {
        DriverBoltConnectionSource boltConnectionProvider = null;
        try {
            var homeDatabaseCache = HomeDatabaseCache.newInstance(Scheme.isRoutingScheme(uri.getScheme()));
            var valueFactory = BoltValueFactory.getInstance();
            boltConnectionProvider = createDriverBoltConnectionProvider(
                    uri,
                    config,
                    eventLoopGroup,
                    rediscoverySupplier,
                    homeDatabaseCache,
                    DriverInfoUtil.boltAgent(),
                    config.userAgent(),
                    config.connectionTimeoutMillis(),
                    metricsProvider.metricsListener(),
                    authTokenManager,
                    securityPlanManager::plan,
                    NotificationConfigMapper.map(config.notificationConfig()),
                    boltConnectionProviderFactory);
            var sessionFactory = createSessionFactory(
                    securityPlanManager,
                    boltConnectionProvider,
                    retryLogic,
                    config,
                    authTokenManager,
                    homeDatabaseCache);
            var driver = createDriver(securityPlanManager, sessionFactory, metricsProvider, config);
            var log = config.logging().getLog(getClass());
            log.info("Driver instance %s created for server uri '%s'", driver.hashCode(), uri);
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

    private DriverBoltConnectionSource createDriverBoltConnectionProvider(
            URI uri,
            Config config,
            ScheduledExecutorService eventLoopGroup,
            Supplier<Rediscovery> rediscoverySupplier,
            BoltConnectionListener boltConnectionListener,
            BoltAgent boltAgent,
            String userAgent,
            int connectTimeoutMillis,
            MetricsListener metricsListener,
            AuthTokenManager authTokenManager,
            SecurityPlanSupplier securityPlanSupplier,
            NotificationConfig notificationConfig,
            BoltConnectionProviderFactory boltConnectionProviderFactory) {
        var clock = createClock();
        var boltValueFactory = BoltValueFactory.getInstance();
        var errorMapper = ErrorMapper.getInstance();
        var boltAuthTokenManager = new BoltAuthTokenManager(authTokenManager, boltValueFactory, errorMapper);
        var boltConnectionProvider = createBoltConnectionSource(
                uri,
                config,
                eventLoopGroup,
                rediscoverySupplier,
                boltConnectionListener,
                boltAgent,
                userAgent,
                connectTimeoutMillis,
                metricsListener,
                clock,
                boltAuthTokenManager,
                securityPlanSupplier,
                notificationConfig,
                boltConnectionProviderFactory);
        return new AdaptingDriverBoltConnectionSource(
                boltConnectionProvider, errorMapper, boltValueFactory, Scheme.isRoutingScheme(uri.getScheme()));
    }

    @SuppressWarnings("deprecation")
    protected BoltConnectionSource<RoutedBoltConnectionParameters> createBoltConnectionSource(
            URI uri,
            Config config,
            ScheduledExecutorService eventLoopGroup,
            Supplier<Rediscovery> rediscoverySupplier,
            BoltConnectionListener boltConnectionListener,
            BoltAgent boltAgent,
            String userAgent,
            int connectTimeoutMillis,
            MetricsListener metricsListener,
            Clock clock,
            org.neo4j.bolt.connection.pooled.AuthTokenManager authTokenManager,
            SecurityPlanSupplier securityPlanSupplier,
            NotificationConfig notificationConfig,
            BoltConnectionProviderFactory boltConnectionProviderFactory) {
        BoltConnectionSource<RoutedBoltConnectionParameters> boltConnectionSource;
        var loggingProvider = new BoltLoggingProvider(config.logging());

        switch (uri.getScheme()) {
            case Scheme.BOLT_URI_SCHEME, Scheme.BOLT_LOW_TRUST_URI_SCHEME, Scheme.BOLT_HIGH_TRUST_URI_SCHEME -> {
                if (uri.getQuery() != null && !uri.getQuery().isEmpty()) {
                    throw new IllegalArgumentException(NO_ROUTING_CONTEXT_ERROR_MESSAGE + "'" + uri + "'");
                }
            }
        }
        var routingContextAddress = "%s:%d".formatted(uri.getHost(), uri.getPort() != -1 ? uri.getPort() : 7687);

        var pooledSourceSupplierFactory = createPooledBoltConnectionSource(
                config,
                eventLoopGroup,
                clock,
                loggingProvider,
                boltConnectionListener,
                routingContextAddress,
                boltAgent,
                userAgent,
                connectTimeoutMillis,
                metricsListener,
                authTokenManager,
                securityPlanSupplier,
                notificationConfig,
                boltConnectionProviderFactory);
        if (Scheme.isRoutingScheme(uri.getScheme())) {
            boltConnectionSource = createRoutedBoltConnectionProvider(
                    config,
                    pooledSourceSupplierFactory,
                    config.routingTablePurgeDelayMillis(),
                    rediscoverySupplier,
                    clock,
                    loggingProvider,
                    uri,
                    boltAgent,
                    userAgent,
                    connectTimeoutMillis,
                    metricsListener);
        } else {
            boltConnectionSource = new SingleRoutedBoltConnectionSource(pooledSourceSupplierFactory.create(uri, null));
        }
        return boltConnectionSource;
    }

    private RoutedBoltConnectionSource createRoutedBoltConnectionProvider(
            Config config,
            BoltConnectionSourceFactory boltConnectionSourceFactory,
            long routingTablePurgeDelayMs,
            Supplier<Rediscovery> rediscoverySupplier,
            Clock clock,
            LoggingProvider loggingProvider,
            URI uri,
            BoltAgent boltAgent,
            String userAgent,
            int connectTimeoutMillis,
            MetricsListener metricsListener) {
        var boltServerAddressResolver = createBoltServerAddressResolver(config);
        var rediscovery = rediscoverySupplier != null ? rediscoverySupplier.get() : null;
        return new RoutedBoltConnectionSource(
                boltConnectionSourceFactory,
                boltServerAddressResolver,
                getDomainNameResolver(),
                routingTablePurgeDelayMs,
                rediscovery,
                clock,
                loggingProvider,
                uri,
                List.of(AuthTokenManagerExecutionException.class));
    }

    private BoltConnectionSourceFactory createPooledBoltConnectionSource(
            Config config,
            ScheduledExecutorService eventLoopGroup,
            Clock clock,
            LoggingProvider loggingProvider,
            BoltConnectionListener boltConnectionListener,
            String routingContextAddress,
            BoltAgent boltAgent,
            String userAgent,
            int connectTimeoutMillis,
            MetricsListener metricsListener,
            org.neo4j.bolt.connection.pooled.AuthTokenManager authTokenManager,
            SecurityPlanSupplier securityPlanSupplier,
            NotificationConfig notificationConfig,
            BoltConnectionProviderFactory boltConnectionProviderFactory) {
        return (uri, expectedVerificationHostname) -> {
            var boltConnectionProvider = createBoltConnectionProvider(
                    uri,
                    eventLoopGroup,
                    clock,
                    loggingProvider,
                    config.eventLoopThreads(),
                    boltConnectionProviderFactory);
            var listeningBoltConnectionProvider = BoltConnectionListener.listeningBoltConnectionProvider(
                    boltConnectionProvider, boltConnectionListener);
            return new PooledBoltConnectionSource(
                    loggingProvider,
                    clock,
                    uri,
                    listeningBoltConnectionProvider,
                    authTokenManager,
                    createSecurityPlanSupplierWithHostname(securityPlanSupplier, expectedVerificationHostname),
                    config.maxConnectionPoolSize(),
                    config.connectionAcquisitionTimeoutMillis(),
                    config.maxConnectionLifetimeMillis(),
                    config.idleTimeBeforeConnectionTest(),
                    metricsListener,
                    routingContextAddress,
                    boltAgent,
                    userAgent,
                    connectTimeoutMillis,
                    notificationConfig);
        };
    }

    private BoltConnectionProvider createBoltConnectionProvider(
            URI uri,
            ScheduledExecutorService eventLoopGroup,
            Clock clock,
            LoggingProvider loggingProvider,
            int eventLoopThreads,
            BoltConnectionProviderFactory boltConnectionProviderFactory) {
        var additionalConfig = new HashMap<String, Object>();
        additionalConfig.put("clock", clock);
        if (eventLoopGroup != null) {
            additionalConfig.put("eventLoopGroup", eventLoopGroup);
        } else if (eventLoopThreads > 0) {
            additionalConfig.put("eventLoopThreads", eventLoopThreads);
        }
        var localAddress = localAddress();
        if (localAddress != null) {
            additionalConfig.put("localAddress", localAddress);
        }
        return boltConnectionProviderFactory.create(
                loggingProvider, BoltValueFactory.getInstance(), null, additionalConfig);
    }

    @SuppressWarnings("SameReturnValue")
    protected SocketAddress localAddress() {
        return null;
    }

    /**
     * Creates new {@link Driver}.
     * <p>
     * <b>This method is protected only for testing</b>
     */
    @SuppressWarnings("deprecation")
    protected InternalDriver createDriver(
            BoltSecurityPlanManager securityPlanManager,
            SessionFactory sessionFactory,
            MetricsProvider metricsProvider,
            Config config) {
        return new InternalDriver(
                securityPlanManager, sessionFactory, metricsProvider, config.isTelemetryDisabled(), config.logging());
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
            DriverBoltConnectionSource connectionProvider,
            RetryLogic retryLogic,
            Config config,
            AuthTokenManager authTokenManager,
            HomeDatabaseCache homeDatabaseCache) {
        return new SessionFactoryImpl(
                securityPlanManager, connectionProvider, retryLogic, config, authTokenManager, homeDatabaseCache);
    }

    /**
     * Creates new {@link RetryLogic}.
     * <p>
     * <b>This method is protected only for testing</b>
     */
    protected RetryLogic createRetryLogic(
            long maxTransactionRetryTime,
            ScheduledExecutorService executor,
            @SuppressWarnings("deprecation") Logging logging) {
        return new ExponentialBackoffRetryLogic(maxTransactionRetryTime, executor, createClock(), logging);
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

    private static SecurityPlanSupplier createSecurityPlanSupplierWithHostname(
            SecurityPlanSupplier securityPlanSupplier, String expectedVerificationHostname) {
        return expectedVerificationHostname != null
                ? () -> securityPlanSupplier.getPlan().thenApply(securityPlan -> {
                    if (securityPlan != null && securityPlan.expectedHostname() == null) {
                        return org.neo4j.bolt.connection.SecurityPlans.encrypted(
                                securityPlan.sslContext(), securityPlan.verifyHostname(), expectedVerificationHostname);
                    } else {
                        return securityPlan;
                    }
                })
                : securityPlanSupplier;
    }
}
