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

import static java.util.Objects.requireNonNull;
import static org.neo4j.driver.internal.Scheme.isRoutingScheme;
import static org.neo4j.driver.internal.cluster.IdentityResolver.IDENTITY_RESOLVER;
import static org.neo4j.driver.internal.util.ErrorUtil.addSuppressed;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.internal.logging.InternalLoggerFactory;
import java.net.URI;
import java.time.Clock;
import java.util.function.Supplier;
import org.neo4j.driver.AuthTokenManager;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Logging;
import org.neo4j.driver.MetricsAdapter;
import org.neo4j.driver.internal.async.connection.BootstrapFactory;
import org.neo4j.driver.internal.async.connection.ChannelConnector;
import org.neo4j.driver.internal.async.connection.ChannelConnectorImpl;
import org.neo4j.driver.internal.async.pool.ConnectionPoolImpl;
import org.neo4j.driver.internal.async.pool.PoolSettings;
import org.neo4j.driver.internal.cluster.Rediscovery;
import org.neo4j.driver.internal.cluster.RediscoveryImpl;
import org.neo4j.driver.internal.cluster.RoutingContext;
import org.neo4j.driver.internal.cluster.RoutingProcedureClusterCompositionProvider;
import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.cluster.loadbalancing.LeastConnectedLoadBalancingStrategy;
import org.neo4j.driver.internal.cluster.loadbalancing.LoadBalancer;
import org.neo4j.driver.internal.logging.NettyLogging;
import org.neo4j.driver.internal.metrics.DevNullMetricsProvider;
import org.neo4j.driver.internal.metrics.InternalMetricsProvider;
import org.neo4j.driver.internal.metrics.MetricsProvider;
import org.neo4j.driver.internal.metrics.MicrometerMetricsProvider;
import org.neo4j.driver.internal.retry.ExponentialBackoffRetryLogic;
import org.neo4j.driver.internal.retry.RetryLogic;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.security.SecurityPlans;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.spi.ConnectionProvider;
import org.neo4j.driver.internal.util.DriverInfoUtil;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.net.ServerAddressResolver;

public class DriverFactory {
    public static final String NO_ROUTING_CONTEXT_ERROR_MESSAGE =
            "Routing parameters are not supported with scheme 'bolt'. Given URI: ";

    public final Driver newInstance(URI uri, AuthTokenManager authTokenManager, Config config) {
        return newInstance(uri, authTokenManager, config, null, null, null);
    }

    public final Driver newInstance(
            URI uri,
            AuthTokenManager authTokenManager,
            Config config,
            SecurityPlan securityPlan,
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

        if (securityPlan == null) {
            var settings = new SecuritySettings(config.encrypted(), config.trustStrategy());
            securityPlan = SecurityPlans.createSecurityPlan(settings, uri.getScheme());
        }

        var address = new BoltServerAddress(uri);
        var routingSettings = new RoutingSettings(config.routingTablePurgeDelayMillis(), new RoutingContext(uri));

        InternalLoggerFactory.setDefaultFactory(new NettyLogging(config.logging()));
        EventExecutorGroup eventExecutorGroup = bootstrap.config().group();
        var retryLogic = createRetryLogic(config.maxTransactionRetryTimeMillis(), eventExecutorGroup, config.logging());

        var metricsProvider = getOrCreateMetricsProvider(config, createClock());
        var connectionPool = createConnectionPool(
                authTokenManager,
                securityPlan,
                bootstrap,
                metricsProvider,
                config,
                ownsEventLoopGroup,
                routingSettings.routingContext());

        return createDriver(
                uri,
                securityPlan,
                address,
                connectionPool,
                eventExecutorGroup,
                routingSettings,
                retryLogic,
                metricsProvider,
                rediscoverySupplier,
                config);
    }

    protected ConnectionPool createConnectionPool(
            AuthTokenManager authTokenManager,
            SecurityPlan securityPlan,
            Bootstrap bootstrap,
            MetricsProvider metricsProvider,
            Config config,
            boolean ownsEventLoopGroup,
            RoutingContext routingContext) {
        var clock = createClock();
        var settings = new ConnectionSettings(authTokenManager, config.userAgent(), config.connectionTimeoutMillis());
        var boltAgent = DriverInfoUtil.boltAgent();
        var connector = createConnector(settings, securityPlan, config, clock, routingContext, boltAgent);
        var poolSettings = new PoolSettings(
                config.maxConnectionPoolSize(),
                config.connectionAcquisitionTimeoutMillis(),
                config.maxConnectionLifetimeMillis(),
                config.idleTimeBeforeConnectionTest());
        return new ConnectionPoolImpl(
                connector,
                bootstrap,
                poolSettings,
                metricsProvider.metricsListener(),
                config.logging(),
                clock,
                ownsEventLoopGroup);
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

    protected ChannelConnector createConnector(
            ConnectionSettings settings,
            SecurityPlan securityPlan,
            Config config,
            Clock clock,
            RoutingContext routingContext,
            BoltAgent boltAgent) {
        return new ChannelConnectorImpl(
                settings,
                securityPlan,
                config.logging(),
                clock,
                routingContext,
                getDomainNameResolver(),
                config.notificationConfig(),
                boltAgent);
    }

    private InternalDriver createDriver(
            URI uri,
            SecurityPlan securityPlan,
            BoltServerAddress address,
            ConnectionPool connectionPool,
            EventExecutorGroup eventExecutorGroup,
            RoutingSettings routingSettings,
            RetryLogic retryLogic,
            MetricsProvider metricsProvider,
            Supplier<Rediscovery> rediscoverySupplier,
            Config config) {
        try {
            var scheme = uri.getScheme().toLowerCase();

            if (isRoutingScheme(scheme)) {
                return createRoutingDriver(
                        securityPlan,
                        address,
                        connectionPool,
                        eventExecutorGroup,
                        routingSettings,
                        retryLogic,
                        metricsProvider,
                        rediscoverySupplier,
                        config);
            } else {
                assertNoRoutingContext(uri, routingSettings);
                return createDirectDriver(securityPlan, address, connectionPool, retryLogic, metricsProvider, config);
            }
        } catch (Throwable driverError) {
            // we need to close the connection pool if driver creation threw exception
            closeConnectionPoolAndSuppressError(connectionPool, driverError);
            throw driverError;
        }
    }

    /**
     * Creates a new driver for "bolt" scheme.
     * <p>
     * <b>This method is protected only for testing</b>
     */
    protected InternalDriver createDirectDriver(
            SecurityPlan securityPlan,
            BoltServerAddress address,
            ConnectionPool connectionPool,
            RetryLogic retryLogic,
            MetricsProvider metricsProvider,
            Config config) {
        ConnectionProvider connectionProvider = new DirectConnectionProvider(address, connectionPool);
        var sessionFactory = createSessionFactory(connectionProvider, retryLogic, config);
        var driver = createDriver(securityPlan, sessionFactory, metricsProvider, config);
        var log = config.logging().getLog(getClass());
        log.info("Direct driver instance %s created for server address %s", driver.hashCode(), address);
        return driver;
    }

    /**
     * Creates new a new driver for "neo4j" scheme.
     * <p>
     * <b>This method is protected only for testing</b>
     */
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
        ConnectionProvider connectionProvider = createLoadBalancer(
                address, connectionPool, eventExecutorGroup, config, routingSettings, rediscoverySupplier);
        var sessionFactory = createSessionFactory(connectionProvider, retryLogic, config);
        var driver = createDriver(securityPlan, sessionFactory, metricsProvider, config);
        var log = config.logging().getLog(getClass());
        log.info("Routing driver instance %s created for server address %s", driver.hashCode(), address);
        return driver;
    }

    /**
     * Creates new {@link Driver}.
     * <p>
     * <b>This method is protected only for testing</b>
     */
    protected InternalDriver createDriver(
            SecurityPlan securityPlan, SessionFactory sessionFactory, MetricsProvider metricsProvider, Config config) {
        return new InternalDriver(securityPlan, sessionFactory, metricsProvider, config.logging());
    }

    /**
     * Creates new {@link LoadBalancer} for the routing driver.
     * <p>
     * <b>This method is protected only for testing</b>
     */
    protected LoadBalancer createLoadBalancer(
            BoltServerAddress address,
            ConnectionPool connectionPool,
            EventExecutorGroup eventExecutorGroup,
            Config config,
            RoutingSettings routingSettings,
            Supplier<Rediscovery> rediscoverySupplier) {
        var loadBalancingStrategy = new LeastConnectedLoadBalancingStrategy(connectionPool, config.logging());
        var resolver = createResolver(config);
        var domainNameResolver = requireNonNull(getDomainNameResolver(), "domainNameResolver must not be null");
        var clock = createClock();
        var logging = config.logging();
        if (rediscoverySupplier == null) {
            rediscoverySupplier =
                    () -> createRediscovery(address, resolver, routingSettings, clock, logging, domainNameResolver);
        }
        var loadBalancer = new LoadBalancer(
                connectionPool,
                rediscoverySupplier.get(),
                routingSettings,
                loadBalancingStrategy,
                eventExecutorGroup,
                clock,
                logging);
        handleNewLoadBalancer(loadBalancer);
        return loadBalancer;
    }

    protected Rediscovery createRediscovery(
            BoltServerAddress initialRouter,
            ServerAddressResolver resolver,
            RoutingSettings settings,
            Clock clock,
            Logging logging,
            DomainNameResolver domainNameResolver) {
        var clusterCompositionProvider =
                new RoutingProcedureClusterCompositionProvider(clock, settings.routingContext(), logging);
        return new RediscoveryImpl(initialRouter, clusterCompositionProvider, resolver, logging, domainNameResolver);
    }

    /**
     * Handles new {@link LoadBalancer} instance.
     * <p>
     * <b>This method is protected for Testkit backend usage only.</b>
     *
     * @param loadBalancer the new load balancer instance.
     */
    protected void handleNewLoadBalancer(LoadBalancer loadBalancer) {}

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
            ConnectionProvider connectionProvider, RetryLogic retryLogic, Config config) {
        return new SessionFactoryImpl(connectionProvider, retryLogic, config);
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

    private static void closeConnectionPoolAndSuppressError(ConnectionPool connectionPool, Throwable mainError) {
        try {
            Futures.blockingGet(connectionPool.close());
        } catch (Throwable closeError) {
            addSuppressed(mainError, closeError);
        }
    }
}
