/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import io.netty.bootstrap.Bootstrap;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.security.GeneralSecurityException;

import org.neo4j.driver.internal.async.BootstrapFactory;
import org.neo4j.driver.internal.async.ChannelConnector;
import org.neo4j.driver.internal.async.ChannelConnectorImpl;
import org.neo4j.driver.internal.async.pool.ConnectionPoolImpl;
import org.neo4j.driver.internal.async.pool.PoolSettings;
import org.neo4j.driver.internal.cluster.RoutingContext;
import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.cluster.loadbalancing.LeastConnectedLoadBalancingStrategy;
import org.neo4j.driver.internal.cluster.loadbalancing.LoadBalancer;
import org.neo4j.driver.internal.cluster.loadbalancing.LoadBalancingStrategy;
import org.neo4j.driver.internal.cluster.loadbalancing.RoundRobinLoadBalancingStrategy;
import org.neo4j.driver.internal.logging.NettyLogging;
import org.neo4j.driver.internal.retry.ExponentialBackoffRetryLogic;
import org.neo4j.driver.internal.retry.RetryLogic;
import org.neo4j.driver.internal.retry.RetrySettings;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.spi.ConnectionProvider;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.exceptions.ClientException;

import static java.lang.String.format;
import static org.neo4j.driver.internal.security.SecurityPlan.insecure;

public class DriverFactory
{
    public static final String BOLT_URI_SCHEME = "bolt";
    public static final String BOLT_ROUTING_URI_SCHEME = "bolt+routing";

    public final Driver newInstance( URI uri, AuthToken authToken, RoutingSettings routingSettings,
            RetrySettings retrySettings, Config config )
    {
        authToken = authToken == null ? AuthTokens.none() : authToken;

        BoltServerAddress address = new BoltServerAddress( uri );
        RoutingSettings newRoutingSettings = routingSettings.withRoutingContext( new RoutingContext( uri ) );
        SecurityPlan securityPlan = createSecurityPlan( address, config );

        InternalLoggerFactory.setDefaultFactory( new NettyLogging( config.logging() ) );
        Bootstrap bootstrap = createBootstrap();
        EventExecutorGroup eventExecutorGroup = bootstrap.config().group();
        RetryLogic retryLogic = createRetryLogic( retrySettings, eventExecutorGroup, config.logging() );

        ConnectionPool connectionPool = createConnectionPool( authToken, securityPlan, bootstrap, config );

        try
        {
            InternalDriver driver = createDriver( uri, address, connectionPool, config, newRoutingSettings,
                    eventExecutorGroup, securityPlan, retryLogic );

            // block to verify connectivity, close connection pool if thread gets interrupted
            Futures.blockingGet( driver.verifyConnectivity(),
                    () -> closeConnectionPoolOnThreadInterrupt( connectionPool, config.logging() ) );
            return driver;
        }
        catch ( Throwable driverError )
        {
            // we need to close the connection pool if driver creation threw exception
            try
            {
                Futures.blockingGet( connectionPool.close() );
            }
            catch ( Throwable closeError )
            {
                driverError.addSuppressed( closeError );
            }
            throw driverError;
        }
    }

    protected ConnectionPool createConnectionPool( AuthToken authToken, SecurityPlan securityPlan,
            Bootstrap bootstrap, Config config )
    {
        Clock clock = createClock();
        ConnectionSettings settings = new ConnectionSettings( authToken, config.connectionTimeoutMillis() );
        ChannelConnector connector = createConnector( settings, securityPlan, config, clock );
        PoolSettings poolSettings = new PoolSettings( config.maxConnectionPoolSize(),
                config.connectionAcquisitionTimeoutMillis(), config.maxConnectionLifetimeMillis(),
                config.idleTimeBeforeConnectionTest()
        );
        return new ConnectionPoolImpl( connector, bootstrap, poolSettings, config.logging(), clock );
    }

    protected ChannelConnector createConnector( ConnectionSettings settings, SecurityPlan securityPlan,
            Config config, Clock clock )
    {
        return new ChannelConnectorImpl( settings, securityPlan, config.logging(), clock );
    }

    private InternalDriver createDriver( URI uri, BoltServerAddress address,
            ConnectionPool connectionPool, Config config, RoutingSettings routingSettings,
            EventExecutorGroup eventExecutorGroup, SecurityPlan securityPlan, RetryLogic retryLogic )
    {
        String scheme = uri.getScheme().toLowerCase();
        switch ( scheme )
        {
        case BOLT_URI_SCHEME:
            assertNoRoutingContext( uri, routingSettings );
            return createDirectDriver( address, config, securityPlan, retryLogic, connectionPool );
        case BOLT_ROUTING_URI_SCHEME:
            return createRoutingDriver( address, connectionPool, config, routingSettings, securityPlan, retryLogic,
                    eventExecutorGroup );
        default:
            throw new ClientException( format( "Unsupported URI scheme: %s", scheme ) );
        }
    }

    /**
     * Creates a new driver for "bolt" scheme.
     * <p>
     * <b>This method is protected only for testing</b>
     */
    protected InternalDriver createDirectDriver( BoltServerAddress address, Config config,
            SecurityPlan securityPlan, RetryLogic retryLogic, ConnectionPool connectionPool )
    {
        ConnectionProvider connectionProvider = new DirectConnectionProvider( address, connectionPool );
        SessionFactory sessionFactory = createSessionFactory( connectionProvider, retryLogic, config );
        return createDriver( sessionFactory, securityPlan, config );
    }

    /**
     * Creates new a new driver for "bolt+routing" scheme.
     * <p>
     * <b>This method is protected only for testing</b>
     */
    protected InternalDriver createRoutingDriver( BoltServerAddress address, ConnectionPool connectionPool,
            Config config, RoutingSettings routingSettings, SecurityPlan securityPlan, RetryLogic retryLogic,
            EventExecutorGroup eventExecutorGroup )
    {
        if ( !securityPlan.isRoutingCompatible() )
        {
            throw new IllegalArgumentException( "The chosen security plan is not compatible with a routing driver" );
        }
        ConnectionProvider connectionProvider = createLoadBalancer( address, connectionPool, eventExecutorGroup,
                config, routingSettings );
        SessionFactory sessionFactory = createSessionFactory( connectionProvider, retryLogic, config );
        return createDriver( sessionFactory, securityPlan, config );
    }

    /**
     * Creates new {@link Driver}.
     * <p>
     * <b>This method is protected only for testing</b>
     */
    protected InternalDriver createDriver( SessionFactory sessionFactory, SecurityPlan securityPlan, Config config )
    {
        return new InternalDriver( securityPlan, sessionFactory, config.logging() );
    }

    /**
     * Creates new {@link LoadBalancer} for the routing driver.
     * <p>
     * <b>This method is protected only for testing</b>
     */
    protected LoadBalancer createLoadBalancer( BoltServerAddress address, ConnectionPool connectionPool,
            EventExecutorGroup eventExecutorGroup, Config config, RoutingSettings routingSettings )
    {
        LoadBalancingStrategy loadBalancingStrategy = createLoadBalancingStrategy( config, connectionPool );
        return new LoadBalancer( address, routingSettings, connectionPool, eventExecutorGroup, createClock(),
                config.logging(), loadBalancingStrategy );
    }

    private static LoadBalancingStrategy createLoadBalancingStrategy( Config config,
            ConnectionPool connectionPool )
    {
        switch ( config.loadBalancingStrategy() )
        {
        case ROUND_ROBIN:
            return new RoundRobinLoadBalancingStrategy( config.logging() );
        case LEAST_CONNECTED:
            return new LeastConnectedLoadBalancingStrategy( connectionPool, config.logging() );
        default:
            throw new IllegalArgumentException( "Unknown load balancing strategy: " + config.loadBalancingStrategy() );
        }
    }

    /**
     * Creates new {@link Clock}.
     * <p>
     * <b>This method is protected only for testing</b>
     */
    protected Clock createClock()
    {
        return Clock.SYSTEM;
    }

    /**
     * Creates new {@link SessionFactory}.
     * <p>
     * <b>This method is protected only for testing</b>
     */
    protected SessionFactory createSessionFactory( ConnectionProvider connectionProvider, RetryLogic retryLogic,
            Config config )
    {
        return new SessionFactoryImpl( connectionProvider, retryLogic, config );
    }

    /**
     * Creates new {@link RetryLogic}.
     * <p>
     * <b>This method is protected only for testing</b>
     */
    protected RetryLogic createRetryLogic( RetrySettings settings, EventExecutorGroup eventExecutorGroup,
            Logging logging )
    {
        return new ExponentialBackoffRetryLogic( settings, eventExecutorGroup, createClock(), logging );
    }

    /**
     * Creates new {@link Bootstrap}.
     * <p>
     * <b>This method is protected only for testing</b>
     */
    protected Bootstrap createBootstrap()
    {
        return BootstrapFactory.newBootstrap();
    }

    private static SecurityPlan createSecurityPlan( BoltServerAddress address, Config config )
    {
        try
        {
            return createSecurityPlanImpl( address, config );
        }
        catch ( GeneralSecurityException | IOException ex )
        {
            throw new ClientException( "Unable to establish SSL parameters", ex );
        }
    }

    /*
     * Establish a complete SecurityPlan based on the details provided for
     * driver construction.
     */
    @SuppressWarnings( "deprecation" )
    private static SecurityPlan createSecurityPlanImpl( BoltServerAddress address, Config config )
            throws GeneralSecurityException, IOException
    {
        if ( config.encrypted() )
        {
            Logger logger = config.logging().getLog( "SecurityPlan" );
            switch ( config.trustStrategy().strategy() )
            {

            // DEPRECATED CASES //
            case TRUST_ON_FIRST_USE:
                logger.warn(
                        "Option `TRUST_ON_FIRST_USE` has been deprecated and will be removed in a future " +
                        "version of the driver. Please switch to use `TRUST_ALL_CERTIFICATES` instead." );
                return SecurityPlan.forTrustOnFirstUse( config.trustStrategy().certFile(), address, logger );
            case TRUST_SIGNED_CERTIFICATES:
                logger.warn(
                        "Option `TRUST_SIGNED_CERTIFICATE` has been deprecated and will be removed in a future " +
                        "version of the driver. Please switch to use `TRUST_CUSTOM_CA_SIGNED_CERTIFICATES` instead." );
                // intentional fallthrough
                // END OF DEPRECATED CASES //

            case TRUST_CUSTOM_CA_SIGNED_CERTIFICATES:
                return SecurityPlan.forCustomCASignedCertificates( config.trustStrategy().certFile() );
            case TRUST_SYSTEM_CA_SIGNED_CERTIFICATES:
                return SecurityPlan.forSystemCASignedCertificates();
            case TRUST_ALL_CERTIFICATES:
                return SecurityPlan.forAllCertificates();
            default:
                throw new ClientException(
                        "Unknown TLS authentication strategy: " + config.trustStrategy().strategy().name() );
            }
        }
        else
        {
            return insecure();
        }
    }

    private static void assertNoRoutingContext( URI uri, RoutingSettings routingSettings )
    {
        RoutingContext routingContext = routingSettings.routingContext();
        if ( routingContext.isDefined() )
        {
            throw new IllegalArgumentException(
                    "Routing parameters are not supported with scheme 'bolt'. Given URI: '" + uri + "'" );
        }
    }

    private static void closeConnectionPoolOnThreadInterrupt( ConnectionPool pool, Logging logging )
    {
        Logger log = logging.getLog( Driver.class.getSimpleName() );
        log.warn( "Driver creation interrupted while verifying connectivity. Connection pool will be closed" );
        pool.close();
    }
}
