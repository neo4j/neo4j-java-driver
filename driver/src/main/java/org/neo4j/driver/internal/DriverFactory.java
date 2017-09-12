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

import java.io.IOException;
import java.net.URI;
import java.security.GeneralSecurityException;

import org.neo4j.driver.internal.async.AsyncConnectorImpl;
import org.neo4j.driver.internal.async.BootstrapFactory;
import org.neo4j.driver.internal.async.pool.ActiveChannelTracker;
import org.neo4j.driver.internal.async.pool.AsyncConnectionPool;
import org.neo4j.driver.internal.async.pool.AsyncConnectionPoolImpl;
import org.neo4j.driver.internal.cluster.RoutingContext;
import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.cluster.loadbalancing.LeastConnectedLoadBalancingStrategy;
import org.neo4j.driver.internal.cluster.loadbalancing.LoadBalancer;
import org.neo4j.driver.internal.cluster.loadbalancing.LoadBalancingStrategy;
import org.neo4j.driver.internal.cluster.loadbalancing.RoundRobinLoadBalancingStrategy;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.net.SocketConnector;
import org.neo4j.driver.internal.net.pooling.PoolSettings;
import org.neo4j.driver.internal.net.pooling.SocketConnectionPool;
import org.neo4j.driver.internal.retry.ExponentialBackoffRetryLogic;
import org.neo4j.driver.internal.retry.RetryLogic;
import org.neo4j.driver.internal.retry.RetrySettings;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.spi.ConnectionProvider;
import org.neo4j.driver.internal.spi.Connector;
import org.neo4j.driver.internal.util.Clock;
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
        ConnectionPool connectionPool = createConnectionPool( authToken, securityPlan, config );
        RetryLogic retryLogic = createRetryLogic( retrySettings, config.logging() );

        AsyncConnectionPool asyncConnectionPool = createAsyncConnectionPool( authToken, securityPlan, config );

        try
        {
            return createDriver( uri, address, connectionPool, config, newRoutingSettings, securityPlan, retryLogic,
                    asyncConnectionPool );
        }
        catch ( Throwable driverError )
        {
            // we need to close the connection pool if driver creation threw exception
            try
            {
                connectionPool.close();
            }
            catch ( Throwable closeError )
            {
                driverError.addSuppressed( closeError );
            }
            throw driverError;
        }
    }

    private AsyncConnectionPool createAsyncConnectionPool( AuthToken authToken, SecurityPlan securityPlan,
            Config config )
    {
        Clock clock = createClock();
        ConnectionSettings connectionSettings = new ConnectionSettings( authToken, config.connectionTimeoutMillis() );
        ActiveChannelTracker activeChannelTracker = new ActiveChannelTracker( config.logging() );
        AsyncConnectorImpl connector = new AsyncConnectorImpl( connectionSettings, securityPlan,
                activeChannelTracker, config.logging(), clock );
        Bootstrap bootstrap = BootstrapFactory.newBootstrap();
        PoolSettings poolSettings = new PoolSettings( config.maxIdleConnectionPoolSize(),
                config.idleTimeBeforeConnectionTest(), config.maxConnectionLifetimeMillis(),
                config.maxConnectionPoolSize(),
                config.connectionAcquisitionTimeoutMillis() );
        return new AsyncConnectionPoolImpl( connector, bootstrap, activeChannelTracker, poolSettings, config.logging(),
                clock );
    }

    private Driver createDriver( URI uri, BoltServerAddress address, ConnectionPool connectionPool,
            Config config, RoutingSettings routingSettings, SecurityPlan securityPlan,
            RetryLogic retryLogic, AsyncConnectionPool asyncConnectionPool )
    {
        String scheme = uri.getScheme().toLowerCase();
        switch ( scheme )
        {
        case BOLT_URI_SCHEME:
            assertNoRoutingContext( uri, routingSettings );
            return createDirectDriver( address, connectionPool, config, securityPlan, retryLogic, asyncConnectionPool );
        case BOLT_ROUTING_URI_SCHEME:
            return createRoutingDriver( address, connectionPool, config, routingSettings, securityPlan, retryLogic );
        default:
            throw new ClientException( format( "Unsupported URI scheme: %s", scheme ) );
        }
    }

    /**
     * Creates a new driver for "bolt" scheme.
     * <p>
     * <b>This method is protected only for testing</b>
     */
    protected Driver createDirectDriver( BoltServerAddress address, ConnectionPool connectionPool, Config config,
            SecurityPlan securityPlan, RetryLogic retryLogic, AsyncConnectionPool asyncConnectionPool )
    {
        ConnectionProvider connectionProvider =
                new DirectConnectionProvider( address, connectionPool, asyncConnectionPool );
        SessionFactory sessionFactory = createSessionFactory( connectionProvider, retryLogic, config );
        return createDriver( config, securityPlan, sessionFactory );
    }

    /**
     * Creates new a new driver for "bolt+routing" scheme.
     * <p>
     * <b>This method is protected only for testing</b>
     */
    protected Driver createRoutingDriver( BoltServerAddress address, ConnectionPool connectionPool,
            Config config, RoutingSettings routingSettings, SecurityPlan securityPlan, RetryLogic retryLogic )
    {
        if ( !securityPlan.isRoutingCompatible() )
        {
            throw new IllegalArgumentException( "The chosen security plan is not compatible with a routing driver" );
        }
        ConnectionProvider connectionProvider = createLoadBalancer( address, connectionPool, config, routingSettings );
        SessionFactory sessionFactory = createSessionFactory( connectionProvider, retryLogic, config );
        return createDriver( config, securityPlan, sessionFactory );
    }

    /**
     * Creates new {@link Driver}.
     * <p>
     * <b>This method is protected only for testing</b>
     */
    protected InternalDriver createDriver( Config config, SecurityPlan securityPlan, SessionFactory sessionFactory )
    {
        return new InternalDriver( securityPlan, sessionFactory, config.logging() );
    }

    /**
     * Creates new {@link LoadBalancer} for the routing driver.
     * <p>
     * <b>This method is protected only for testing</b>
     */
    protected LoadBalancer createLoadBalancer( BoltServerAddress address, ConnectionPool connectionPool, Config config,
            RoutingSettings routingSettings )
    {
        return new LoadBalancer( address, routingSettings, connectionPool, createClock(), config.logging(),
                createLoadBalancingStrategy( config, connectionPool ) );
    }

    private static LoadBalancingStrategy createLoadBalancingStrategy( Config config, ConnectionPool connectionPool )
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
     * Creates new {@link ConnectionPool}.
     * <p>
     * <b>This method is protected only for testing</b>
     */
    protected ConnectionPool createConnectionPool( AuthToken authToken, SecurityPlan securityPlan, Config config )
    {
        ConnectionSettings connectionSettings = new ConnectionSettings( authToken, config.connectionTimeoutMillis() );
        PoolSettings poolSettings = new PoolSettings( config.maxIdleConnectionPoolSize(),
                config.idleTimeBeforeConnectionTest(), config.maxConnectionLifetimeMillis(),
                config.maxConnectionPoolSize(), config.connectionAcquisitionTimeoutMillis() );
        Connector connector = createConnector( connectionSettings, securityPlan, config.logging() );

        return new SocketConnectionPool( poolSettings, connector, createClock(), config.logging() );
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
     * Creates new {@link Connector}.
     * <p>
     * <b>This method is protected only for testing</b>
     */
    protected Connector createConnector( final ConnectionSettings connectionSettings, SecurityPlan securityPlan,
            Logging logging )
    {
        return new SocketConnector( connectionSettings, securityPlan, logging );
    }

    /**
     * Creates new {@link SessionFactory}.
     * <p>
     * <b>This method is protected only for testing</b>
     */
    protected SessionFactory createSessionFactory( ConnectionProvider connectionProvider,
            RetryLogic retryLogic, Config config )
    {
        return new SessionFactoryImpl( connectionProvider, retryLogic, config );
    }

    /**
     * Creates new {@link RetryLogic >}.
     * <p>
     * <b>This method is protected only for testing</b>
     */
    protected RetryLogic createRetryLogic( RetrySettings settings, Logging logging )
    {
        return new ExponentialBackoffRetryLogic( settings, createClock(), logging );
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
}
