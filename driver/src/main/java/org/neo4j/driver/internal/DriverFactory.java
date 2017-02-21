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

import java.io.IOException;
import java.net.URI;
import java.security.GeneralSecurityException;

import org.neo4j.driver.internal.cluster.LoadBalancer;
import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.net.SocketConnector;
import org.neo4j.driver.internal.net.pooling.PoolSettings;
import org.neo4j.driver.internal.net.pooling.SocketConnectionPool;
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
import static org.neo4j.driver.v1.Config.EncryptionLevel.REQUIRED;

public class DriverFactory
{
    public final Driver newInstance( URI uri, AuthToken authToken, RoutingSettings routingSettings, Config config )
    {
        BoltServerAddress address = BoltServerAddress.from( uri );
        SecurityPlan securityPlan = createSecurityPlan( address, config );
        ConnectionPool connectionPool = createConnectionPool( authToken, securityPlan, config );
        SessionFactory sessionFactory = createSessionFactory( config );

        try
        {
            return createDriver( address, uri.getScheme(), connectionPool, config, routingSettings, securityPlan,
                    sessionFactory
            );
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

    private Driver createDriver( BoltServerAddress address, String scheme, ConnectionPool connectionPool,
            Config config, RoutingSettings routingSettings, SecurityPlan securityPlan,
            SessionFactory sessionFactory )
    {
        switch ( scheme.toLowerCase() )
        {
        case "bolt":
            return createDirectDriver( address, connectionPool, config, securityPlan, sessionFactory );
        case "bolt+routing":
            return createRoutingDriver( address, connectionPool, config, routingSettings, securityPlan,
                    sessionFactory );
        default:
            throw new ClientException( format( "Unsupported URI scheme: %s", scheme ) );
        }
    }

    /**
     * Creates a new driver for "bolt" scheme.
     * <p>
     * <b>This method is protected only for testing</b>
     */
    protected Driver createDirectDriver( BoltServerAddress address, ConnectionPool connectionPool,
            Config config, SecurityPlan securityPlan, SessionFactory sessionFactory )
    {
        ConnectionProvider connectionProvider = new DirectConnectionProvider( address, connectionPool );
        return new InternalDriver( securityPlan, sessionFactory, connectionProvider, config.logging() );
    }

    /**
     * Creates new a new driver for "bolt+routing" scheme.
     * <p>
     * <b>This method is protected only for testing</b>
     */
    protected Driver createRoutingDriver( BoltServerAddress address, ConnectionPool connectionPool,
            Config config, RoutingSettings routingSettings, SecurityPlan securityPlan, SessionFactory sessionFactory )
    {
        if ( !securityPlan.isRoutingCompatible() )
        {
            throw new IllegalArgumentException( "The chosen security plan is not compatible with a routing driver" );
        }
        ConnectionProvider connectionProvider = new LoadBalancer( routingSettings, connectionPool, createClock(),
                config.logging(), address );
        return new InternalDriver( securityPlan, sessionFactory, connectionProvider, config.logging() );
    }

    /**
     * Creates new {@link ConnectionPool}.
     * <p>
     * <b>This method is protected only for testing</b>
     */
    protected ConnectionPool createConnectionPool( AuthToken authToken, SecurityPlan securityPlan, Config config )
    {
        authToken = authToken == null ? AuthTokens.none() : authToken;

        ConnectionSettings connectionSettings = new ConnectionSettings( authToken, config.connectionTimeoutMillis() );
        PoolSettings poolSettings = new PoolSettings( config.maxIdleConnectionPoolSize(),
                config.idleTimeBeforeConnectionTest() );
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
    protected Connector createConnector( ConnectionSettings connectionSettings, SecurityPlan securityPlan,
            Logging logging )
    {
        return new SocketConnector( connectionSettings, securityPlan, logging );
    }

    private static SessionFactory createSessionFactory( Config config )
    {
        if ( config.logLeakedSessions() )
        {
            return new LeakLoggingNetworkSessionFactory( config.logging() );
        }
        return new NetworkSessionFactory();
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
    private static SecurityPlan createSecurityPlanImpl( BoltServerAddress address, Config config )
            throws GeneralSecurityException, IOException
    {
        Config.EncryptionLevel encryptionLevel = config.encryptionLevel();
        boolean requiresEncryption = encryptionLevel.equals( REQUIRED );

        if ( requiresEncryption )
        {
            Logger logger = config.logging().getLog( "session" );
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
}
