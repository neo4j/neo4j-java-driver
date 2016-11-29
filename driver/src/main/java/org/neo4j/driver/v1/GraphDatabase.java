/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.driver.v1;

import java.io.IOException;
import java.net.URI;
import java.security.GeneralSecurityException;

import org.neo4j.driver.internal.ConnectionSettings;
import org.neo4j.driver.internal.DirectDriver;
import org.neo4j.driver.internal.NetworkSession;
import org.neo4j.driver.internal.RoutingDriver;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.net.pooling.PoolSettings;
import org.neo4j.driver.internal.net.pooling.SocketConnectionPool;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.util.Function;

import static java.lang.String.format;
import static org.neo4j.driver.internal.security.SecurityPlan.insecure;
import static org.neo4j.driver.v1.Config.EncryptionLevel.REQUIRED;

/**
 * Creates {@link Driver drivers}, optionally letting you {@link #driver(URI, Config)} to configure them.
 * @see Driver
 * @since 1.0
 */
public class GraphDatabase
{

    private static final Function<Connection,Session>
            SESSION_PROVIDER = new Function<Connection,Session>()
    {
        @Override
        public Session apply( Connection connection )
        {
            return new NetworkSession( connection );
        }
    };

    /**
     * Return a driver for a Neo4j instance with the default configuration settings
     *
     * @param uri the URL to a Neo4j instance
     * @return a new driver to the database instance specified by the URL
     */
    public static Driver driver( String uri )
    {
        return driver( uri, Config.defaultConfig() );
    }

    /**
     * Return a driver for a Neo4j instance with the default configuration settings
     *
     * @param uri the URL to a Neo4j instance
     * @return a new driver to the database instance specified by the URL
     */
    public static Driver driver( URI uri )
    {
        return driver( uri, Config.defaultConfig() );
    }

    /**
     * Return a driver for a Neo4j instance with custom configuration.
     *
     * @param uri the URL to a Neo4j instance
     * @param config user defined configuration
     * @return a new driver to the database instance specified by the URL
     */
    public static Driver driver( URI uri, Config config )
    {
        return driver( uri, AuthTokens.none(), config );
    }

    /**
     * Return a driver for a Neo4j instance with custom configuration.
     *
     * @param uri the URL to a Neo4j instance
     * @param config user defined configuration
     * @return a new driver to the database instance specified by the URL
     */
    public static Driver driver( String uri, Config config )
    {
        return driver( URI.create( uri ), config );
    }

    /**
     * Return a driver for a Neo4j instance with the default configuration settings
     *
     * @param uri the URL to a Neo4j instance
     * @param authToken authentication to use, see {@link AuthTokens}
     * @return a new driver to the database instance specified by the URL
     */
    public static Driver driver( String uri, AuthToken authToken )
    {
        return driver( uri, authToken, Config.defaultConfig() );
    }

    /**
     * Return a driver for a Neo4j instance with the default configuration settings
     *
     * @param uri the URL to a Neo4j instance
     * @param authToken authentication to use, see {@link AuthTokens}
     * @return a new driver to the database instance specified by the URL
     */
    public static Driver driver( URI uri, AuthToken authToken )
    {
        return driver( uri, authToken, Config.defaultConfig() );
    }

    /**
     * Return a driver for a Neo4j instance with custom configuration.
     *
     * @param uri the URL to a Neo4j instance
     * @param authToken authentication to use, see {@link AuthTokens}
     * @param config user defined configuration
     * @return a new driver to the database instance specified by the URL
     */
    public static Driver driver( String uri, AuthToken authToken, Config config )
    {
        return driver( URI.create( uri ), authToken, config );
    }

    /**
     * Return a driver for a Neo4j instance with custom configuration.
     *
     * @param uri the URL to a Neo4j instance
     * @param authToken authentication to use, see {@link AuthTokens}
     * @param config user defined configuration
     * @return a new driver to the database instance specified by the URL
     */
    public static Driver driver( URI uri, AuthToken authToken, Config config )
    {
        // Break down the URI into its constituent parts
        String scheme = uri.getScheme();
        BoltServerAddress address = BoltServerAddress.from( uri );

        // Collate session parameters
        ConnectionSettings connectionSettings =
                new ConnectionSettings( authToken == null ? AuthTokens.none() : authToken );

        // Make sure we have some configuration to play with
        if ( config == null )
        {
            config = Config.defaultConfig();
        }

        // Construct security plan
        SecurityPlan securityPlan;
        try
        {
            securityPlan = createSecurityPlan( address, config );
        }
        catch ( GeneralSecurityException | IOException ex )
        {
            throw new ClientException( "Unable to establish SSL parameters", ex );
        }

        // Establish pool settings
        PoolSettings poolSettings = new PoolSettings( config.maxIdleConnectionPoolSize() );

        // And finally, construct the driver proper
        ConnectionPool connectionPool =
                new SocketConnectionPool( connectionSettings, securityPlan, poolSettings, config.logging() );
        switch ( scheme.toLowerCase() )
        {
        case "bolt":
            return new DirectDriver( address, connectionPool, securityPlan, config.logging() );
        case "bolt+routing":
            return new RoutingDriver(
                    config.routingSettings(),
                    address,
                    connectionPool,
                    securityPlan,
                    Clock.SYSTEM,
                    config.logging() );
        default:
            throw new ClientException( format( "Unsupported URI scheme: %s", scheme ) );
        }
    }

    /*
     * Establish a complete SecurityPlan based on the details provided for
     * driver construction.
     */
    private static SecurityPlan createSecurityPlan( BoltServerAddress address, Config config )
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
