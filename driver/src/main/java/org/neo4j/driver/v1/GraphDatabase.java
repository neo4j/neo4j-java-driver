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

import org.neo4j.driver.internal.InternalDriver;
import org.neo4j.driver.internal.pool.PoolSettings;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.v1.exceptions.ClientException;

import static org.neo4j.driver.internal.util.AddressUtil.isLocalHost;
import static org.neo4j.driver.v1.Config.EncryptionLevel.REQUIRED;
import static org.neo4j.driver.v1.Config.EncryptionLevel.REQUIRED_NON_LOCAL;

/**
 * Creates {@link Driver drivers}, optionally letting you {@link #driver(URI, Config)} to configure them.
 * @see Driver
 * @since 1.0
 */
public class GraphDatabase
{
    /**
     * Return a driver for a Neo4j instance with the default configuration settings
     *
     * @param url the URL to a Neo4j instance
     * @return a new driver to the database instance specified by the URL
     */
    public static Driver driver( String url )
    {
        return driver( url, Config.defaultConfig() );
    }

    /**
     * Return a driver for a Neo4j instance with the default configuration settings
     *
     * @param url the URL to a Neo4j instance
     * @return a new driver to the database instance specified by the URL
     */
    public static Driver driver( URI url )
    {
        return driver( url, Config.defaultConfig() );
    }

    /**
     * Return a driver for a Neo4j instance with custom configuration.
     *
     * @param url the URL to a Neo4j instance
     * @param config user defined configuration
     * @return a new driver to the database instance specified by the URL
     */
    public static Driver driver( URI url, Config config )
    {
        return driver( url, AuthTokens.none(), config );
    }

    /**
     * Return a driver for a Neo4j instance with custom configuration.
     *
     * @param url the URL to a Neo4j instance
     * @param config user defined configuration
     * @return a new driver to the database instance specified by the URL
     */
    public static Driver driver( String url, Config config )
    {
        return driver( URI.create( url ), config );
    }

    /**
     * Return a driver for a Neo4j instance with the default configuration settings
     *
     * @param url the URL to a Neo4j instance
     * @param authToken authentication to use, see {@link AuthTokens}
     * @return a new driver to the database instance specified by the URL
     */
    public static Driver driver( String url, AuthToken authToken )
    {
        return driver( url, authToken, Config.defaultConfig() );
    }

    /**
     * Return a driver for a Neo4j instance with the default configuration settings
     *
     * @param url the URL to a Neo4j instance
     * @param authToken authentication to use, see {@link AuthTokens}
     * @return a new driver to the database instance specified by the URL
     */
    public static Driver driver( URI url, AuthToken authToken )
    {
        return driver( url, authToken, Config.defaultConfig() );
    }

    /**
     * Return a driver for a Neo4j instance with custom configuration.
     *
     * @param url the URL to a Neo4j instance
     * @param authToken authentication to use, see {@link AuthTokens}
     * @param config user defined configuration
     * @return a new driver to the database instance specified by the URL
     */
    public static Driver driver( String url, AuthToken authToken, Config config )
    {
        return driver( URI.create( url ), authToken, config );
    }

    /**
     * Return a driver for a Neo4j instance with custom configuration.
     *
     * @param url the URL to a Neo4j instance
     * @param authToken authentication to use, see {@link AuthTokens}
     * @param config user defined configuration
     * @return a new driver to the database instance specified by the URL
     */
    public static Driver driver( URI url, AuthToken authToken, Config config )
    {
        // Fill in defaults
        if (authToken == null)
        {
            authToken = AuthTokens.none();
        }
        if (config == null)
        {
            config = Config.defaultConfig();
        }

        // Construct security plan
        SecurityPlan securityPlan;
        try
        {
            securityPlan = createSecurityPlan( url, authToken, config );
        }
        catch ( GeneralSecurityException | IOException ex )
        {
            throw new ClientException( "Unable to establish SSL parameters", ex );
        }

        // Establish pool settings
        PoolSettings poolSettings = new PoolSettings(
                config.maxIdleConnectionPoolSize(),
                config.idleTimeBeforeConnectionTest() );

        // Finally, construct the driver proper
        return new InternalDriver( url, securityPlan, poolSettings, config.logging() );
    }

    /*
     * Establish a complete SecurityPlan based on the details provided for
     * driver construction.
     */
    private static SecurityPlan createSecurityPlan( URI url, AuthToken authToken, Config config )
            throws GeneralSecurityException, IOException
    {
        Config.EncryptionLevel encryptionLevel = config.encryptionLevel();
        boolean requiresEncryption = encryptionLevel.equals( REQUIRED ) ||
                (encryptionLevel.equals( REQUIRED_NON_LOCAL ) && !isLocalHost( url.getHost() ));

        if ( requiresEncryption )
        {
            switch ( config.trustStrategy().strategy() )
            {
            case TRUST_SIGNED_CERTIFICATES:
                return SecurityPlan.forSignedCertificates( authToken, config.trustStrategy().certFile() );
            case TRUST_ON_FIRST_USE:
                return SecurityPlan.forTrustOnFirstUse( authToken, config.trustStrategy().certFile(),
                        url.getHost(), url.getPort(), config.logging().getLog( "session" ) );
            default:
                throw new ClientException( "Unknown TLS authentication strategy: " + config.trustStrategy().strategy().name() );
            }
        }
        else
        {
            return new SecurityPlan( authToken, false );
        }
    }

}
