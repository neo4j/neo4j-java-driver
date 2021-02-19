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
package org.neo4j.driver.v1;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

import org.neo4j.driver.internal.DriverFactory;
import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.retry.RetrySettings;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;

import static org.neo4j.driver.internal.DriverFactory.BOLT_ROUTING_URI_SCHEME;
import static org.neo4j.driver.internal.DriverFactory.NEO4J_URI_SCHEME;

/**
 * Creates {@link Driver drivers}, optionally letting you {@link #driver(URI, Config)} to configure them.
 *
 * @see Driver
 * @since 1.0
 */
public class GraphDatabase
{
    private static final String LOGGER_NAME = GraphDatabase.class.getSimpleName();
    private static final List<String> VALID_ROUTING_URIS = Arrays.asList( BOLT_ROUTING_URI_SCHEME, NEO4J_URI_SCHEME );

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
        config = getOrDefault( config );
        RoutingSettings routingSettings = config.routingSettings();
        RetrySettings retrySettings = config.retrySettings();

        return new DriverFactory().newInstance( uri, authToken, routingSettings, retrySettings, config );
    }

    /**
     * Try to create a routing driver from the <b>first</b> available address.
     * This is wrapper for the {@link #driver} method that finds the <b>first</b>
     * server to respond positively.
     *
     * @param routingUris an {@link Iterable} of server {@link URI}s for Neo4j instances. All given URIs should
     * have 'bolt+routing' or 'neo4j' scheme.
     * @param authToken authentication to use, see {@link AuthTokens}
     * @param config user defined configuration
     * @return a new driver instance
     */
    public static Driver routingDriver( Iterable<URI> routingUris, AuthToken authToken, Config config )
    {
        assertRoutingUris( routingUris );
        Logger log = createLogger( config );

        for ( URI uri : routingUris )
        {
            try
            {
                return driver( uri, authToken, config );
            }
            catch ( ServiceUnavailableException e )
            {
                log.warn( "Unable to create routing driver for URI: " + uri, e );
            }
        }

        throw new ServiceUnavailableException( "Failed to discover an available server" );
    }

    private static void assertRoutingUris( Iterable<URI> uris )
    {
        for ( URI uri : uris )
        {
            if ( !VALID_ROUTING_URIS.contains( uri.getScheme() ) )
            {
                throw new IllegalArgumentException(
                        String.format(
                                "Illegal URI scheme, expected URI scheme '%s' to be among '%s'", uri.getScheme(), VALID_ROUTING_URIS.toString() ) );
            }
        }
    }

    private static Logger createLogger( Config config )
    {
        Logging logging = getOrDefault( config ).logging();
        return logging.getLog( LOGGER_NAME );
    }

    private static Config getOrDefault( Config config )
    {
        return config != null ? config : Config.defaultConfig();
    }
}
