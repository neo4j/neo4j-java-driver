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

import java.net.URI;
import java.util.Map;

import org.neo4j.driver.internal.InternalDriver;

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
        AuthToken tokenToUse = authToken != null ? authToken: AuthTokens.none();
        Config configToUse = config != null ? config: Config.defaultConfig();

        return new InternalDriver( url, tokenToUse, configToUse );
    }

    /**
     * Return a driver for a Neo4j instance with custom configuration.
     *
     * @param url the URL to a Neo4j instance
     * @param authToken authentication to use, see {@link AuthTokens}
     * @param config user defined configuration
     * @return a new driver to the database instance specified by the URL
     */
    public static Driver driver( URI url, AuthToken authToken, Map<String, Object> config )
    {
        AuthToken tokenToUse = authToken != null ? authToken: AuthTokens.none();
        Config configToUse =  Config.defaultConfig();

        return new InternalDriver( url, tokenToUse, configToUse );
    }
}
