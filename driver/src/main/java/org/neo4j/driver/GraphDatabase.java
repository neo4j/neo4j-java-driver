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
package org.neo4j.driver;

import static java.util.Objects.requireNonNull;

import java.net.URI;
import org.neo4j.driver.internal.DriverFactory;
import org.neo4j.driver.internal.security.StaticAuthTokenManager;
import org.neo4j.driver.internal.security.ValidatingAuthTokenManager;

/**
 * Creates {@link Driver drivers}, optionally letting you {@link #driver(URI, Config)} to configure them.
 * @see Driver
 * @since 1.0
 */
public final class GraphDatabase {
    private GraphDatabase() {}

    /**
     * Return a driver for a Neo4j instance with the default configuration settings
     *
     * @param uri the URL to a Neo4j instance
     * @return a new driver to the database instance specified by the URL
     */
    public static Driver driver(String uri) {
        return driver(uri, Config.defaultConfig());
    }

    /**
     * Return a driver for a Neo4j instance with the default configuration settings
     *
     * @param uri the URL to a Neo4j instance
     * @return a new driver to the database instance specified by the URL
     */
    public static Driver driver(URI uri) {
        return driver(uri, Config.defaultConfig());
    }

    /**
     * Return a driver for a Neo4j instance with custom configuration.
     *
     * @param uri the URL to a Neo4j instance
     * @param config user defined configuration
     * @return a new driver to the database instance specified by the URL
     */
    public static Driver driver(URI uri, Config config) {
        return driver(uri, AuthTokens.none(), config);
    }

    /**
     * Return a driver for a Neo4j instance with custom configuration.
     *
     * @param uri the URL to a Neo4j instance
     * @param config user defined configuration
     * @return a new driver to the database instance specified by the URL
     */
    public static Driver driver(String uri, Config config) {
        return driver(URI.create(uri), config);
    }

    /**
     * Return a driver for a Neo4j instance with the default configuration settings
     *
     * @param uri the URL to a Neo4j instance
     * @param authToken authentication to use, see {@link AuthTokens}
     * @return a new driver to the database instance specified by the URL
     */
    public static Driver driver(String uri, AuthToken authToken) {
        return driver(uri, authToken, Config.defaultConfig());
    }

    /**
     * Return a driver for a Neo4j instance with the default configuration settings
     *
     * @param uri the URL to a Neo4j instance
     * @param authToken authentication to use, see {@link AuthTokens}
     * @return a new driver to the database instance specified by the URL
     */
    public static Driver driver(URI uri, AuthToken authToken) {
        return driver(uri, authToken, Config.defaultConfig());
    }

    /**
     * Return a driver for a Neo4j instance with custom configuration.
     *
     * @param uri the URL to a Neo4j instance
     * @param authToken authentication to use, see {@link AuthTokens}
     * @param config user defined configuration
     * @return a new driver to the database instance specified by the URL
     */
    public static Driver driver(String uri, AuthToken authToken, Config config) {
        return driver(URI.create(uri), authToken, config);
    }

    /**
     * Return a driver for a Neo4j instance with custom configuration.
     *
     * @param uri       the URL to a Neo4j instance
     * @param authToken authentication to use, see {@link AuthTokens}
     * @param config    user defined configuration
     * @return a new driver to the database instance specified by the URL
     */
    public static Driver driver(URI uri, AuthToken authToken, Config config) {
        if (authToken == null) {
            authToken = AuthTokens.none();
        }
        return driver(uri, authToken, config, new DriverFactory());
    }

    /**
     * Returns a driver for a Neo4j instance with the default configuration settings and the provided
     * {@link AuthTokenManager}.
     *
     * @param uri the URL to a Neo4j instance
     * @param authTokenManager manager to use
     * @return a new driver to the database instance specified by the URL
     * @since 5.8
     * @see AuthTokenManager
     */
    public static Driver driver(URI uri, AuthTokenManager authTokenManager) {
        return driver(uri, authTokenManager, Config.defaultConfig());
    }

    /**
     * Returns a driver for a Neo4j instance with the default configuration settings and the provided
     * {@link AuthTokenManager}.
     *
     * @param uri the URL to a Neo4j instance
     * @param authTokenManager manager to use
     * @return a new driver to the database instance specified by the URL
     * @since 5.8
     * @see AuthTokenManager
     */
    public static Driver driver(String uri, AuthTokenManager authTokenManager) {
        return driver(URI.create(uri), authTokenManager);
    }

    /**
     * Returns a driver for a Neo4j instance with the provided {@link AuthTokenManager} and custom configuration.
     *
     * @param uri the URL to a Neo4j instance
     * @param authTokenManager manager to use
     * @param config user defined configuration
     * @return a new driver to the database instance specified by the URL
     * @since 5.8
     * @see AuthTokenManager
     */
    public static Driver driver(URI uri, AuthTokenManager authTokenManager, Config config) {
        return driver(uri, authTokenManager, config, new DriverFactory());
    }

    /**
     * Returns a driver for a Neo4j instance with the provided {@link AuthTokenManager} and custom configuration.
     *
     * @param uri the URL to a Neo4j instance
     * @param authTokenManager manager to use
     * @param config user defined configuration
     * @return a new driver to the database instance specified by the URL
     * @since 5.8
     * @see AuthTokenManager
     */
    public static Driver driver(String uri, AuthTokenManager authTokenManager, Config config) {
        return driver(URI.create(uri), authTokenManager, config);
    }

    private static Driver driver(URI uri, AuthToken authToken, Config config, DriverFactory driverFactory) {
        config = getOrDefault(config);
        return driverFactory.newInstance(uri, new StaticAuthTokenManager(authToken), config);
    }

    private static Driver driver(
            URI uri, AuthTokenManager authTokenManager, Config config, DriverFactory driverFactory) {
        requireNonNull(authTokenManager, "authTokenManager must not be null");
        config = getOrDefault(config);
        return driverFactory.newInstance(
                uri, new ValidatingAuthTokenManager(authTokenManager, config.logging()), config);
    }

    private static Config getOrDefault(Config config) {
        return config != null ? config : Config.defaultConfig();
    }
}
