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
import org.neo4j.driver.internal.ValidatingClientCertificateManager;
import org.neo4j.driver.internal.security.StaticAuthTokenManager;
import org.neo4j.driver.internal.security.ValidatingAuthTokenManager;

/**
 * Creates {@link Driver drivers}, optionally letting you {@link #driver(URI, Config)} to configure them.
 *
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
     * @param uri    the URL to a Neo4j instance
     * @param config user defined configuration
     * @return a new driver to the database instance specified by the URL
     */
    public static Driver driver(URI uri, Config config) {
        return driver(uri, AuthTokens.none(), config);
    }

    /**
     * Return a driver for a Neo4j instance with custom configuration.
     *
     * @param uri    the URL to a Neo4j instance
     * @param config user defined configuration
     * @return a new driver to the database instance specified by the URL
     */
    public static Driver driver(String uri, Config config) {
        return driver(URI.create(uri), config);
    }

    /**
     * Return a driver for a Neo4j instance with the default configuration settings
     *
     * @param uri       the URL to a Neo4j instance
     * @param authToken authentication to use, see {@link AuthTokens}
     * @return a new driver to the database instance specified by the URL
     */
    public static Driver driver(String uri, AuthToken authToken) {
        return driver(uri, authToken, Config.defaultConfig());
    }

    /**
     * Return a driver for a Neo4j instance with the default configuration settings
     *
     * @param uri       the URL to a Neo4j instance
     * @param authToken authentication to use, see {@link AuthTokens}
     * @return a new driver to the database instance specified by the URL
     */
    public static Driver driver(URI uri, AuthToken authToken) {
        return driver(uri, authToken, Config.defaultConfig());
    }

    /**
     * Return a driver for a Neo4j instance with custom configuration.
     *
     * @param uri       the URL to a Neo4j instance
     * @param authToken authentication to use, see {@link AuthTokens}
     * @param config    user defined configuration
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
        return driver(uri, authToken, null, config, new DriverFactory());
    }

    /**
     * Returns a driver for a Neo4j instance with the default configuration settings and the provided
     * {@link AuthTokenManager}.
     *
     * @param uri              the URL to a Neo4j instance
     * @param authTokenManager manager to use
     * @return a new driver to the database instance specified by the URL
     * @see AuthTokenManager
     * @since 5.8
     */
    public static Driver driver(URI uri, AuthTokenManager authTokenManager) {
        return driver(uri, authTokenManager, Config.defaultConfig());
    }

    /**
     * Returns a driver for a Neo4j instance with the default configuration settings and the provided
     * {@link AuthTokenManager}.
     *
     * @param uri              the URL to a Neo4j instance
     * @param authTokenManager manager to use
     * @return a new driver to the database instance specified by the URL
     * @see AuthTokenManager
     * @since 5.8
     */
    public static Driver driver(String uri, AuthTokenManager authTokenManager) {
        return driver(URI.create(uri), authTokenManager);
    }

    /**
     * Returns a driver for a Neo4j instance with the provided {@link AuthTokenManager} and custom configuration.
     *
     * @param uri              the URL to a Neo4j instance
     * @param authTokenManager manager to use
     * @param config           user defined configuration
     * @return a new driver to the database instance specified by the URL
     * @see AuthTokenManager
     * @since 5.8
     */
    public static Driver driver(URI uri, AuthTokenManager authTokenManager, Config config) {
        return driver(uri, authTokenManager, null, config, new DriverFactory());
    }

    /**
     * Returns a driver for a Neo4j instance with the provided {@link AuthTokenManager} and custom configuration.
     *
     * @param uri              the URL to a Neo4j instance
     * @param authTokenManager manager to use
     * @param config           user defined configuration
     * @return a new driver to the database instance specified by the URL
     * @see AuthTokenManager
     * @since 5.8
     */
    public static Driver driver(String uri, AuthTokenManager authTokenManager, Config config) {
        return driver(URI.create(uri), authTokenManager, config);
    }

    /**
     * Returns a driver for a Neo4j instance with the provided {@link ClientCertificateManager}.
     * @param uri the URL to a Neo4j instance
     * @param clientCertificateManager the client certificate manager
     * @return a new driver to the database instance specified by the URL
     * @since 5.19
     * @see ClientCertificateManager
     */
    public static Driver driver(String uri, ClientCertificateManager clientCertificateManager) {
        return driver(URI.create(uri), clientCertificateManager);
    }

    /**
     * Returns a driver for a Neo4j instance with the provided {@link ClientCertificateManager} and driver
     * {@link Config}.
     * @param uri the URL to a Neo4j instance
     * @param clientCertificateManager the client certificate manager
     * @param config the driver config
     * @return a new driver to the database instance specified by the URL
     * @since 5.19
     */
    public static Driver driver(String uri, ClientCertificateManager clientCertificateManager, Config config) {
        return driver(URI.create(uri), clientCertificateManager, config);
    }

    /**
     * Returns a driver for a Neo4j instance with the provided {@link AuthToken} and {@link ClientCertificateManager}.
     * @param uri the URL to a Neo4j instance
     * @param authToken the auth token
     * @param clientCertificateManager the client certificate manager
     * @return a new driver to the database instance specified by the URL
     * @since 5.19
     */
    public static Driver driver(String uri, AuthToken authToken, ClientCertificateManager clientCertificateManager) {
        return driver(URI.create(uri), authToken, clientCertificateManager);
    }

    /**
     * Returns a driver for a Neo4j instance with the provided {@link AuthToken}, {@link ClientCertificateManager} and
     * driver {@link Config}.
     * @param uri the URL to a Neo4j instance
     * @param authToken the auth token
     * @param clientCertificateManager the client certificate manager
     * @param config the driver config
     * @return a new driver to the database instance specified by the URL
     * @since 5.19
     */
    public static Driver driver(
            String uri, AuthToken authToken, ClientCertificateManager clientCertificateManager, Config config) {
        return driver(URI.create(uri), authToken, clientCertificateManager, config);
    }

    /**
     * Returns a driver for a Neo4j instance with the provided {@link AuthTokenManager} and
     * {@link ClientCertificateManager}.
     * @param uri the URL to a Neo4j instance
     * @param authTokenManager the auth token manager
     * @param clientCertificateManager the client certificate manager
     * @return a new driver to the database instance specified by the URL
     * @since 5.19
     */
    public static Driver driver(
            String uri, AuthTokenManager authTokenManager, ClientCertificateManager clientCertificateManager) {
        return driver(URI.create(uri), authTokenManager, clientCertificateManager);
    }

    /**
     * Returns a driver for a Neo4j instance with the provided {@link AuthTokenManager},
     * {@link ClientCertificateManager} and driver {@link Config}.
     * @param uri the URL to a Neo4j instance
     * @param authTokenManager the auth token manager
     * @param clientCertificateManager the client certificate manager
     * @param config the driver config
     * @return a new driver to the database instance specified by the URL
     * @since 5.19
     */
    public static Driver driver(
            String uri,
            AuthTokenManager authTokenManager,
            ClientCertificateManager clientCertificateManager,
            Config config) {
        return driver(URI.create(uri), authTokenManager, clientCertificateManager, config);
    }

    /**
     * Returns a driver for a Neo4j instance with the provided {@link ClientCertificateManager}.
     * @param uri the URL to a Neo4j instance
     * @param clientCertificateManager the client certificate manager
     * @return a new driver to the database instance specified by the URL
     * @since 5.19
     */
    public static Driver driver(URI uri, ClientCertificateManager clientCertificateManager) {
        return driver(uri, clientCertificateManager, Config.defaultConfig());
    }

    /**
     * Returns a driver for a Neo4j instance with the provided {@link ClientCertificateManager} and driver
     * {@link Config}.
     * @param uri the URL to a Neo4j instance
     * @param clientCertificateManager the client certificate manager
     * @param config the driver config
     * @return a new driver to the database instance specified by the URL
     * @since 5.19
     */
    public static Driver driver(URI uri, ClientCertificateManager clientCertificateManager, Config config) {
        return driver(uri, AuthTokens.none(), clientCertificateManager, config);
    }

    /**
     * Returns a driver for a Neo4j instance with the provided {@link AuthToken} and {@link ClientCertificateManager}.
     * @param uri the URL to a Neo4j instance
     * @param authToken the auth token
     * @param clientCertificateManager the client certificate manager
     * @return a new driver to the database instance specified by the URL
     * @since 5.19
     */
    public static Driver driver(URI uri, AuthToken authToken, ClientCertificateManager clientCertificateManager) {
        return driver(uri, authToken, clientCertificateManager, Config.defaultConfig());
    }

    /**
     * Returns a driver for a Neo4j instance with the provided {@link AuthToken}, {@link ClientCertificateManager} and
     * driver {@link Config}.
     * @param uri the URL to a Neo4j instance
     * @param authToken the auth token
     * @param clientCertificateManager the client certificate manager
     * @param config the driver config
     * @return a new driver to the database instance specified by the URL
     * @since 5.19
     */
    public static Driver driver(
            URI uri, AuthToken authToken, ClientCertificateManager clientCertificateManager, Config config) {
        return driver(uri, authToken, clientCertificateManager, config, new DriverFactory());
    }

    /**
     * Returns a driver for a Neo4j instance with the provided {@link AuthTokenManager} and
     * {@link ClientCertificateManager}.
     * @param uri the URL to a Neo4j instance
     * @param authTokenManager the auth token manager
     * @param clientCertificateManager the client certificate manager
     * @return a new driver to the database instance specified by the URL
     * @since 5.19
     */
    public static Driver driver(
            URI uri, AuthTokenManager authTokenManager, ClientCertificateManager clientCertificateManager) {
        return driver(uri, authTokenManager, clientCertificateManager, Config.defaultConfig());
    }

    /**
     * Returns a driver for a Neo4j instance with the provided {@link AuthTokenManager},
     * {@link ClientCertificateManager} and driver {@link Config}.
     * @param uri the URL to a Neo4j instance
     * @param authTokenManager the auth token manager
     * @param clientCertificateManager the client certificate manager
     * @param config the driver config
     * @return a new driver to the database instance specified by the URL
     * @since 5.19
     */
    public static Driver driver(
            URI uri,
            AuthTokenManager authTokenManager,
            ClientCertificateManager clientCertificateManager,
            Config config) {
        return driver(uri, authTokenManager, clientCertificateManager, config, new DriverFactory());
    }

    private static Driver driver(
            URI uri,
            AuthToken authToken,
            ClientCertificateManager clientCertificateManager,
            Config config,
            DriverFactory driverFactory) {
        if (clientCertificateManager != null) {
            clientCertificateManager = new ValidatingClientCertificateManager(clientCertificateManager);
        }
        config = getOrDefault(config);
        return driverFactory.newInstance(uri, new StaticAuthTokenManager(authToken), clientCertificateManager, config);
    }

    @SuppressWarnings("deprecation")
    private static Driver driver(
            URI uri,
            AuthTokenManager authTokenManager,
            ClientCertificateManager clientCertificateManager,
            Config config,
            DriverFactory driverFactory) {
        requireNonNull(authTokenManager, "authTokenManager must not be null");
        if (clientCertificateManager != null) {
            clientCertificateManager = new ValidatingClientCertificateManager(clientCertificateManager);
        }
        config = getOrDefault(config);
        return driverFactory.newInstance(
                uri,
                new ValidatingAuthTokenManager(authTokenManager, config.logging()),
                clientCertificateManager,
                config);
    }

    private static Config getOrDefault(Config config) {
        return config != null ? config : Config.defaultConfig();
    }
}
