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
package org.neo4j.driver;

import java.net.URI;
import org.neo4j.driver.internal.DriverFactory;
import org.neo4j.driver.internal.SecuritySettings;
import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.retry.RetrySettings;
import org.neo4j.driver.internal.security.SecurityPlan;

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
        return driver(uri, authToken, config, new DriverFactory());
    }

    static Driver driver(URI uri, AuthToken authToken, Config config, DriverFactory driverFactory) {
        config = getOrDefault(config);
        RoutingSettings routingSettings = config.routingSettings();
        RetrySettings retrySettings = config.retrySettings();
        SecuritySettings securitySettings = config.securitySettings();
        SecurityPlan securityPlan = securitySettings.createSecurityPlan(uri.getScheme());
        return driverFactory.newInstance(uri, authToken, routingSettings, retrySettings, config, securityPlan);
    }

    private static Config getOrDefault(Config config) {
        return config != null ? config : Config.defaultConfig();
    }
}
