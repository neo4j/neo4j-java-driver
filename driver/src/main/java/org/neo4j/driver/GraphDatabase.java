/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.driver;

import java.net.URI;

/**
 * A static class to create a {@link Driver driver}
 * <p>
 * An example:
 * <pre>
 * {@code
 *
 * // Create a driver with default configuration
 * Driver driver = GraphDatabase.driver( "neo4j://localhost" );
 *
 * // Establish a session with a Neo4j instance
 * Session session = driver.session();
 *
 * // And when you are done, make sure all resources are released
 * driver.close();
 * }
 * </pre>
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
        return driver( URI.create( url ) );
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
        return new Driver( url, config );
    }
}
