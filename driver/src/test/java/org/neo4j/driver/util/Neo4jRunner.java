/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.driver.util;

import java.io.File;
import java.net.URI;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.internal.BoltServerAddress;

import static java.util.logging.Level.INFO;
import static org.neo4j.driver.AuthTokens.basic;
import static org.neo4j.driver.Logging.console;
import static org.neo4j.driver.util.Neo4jSettings.CURRENT_BOLT_PORT;
import static org.neo4j.driver.util.Neo4jSettings.CURRENT_HTTP_PORT;
import static org.neo4j.driver.util.Neo4jSettings.TEST_JVM_ID;

public interface Neo4jRunner
{
    String NEO4J_VERSION = System.getProperty( "neo4jVersion", "3.5-enterprise" );
    Config DEFAULT_DRIVER_CONFIG = Config.builder().withLogging( console( INFO ) ).build();

    String USER = "neo4j";
    String PASSWORD = "password";
    AuthToken DEFAULT_AUTH_TOKEN = basic( USER, PASSWORD );

    String TARGET_DIR = new File( "../target" ).getAbsolutePath();
    String NEO4J_DIR = new File( TARGET_DIR, "test-server-" + TEST_JVM_ID ).getAbsolutePath();
    String CLUSTER_DIR = new File( TARGET_DIR, "test-cluster" ).getAbsolutePath();

    void ensureRunning( Neo4jSettings neo4jSettings );

    Driver driver();

    void startNeo4j();

    void stopNeo4j();

    /**
     * Restart immediately regardless if any configuration has been changed.
     */
    void restartNeo4j();

    /**
     * Will only restart the server if configuration differs from the current one.
     */
    void restartNeo4j( Neo4jSettings neo4jSettings );

    File certificatesDirectory();

    File importDirectory();

    File pluginsDirectory();

    default int httpPort()
    {
        return CURRENT_HTTP_PORT;
    }

    default int boltPort()
    {
        return CURRENT_BOLT_PORT;
    }

    default BoltServerAddress boltAddress()
    {
        return new BoltServerAddress( boltUri() );
    }

    default URI boltUri()
    {
        return URI.create( "bolt://localhost:" + boltPort() );
    }

    static void debug( String text, Object... args )
    {
        System.out.println( String.format( text, args ) );
    }
}
