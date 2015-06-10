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

import java.util.logging.Level;

import org.neo4j.driver.internal.logging.JULogging;
import org.neo4j.driver.internal.spi.Logging;

/**
 * A configuration class to config driver properties.
 * <p>
 * To create a config:
 * <pre>
 * {@code
 * Config config = Config
 *                  .build()
 *               ** .withProperties() **
 *                  .toConfig();
 * }
 * </pre>
 */
public class Config
{
    public static final String SCHEME = "neo4j";
    public static final int DEFAULT_PORT = 7687;

    /** User defined logging */
    public final Logging logging;

    /** The size of connection pool for each database url */
    public final int connectionPoolSize;
    /** Connections that have been idle longer than this threshold will have a ping test performed on them. */
    public final int connectionIdleTimeout;
    /**
     * Timeout for network read operations. By default, this is disabled (the database may take as long as it likes to
     * reply). However, on networks that suffer from frequent net-splits, there is a serious issue where a socket may
     * erroneously block for very long periods (up to 10 minutes). If your application suffers from this issue, you
     * should enable the network timeout, by setting it to some value significantly higher than your slowest query.
     */
    public final int soTimeout; //TODO

    private Config( ConfigBuilder builder )
    {
        this.logging = builder.logging;

        this.connectionPoolSize = builder.connectionPoolSize;
        this.connectionIdleTimeout = builder.connectionIdleTimeout;
        this.soTimeout = builder.soTimeout;
    }

    public static ConfigBuilder build()
    {
        return new ConfigBuilder();
    }

    /**
     * @return A config with all default settings
     */
    public static Config defaultConfig()
    {
        return Config.build().toConfig();
    }

    public static class ConfigBuilder
    {
        private Logging logging = new JULogging( Level.INFO );

        private int connectionPoolSize = 10;
        private int connectionIdleTimeout = 200;
        private int soTimeout = 10;

        private ConfigBuilder()
        {

        }

        public ConfigBuilder withLogging( Logging logging )
        {
            this.logging = logging;
            return this;
        }

        public ConfigBuilder withConnectionPoolSize( int size )
        {
            this.connectionPoolSize = size;
            return this;
        }

        public ConfigBuilder withConnectionIdleTimeout( int milliSecond )
        {
            this.connectionIdleTimeout = milliSecond;
            return this;
        }

        public ConfigBuilder withSoTimeout( int milliSecond )
        {
            this.soTimeout = milliSecond;
            return this;
        }

        public Config toConfig()
        {
            return new Config( this );
        }
    }
}
