/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.driver.internal.net.pooling;

public class PoolSettings
{
    public static final int NO_IDLE_CONNECTION_TEST = -1;
    public static final int INFINITE_CONNECTION_LIFETIME = -1;

    public static final int DEFAULT_MAX_IDLE_CONNECTION_POOL_SIZE = 10;
    public static final int DEFAULT_IDLE_TIME_BEFORE_CONNECTION_TEST = NO_IDLE_CONNECTION_TEST;
    public static final int DEFAULT_MAX_CONNECTION_LIFETIME = INFINITE_CONNECTION_LIFETIME;

    private final int maxIdleConnectionPoolSize;
    private final long idleTimeBeforeConnectionTest;
    private final long maxConnectionLifetime;

    public PoolSettings( int maxIdleConnectionPoolSize, long idleTimeBeforeConnectionTest, long maxConnectionLifetime )
    {
        this.maxIdleConnectionPoolSize = maxIdleConnectionPoolSize;
        this.idleTimeBeforeConnectionTest = idleTimeBeforeConnectionTest;
        this.maxConnectionLifetime = maxConnectionLifetime;
    }

    public int maxIdleConnectionPoolSize()
    {
        return maxIdleConnectionPoolSize;
    }

    public long idleTimeBeforeConnectionTest()
    {
        if ( !idleTimeBeforeConnectionTestConfigured() )
        {
            throw new IllegalStateException(
                    "Idle time before connection test is not configured: " + idleTimeBeforeConnectionTest );
        }
        return idleTimeBeforeConnectionTest;
    }

    public boolean idleTimeBeforeConnectionTestConfigured()
    {
        return idleTimeBeforeConnectionTest >= 0;
    }

    public long maxConnectionLifetime()
    {
        if ( !maxConnectionLifetimeConfigured() )
        {
            throw new IllegalStateException(
                    "Max connection lifetime is not configured: " + maxConnectionLifetime );
        }
        return maxConnectionLifetime;
    }

    public boolean maxConnectionLifetimeConfigured()
    {
        return maxConnectionLifetime > 0;
    }
}
