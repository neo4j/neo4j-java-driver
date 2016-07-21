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

package org.neo4j.driver.internal.net.pooling;

public class PoolSettings
{
    public static PoolSettings defaultSettings()
    {
        return new PoolSettings( DEFAULT_MAX_IDLE_CONNECTION_POOL_SIZE, DEFAULT_IDLE_TIME_BEFORE_CONNECTION_TEST );
    }

    public static final int DEFAULT_MAX_IDLE_CONNECTION_POOL_SIZE = 10;
    public static final long DEFAULT_IDLE_TIME_BEFORE_CONNECTION_TEST = 200;

    /**
     * Maximum number of idle connections per pool.
     */
    private final int maxIdleConnectionPoolSize;

    /**
     * Connections that have been idle longer than this threshold will have a ping test performed on them.
     */
    private final long idleTimeBeforeConnectionTest;

    public PoolSettings( int maxIdleConnectionPoolSize, long idleTimeBeforeConnectionTest )
    {
        this.maxIdleConnectionPoolSize = maxIdleConnectionPoolSize;
        this.idleTimeBeforeConnectionTest = idleTimeBeforeConnectionTest;
    }

    public int maxIdleConnectionPoolSize()
    {
        return maxIdleConnectionPoolSize;
    }

    public long idleTimeBeforeConnectionTest()
    {
        return idleTimeBeforeConnectionTest;
    }

}
