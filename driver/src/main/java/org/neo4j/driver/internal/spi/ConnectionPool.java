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
package org.neo4j.driver.internal.spi;

import org.neo4j.driver.internal.net.BoltServerAddress;

public interface ConnectionPool extends AutoCloseable
{
    /**
     * Acquire a connection - if a live connection exists in the pool, it will
     * be used, otherwise a new connection will be created.
     *
     * @param address The address to acquire
     */
    PooledConnection acquire( BoltServerAddress address );

    /**
     * Removes all connections to a given address from the pool.
     * @param address The address to remove.
     */
    void purge( BoltServerAddress address );

    boolean hasAddress( BoltServerAddress address );
}
