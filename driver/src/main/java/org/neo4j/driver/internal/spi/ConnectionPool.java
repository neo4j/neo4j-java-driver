/*
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
package org.neo4j.driver.internal.spi;

import org.neo4j.driver.internal.exceptions.BoltProtocolException;
import org.neo4j.driver.internal.exceptions.ConnectionException;
import org.neo4j.driver.internal.exceptions.InvalidOperationException;
import org.neo4j.driver.internal.exceptions.ServerNeo4jException;
import org.neo4j.driver.internal.net.BoltServerAddress;

public interface ConnectionPool extends AutoCloseable
{
    /**
     * Acquire a connection - if a live connection exists in the pool, it will
     * be used, otherwise a new connection will be created.
     *
     * @param address The address to acquire
     * @throws ConnectionException if failed to connect to server with the provided address due to network errors.
     * @throws InvalidOperationException if a user misuses the driver such as providing a wrong bolt port or
     * trying to obtain a connection after the pool is already closed.
     * When this exception is thrown, {@link ConnectionPool} ensures that no connection will be added into the pool
     * and will try with best-effort to close any resource that should not have been opened.
     * @throws ServerNeo4jException if failed to run a statement on the server.
     * @throws BoltProtocolException if failed to encode/decode bolt messages
     */
    PooledConnection acquire( BoltServerAddress address )
            throws ConnectionException, InvalidOperationException, ServerNeo4jException, BoltProtocolException;

    /**
     * Removes all connections to a given address from the pool.
     * @param address The address to remove.
     * @throws InvalidOperationException if failed to close the connections in the connection pool properly
     */
    void purge( BoltServerAddress address ) throws InvalidOperationException;

    boolean hasAddress( BoltServerAddress address );
}
