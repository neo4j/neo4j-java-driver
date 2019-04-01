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
package org.neo4j.driver.internal.metrics;

import java.util.concurrent.TimeUnit;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.async.connection.DirectConnection;
import org.neo4j.driver.internal.async.pool.ConnectionPoolImpl;
import org.neo4j.driver.Config;

public interface MetricsListener
{
    /**
     * Before creating a netty channel.
     * @param serverAddress the server the netty channel binds to.
     * @param creatingEvent a connection listener event registered when a connection is creating.
     */
    void beforeCreating( BoltServerAddress serverAddress, ListenerEvent creatingEvent );

    /**
     * After a netty channel is created successfully.
     * @param serverAddress the server the netty channel binds to.
     */
    void afterCreated( BoltServerAddress serverAddress, ListenerEvent creatingEvent );

    /**
     * After a netty channel is created with a failure.
     * @param serverAddress the server the netty channel binds to.
     */
    void afterFailedToCreate( BoltServerAddress serverAddress );

    /**
     * After a netty channel is closed successfully.
     * @param serverAddress the server the netty channel binds to.
     */
    void afterClosed( BoltServerAddress serverAddress );

    /**
     * Before acquiring or creating a new netty channel from pool.
     * @param serverAddress the server the netty channel binds to.
     * @param acquireEvent a pool listener event registered in pool for this acquire event.
     */
    void beforeAcquiringOrCreating( BoltServerAddress serverAddress, ListenerEvent acquireEvent );

    /**
     * After acquiring or creating a new netty channel from pool regardless it is successful or not.
     * @param serverAddress the server the netty channel binds to.
     */
    void afterAcquiringOrCreating( BoltServerAddress serverAddress );

    /**
     * After acquiring or creating a new netty channel from pool successfully.
     * @param serverAddress the server the netty channel binds to.
     * @param acquireEvent a pool listener event registered in pool for this acquire event.
     */
    void afterAcquiredOrCreated( BoltServerAddress serverAddress, ListenerEvent acquireEvent );

    /**
     * After we failed to acquire a connection from pool within maximum connection acquisition timeout set by
     * {@link Config.ConfigBuilder#withConnectionAcquisitionTimeout(long, TimeUnit)}.
     * @param serverAddress
     */
    void afterTimedOutToAcquireOrCreate( BoltServerAddress serverAddress );

    /**
     * After acquiring or creating a new netty channel from pool successfully.
     * @param serverAddress the server the netty channel binds to.
     * @param inUseEvent a connection listener registered with a {@link DirectConnection} when created.
     */
    void afterConnectionCreated( BoltServerAddress serverAddress, ListenerEvent inUseEvent );

    /**
     * After releasing a netty channel back to pool successfully.
     * @param serverAddress the server the netty channel binds to.
     * @param inUseEvent a connection listener registered with a {@link DirectConnection} when destroyed.
     */
    void afterConnectionReleased( BoltServerAddress serverAddress, ListenerEvent inUseEvent );

    ListenerEvent createListenerEvent();

    void putPoolMetrics( BoltServerAddress address, ConnectionPoolImpl connectionPool );
}
