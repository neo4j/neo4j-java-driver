/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.driver.internal.metrics;

import java.util.concurrent.TimeUnit;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.async.NettyConnection;
import org.neo4j.driver.internal.async.pool.ConnectionPoolImpl;
import org.neo4j.driver.internal.metrics.ListenerEvent.ConnectionListenerEvent;
import org.neo4j.driver.internal.metrics.ListenerEvent.PoolListenerEvent;
import org.neo4j.driver.v1.Config;

public interface MetricsListener
{
    /**
     * Before creating a netty channel
     * @param serverAddress the server the netty channel binds to.
     * @param creatingEvent a connection listener event registered when a connection is creating.
     */
    void beforeCreating( BoltServerAddress serverAddress, ConnectionListenerEvent creatingEvent );

    /**
     * After a netty channel is created successfully
     * @param serverAddress the server the netty channel binds to
     */
    void afterCreated( BoltServerAddress serverAddress, ConnectionListenerEvent creatingEvent );

    /**
     * After a netty channel is created with failure
     * @param serverAddress the server the netty channel binds to
     */
    void afterFailedToCreate( BoltServerAddress serverAddress );

    /**
     * After a netty channel is closed successfully
     * @param serverAddress the server the netty channel binds to
     */
    void afterClosed( BoltServerAddress serverAddress );

    /**
     * After failed to acquire a connection from pool within maximum connection acquisition timeout set by
     * {@link Config.ConfigBuilder#withConnectionAcquisitionTimeout(long, TimeUnit)}
     * @param serverAddress
     */
    void afterTimedOutToAcquireOrCreate( BoltServerAddress serverAddress );

    /**
     * Before acquiring or creating a new netty channel from pool
     * @param serverAddress the server the netty channel binds to
     * @param acquireEvent a pool listener event registered in pool for this acquire event
     */
    void beforeAcquiringOrCreating( BoltServerAddress serverAddress, PoolListenerEvent acquireEvent );

    /**
     * After acquiring or creating a new netty channel from pool successfully.
     * @param serverAddress the server the netty channel binds to
     * @param acquireEvent a pool listener event registered in pool for this acquire event
     */
    void afterAcquiredOrCreated( BoltServerAddress serverAddress, PoolListenerEvent acquireEvent );

    /**
     * After acquiring or creating a new netty channel from pool successfully.
     * @param serverAddress the server the netty channel binds to
     * @param inUseEvent a connection listener registered with a {@link NettyConnection} when created
     */
    void afterAcquiredOrCreated( BoltServerAddress serverAddress, ConnectionListenerEvent inUseEvent );

    /**
     * After releasing a netty channel back to pool successfully
     * @param serverAddress the server the netty channel binds to
     * @param inUseEvent a connection listener registered with a {@link NettyConnection} when destroyed
     */
    void afterReleased( BoltServerAddress serverAddress, ConnectionListenerEvent inUseEvent );

    PoolListenerEvent createPoolListenerEvent();
    ConnectionListenerEvent createConnectionListenerEvent();

    void addMetrics( BoltServerAddress address, ConnectionPoolImpl connectionPool );
}
