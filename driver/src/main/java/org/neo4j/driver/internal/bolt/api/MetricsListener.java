/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.driver.internal.bolt.api;

import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;
import org.neo4j.driver.Config;

public interface MetricsListener {
    /**
     * Before creating a netty channel.
     *
     * @param poolId        the id of the pool where the netty channel lives.
     * @param creatingEvent a connection listener event registered when a connection is creating.
     */
    void beforeCreating(String poolId, ListenerEvent<?> creatingEvent);

    /**
     * After a netty channel is created successfully.
     *
     * @param poolId the id of the pool where the netty channel lives.
     */
    void afterCreated(String poolId, ListenerEvent<?> creatingEvent);

    /**
     * After a netty channel is created with a failure.
     * @param poolId the id of the pool where the netty channel lives.
     */
    void afterFailedToCreate(String poolId);

    /**
     * After a netty channel is closed successfully.
     * @param poolId the id of the pool where the netty channel lives.
     */
    void afterClosed(String poolId);

    /**
     * Before acquiring or creating a new netty channel from pool.
     *
     * @param poolId       the id of the pool where the netty channel lives.
     * @param acquireEvent a pool listener event registered in pool for this acquire event.
     */
    void beforeAcquiringOrCreating(String poolId, ListenerEvent<?> acquireEvent);

    /**
     * After acquiring or creating a new netty channel from pool regardless it is successful or not.
     * @param poolId the id of the pool where the netty channel lives.
     */
    void afterAcquiringOrCreating(String poolId);

    /**
     * After acquiring or creating a new netty channel from pool successfully.
     *
     * @param poolId       the id of the pool where the netty channel lives.
     * @param acquireEvent a pool listener event registered in pool for this acquire event.
     */
    void afterAcquiredOrCreated(String poolId, ListenerEvent<?> acquireEvent);

    /**
     * After we failed to acquire a connection from pool within maximum connection acquisition timeout set by
     * {@link Config.ConfigBuilder#withConnectionAcquisitionTimeout(long, TimeUnit)}.
     * @param poolId the id of the pool where the netty channel lives.
     */
    void afterTimedOutToAcquireOrCreate(String poolId);

    /**
     * After acquiring or creating a new netty channel from pool successfully.
     *
     * @param poolId     the id of the pool where the netty channel lives.
     * @param inUseEvent a connection listener event fired from the newly created connection.
     */
    void afterConnectionCreated(String poolId, ListenerEvent<?> inUseEvent);

    /**
     * After releasing a netty channel back to pool successfully.
     *
     * @param poolId     the id of the pool where the netty channel lives.
     * @param inUseEvent a connection listener event fired from the connection being released.
     */
    void afterConnectionReleased(String poolId, ListenerEvent<?> inUseEvent);

    ListenerEvent<?> createListenerEvent();

    void registerPoolMetrics(
            String poolId, BoltServerAddress serverAddress, IntSupplier inUseSupplier, IntSupplier idleSupplier);

    void removePoolMetrics(String poolId);
}
