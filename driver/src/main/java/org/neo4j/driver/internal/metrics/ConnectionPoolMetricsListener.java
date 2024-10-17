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
package org.neo4j.driver.internal.metrics;

import org.neo4j.driver.internal.bolt.api.ListenerEvent;

interface ConnectionPoolMetricsListener {
    /**
     * Invoked before a connection is creating.
     */
    void beforeCreating(ListenerEvent<?> listenerEvent);

    /**
     * Invoked after a connection is created successfully.
     */
    void afterCreated(ListenerEvent<?> listenerEvent);

    /**
     * Invoked after a connection is failed to create due to timeout, any kind of error.
     */
    void afterFailedToCreate();

    /**
     * Invoked after a connection is closed.
     */
    void afterClosed();

    /**
     * Invoked before acquiring or creating a connection.
     *
     * @param acquireEvent the event
     */
    void beforeAcquiringOrCreating(ListenerEvent<?> acquireEvent);

    /**
     * Invoked after a connection is being acquired or created regardless weather it is successful or not.
     */
    void afterAcquiringOrCreating();

    /**
     * Invoked after a connection is acquired or created successfully.
     *
     * @param acquireEvent the event
     */
    void afterAcquiredOrCreated(ListenerEvent<?> acquireEvent);

    /**
     * Invoked after it is timed out to acquire or create a connection.
     */
    void afterTimedOutToAcquireOrCreate();

    /**
     * After a connection is acquired from the pool.
     *
     * @param inUseEvent the event
     */
    void acquired(ListenerEvent<?> inUseEvent);

    /**
     * After a connection is released back to pool.
     *
     * @param inUseEvent the event
     */
    void released(ListenerEvent<?> inUseEvent);
}
