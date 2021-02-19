/*
 * Copyright (c) "Neo4j"
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

public interface ConnectionPoolMetricsListener
{
    /**
     * Invoked before a connection is creating.
     */
    void beforeCreating( ListenerEvent listenerEvent );

    /**
     * Invoked after a connection is created successfully.
     */
    void afterCreated( ListenerEvent listenerEvent );

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
     * @param acquireEvent
     */
    void beforeAcquiringOrCreating( ListenerEvent acquireEvent );

    /**
     * Invoked after a connection is being acquired or created regardless weather it is successful or not.
     */
    void afterAcquiringOrCreating();

    /**
     * Invoked after a connection is acquired or created successfully.
     * @param acquireEvent
     */
    void afterAcquiredOrCreated( ListenerEvent acquireEvent );

    /**
     * Invoked after it is timed out to acquire or create a connection.
     */
    void afterTimedOutToAcquireOrCreate();

    /**
     * After a connection is acquired from the pool.
     * @param inUseEvent
     */
    void acquired( ListenerEvent inUseEvent );

    /**
     * After a connection is released back to pool.
     * @param inUseEvent
     */
    void released( ListenerEvent inUseEvent );

    ConnectionPoolMetricsListener DEV_NULL_POOL_METRICS_LISTENER = new ConnectionPoolMetricsListener()
    {
        @Override
        public void beforeCreating( ListenerEvent listenerEvent )
        {

        }

        @Override
        public void afterCreated( ListenerEvent listenerEvent )
        {

        }

        @Override
        public void afterFailedToCreate()
        {

        }

        @Override
        public void afterClosed()
        {

        }

        @Override
        public void beforeAcquiringOrCreating( ListenerEvent acquireEvent )
        {

        }

        @Override
        public void afterAcquiringOrCreating()
        {

        }

        @Override
        public void afterAcquiredOrCreated( ListenerEvent acquireEvent )
        {

        }

        @Override
        public void afterTimedOutToAcquireOrCreate()
        {

        }

        @Override
        public void acquired( ListenerEvent inUseEvent )
        {

        }

        @Override
        public void released( ListenerEvent inUseEvent )
        {

        }
    };
}

