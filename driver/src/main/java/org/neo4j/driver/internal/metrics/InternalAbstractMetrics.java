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

import java.util.Collection;
import java.util.Collections;

import org.neo4j.driver.ConnectionPoolMetrics;
import org.neo4j.driver.Metrics;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.async.pool.ConnectionPoolImpl;

public abstract class InternalAbstractMetrics implements Metrics, MetricsListener
{
    public static final InternalAbstractMetrics DEV_NULL_METRICS = new InternalAbstractMetrics()
    {

        @Override
        public void beforeCreating( String poolId, ListenerEvent creatingEvent )
        {

        }

        @Override
        public void afterCreated( String poolId, ListenerEvent creatingEvent )
        {

        }

        @Override
        public void afterFailedToCreate( String poolId )
        {

        }

        @Override
        public void afterClosed( String poolId )
        {

        }

        @Override
        public void beforeAcquiringOrCreating( String poolId, ListenerEvent acquireEvent )
        {

        }

        @Override
        public void afterAcquiringOrCreating( String poolId )
        {

        }

        @Override
        public void afterAcquiredOrCreated( String poolId, ListenerEvent acquireEvent )
        {

        }

        @Override
        public void afterTimedOutToAcquireOrCreate( String poolId )
        {

        }

        @Override
        public void afterConnectionCreated( String poolId, ListenerEvent inUseEvent )
        {

        }

        @Override
        public void afterConnectionReleased( String poolId, ListenerEvent inUseEvent )
        {

        }

        @Override
        public ListenerEvent createListenerEvent()
        {
            return ListenerEvent.DEV_NULL_LISTENER_EVENT;
        }

        @Override
        public void putPoolMetrics( String id, BoltServerAddress address, ConnectionPoolImpl connectionPool )
        {

        }

        @Override
        public void removePoolMetrics( String poolId )
        {

        }

        @Override
        public Collection<ConnectionPoolMetrics> connectionPoolMetrics()
        {
            return Collections.emptySet();
        }

        @Override
        public String toString()
        {
            return "Driver metrics not available while driver metrics is not enabled.";
        }
    };
}
