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

import java.util.Collections;
import java.util.Map;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.async.pool.ConnectionPoolImpl;
import org.neo4j.driver.ConnectionPoolMetrics;
import org.neo4j.driver.Metrics;

public abstract class InternalAbstractMetrics implements Metrics, MetricsListener
{
    public static final InternalAbstractMetrics DEV_NULL_METRICS = new InternalAbstractMetrics()
    {
        @Override
        public void beforeCreating( BoltServerAddress serverAddress, ListenerEvent creatingEvent )
        {

        }

        @Override
        public void afterCreated( BoltServerAddress serverAddress, ListenerEvent creatingEvent )
        {

        }

        @Override
        public void afterFailedToCreate( BoltServerAddress serverAddress )
        {

        }

        @Override
        public void afterClosed( BoltServerAddress serverAddress )
        {

        }

        @Override
        public void afterTimedOutToAcquireOrCreate( BoltServerAddress serverAddress )
        {

        }

        @Override
        public void beforeAcquiringOrCreating( BoltServerAddress serverAddress, ListenerEvent acquireEvent )
        {

        }

        @Override
        public void afterAcquiringOrCreating( BoltServerAddress serverAddress )
        {

        }

        @Override
        public void afterAcquiredOrCreated( BoltServerAddress serverAddress, ListenerEvent acquireEvent )
        {

        }

        @Override
        public void afterConnectionCreated( BoltServerAddress serverAddress, ListenerEvent inUseEvent )
        {

        }

        @Override
        public void afterConnectionReleased( BoltServerAddress serverAddress, ListenerEvent inUseEvent )
        {

        }

        @Override
        public ListenerEvent createListenerEvent()
        {
            return ListenerEvent.DEV_NULL_LISTENER_EVENT;
        }

        @Override
        public void putPoolMetrics( BoltServerAddress address, ConnectionPoolImpl connectionPool )
        {

        }

        @Override
        public Map<String,ConnectionPoolMetrics> connectionPoolMetrics()
        {
            return Collections.emptyMap();
        }

        @Override
        public Metrics snapshot()
        {
            return this;
        }

        @Override
        public String toString()
        {
            return "Driver metrics not available while driver metrics is not enabled.";
        }
    };
}
