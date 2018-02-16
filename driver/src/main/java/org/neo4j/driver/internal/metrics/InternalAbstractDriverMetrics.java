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

import java.util.Collections;
import java.util.Map;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.async.pool.ConnectionPoolImpl;
import org.neo4j.driver.internal.metrics.spi.ConnectionMetrics;
import org.neo4j.driver.internal.metrics.spi.ConnectionPoolMetrics;
import org.neo4j.driver.internal.metrics.spi.DriverMetrics;

public abstract class InternalAbstractDriverMetrics implements DriverMetrics, DriverMetricsListener
{
    public static final InternalAbstractDriverMetrics DEV_NULL_METRICS = new InternalAbstractDriverMetrics()
    {

        @Override
        public void beforeCreating( BoltServerAddress serverAddress, ListenerEvent.ConnectionListenerEvent creatingEvent )
        {
        }

        @Override
        public void afterCreating( BoltServerAddress serverAddress, ListenerEvent.ConnectionListenerEvent creatingEvent )
        {
        }

        @Override
        public void afterCreated( BoltServerAddress serverAddress )
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
        public void beforeAcquiringOrCreating( BoltServerAddress serverAddress, ListenerEvent.PoolListenerEvent acquireEvent )
        {
        }

        @Override
        public void afterAcquiringOrCreating( BoltServerAddress serverAddress, ListenerEvent.PoolListenerEvent acquireEvent )
        {
        }

        @Override
        public void afterAcquiredOrCreated( BoltServerAddress serverAddress, ListenerEvent.ConnectionListenerEvent inUseEvent )
        {
        }

        @Override
        public void afterReleased( BoltServerAddress serverAddress, ListenerEvent.ConnectionListenerEvent inUseEvent )
        {
        }

        @Override
        public ListenerEvent.ConnectionListenerEvent createConnectionListenerEvent()
        {
            return null;
        }

        @Override
        public ListenerEvent.PoolListenerEvent createPoolListenerEvent()
        {
            return null;
        }

        @Override
        public void addMetrics( BoltServerAddress address, ConnectionPoolImpl connectionPool )
        {

        }

        @Override
        public Map<String,ConnectionPoolMetrics> connectionPoolMetrics()
        {
            return Collections.emptyMap();
        }

        @Override
        public Map<String,ConnectionMetrics> connectionMetrics()
        {
            return Collections.emptyMap();
        }

        @Override
        public String toString()
        {
            return "Driver metrics not available while driver metrics is not enabled.";
        }
    };
}
