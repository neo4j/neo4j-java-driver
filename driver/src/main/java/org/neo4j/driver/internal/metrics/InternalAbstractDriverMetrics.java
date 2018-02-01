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

import java.util.Map;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.metrics.spi.ConnectionMetrics;
import org.neo4j.driver.internal.metrics.spi.ConnectionPoolMetrics;
import org.neo4j.driver.internal.metrics.spi.DriverMetrics;
import org.neo4j.driver.internal.spi.ConnectionPool;

public abstract class InternalAbstractDriverMetrics implements DriverMetrics, DriverMetricsHandler
{
    public static final InternalAbstractDriverMetrics DEV_NULL_METRICS = new InternalAbstractDriverMetrics()
    {
        @Override
        public void addPoolMetrics( BoltServerAddress serverAddress, ConnectionPool pool )
        {

        }

        @Override
        public void beforeCreating( BoltServerAddress serverAddress )
        {

        }

        @Override
        public void afterCreatedSuccessfully( BoltServerAddress serverAddress )
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
        public void beforeAcquiring( BoltServerAddress serverAddress, ListenerEvent listenerEvent )
        {

        }

        @Override
        public void afterAcquired( BoltServerAddress serverAddress, ListenerEvent listenerEvent )
        {

        }

        @Override
        public Map<String,ConnectionPoolMetrics> connectionPoolMetrics()
        {
            return null;
        }

        @Override
        public Map<String,ConnectionMetrics> connectionMetrics()
        {
            return null;
        }

        @Override
        public String toString()
        {
            return "Driver metrics not available while driver metrics is not enabled.";
        }
    };
}
