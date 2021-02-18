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

import org.neo4j.driver.Metrics;
import org.neo4j.driver.exceptions.ClientException;

import static org.neo4j.driver.internal.metrics.InternalAbstractMetrics.DEV_NULL_METRICS;

public interface MetricsProvider
{
    MetricsProvider METRICS_DISABLED_PROVIDER = new MetricsProvider()
    {
        @Override
        public Metrics metrics()
        {
            // To outside users, we forbidden their access to the metrics API
            throw new ClientException( "Driver metrics not enabled. To access driver metrics, " +
                    "you need to enabled driver metrics in the driver's configuration." );
        }

        @Override
        public MetricsListener metricsListener()
        {
            // Internally we can still register callbacks to this empty metrics listener.
            return DEV_NULL_METRICS;
        }

        @Override
        public boolean isMetricsEnabled()
        {
            return false;
        }
    };

    Metrics metrics();

    MetricsListener metricsListener();

    boolean isMetricsEnabled();
}
