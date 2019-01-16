/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.driver.internal.metrics.spi.ConnectionPoolMetricsTracker;
import org.neo4j.driver.internal.metrics.spi.ConnectionPoolMetricsTrackerProvider;

public final class SimpleConnectionPoolMetricsTrackerProvider implements ConnectionPoolMetricsTrackerProvider
{
    private final Map<String,ConnectionPoolMetricsTracker> connectionPoolMetricsTrackers = new ConcurrentHashMap<>();

    @Override
    public ConnectionPoolMetricsTracker metricsTrackerForPool( String poolId )
    {
        // if the tracker for the pool id does not exist, then we create a new one immediately.
        return connectionPoolMetricsTrackers.computeIfAbsent( poolId, SimpleConnectionPoolMetricsTracker::new );
    }
}
