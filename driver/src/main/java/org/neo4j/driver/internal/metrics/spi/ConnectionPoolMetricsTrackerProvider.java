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
package org.neo4j.driver.internal.metrics.spi;

public interface ConnectionPoolMetricsTrackerProvider
{
    /**
     * Get the connection pool metrics tracker of a specific pool identified by the pool id.
     * Ideally, it is suggested to provide a metrics tracker per pool.
     * Each connection pool connects to a different server. It makes more sense to monitor each server separately regarding socket connection time etc.
     * NOTE: This method will be concurrently called in multiple threads.
     * @param poolId uniquely identifies a pool. See {@link ConnectionPoolMetrics#id()} for more information.
     * @return A connection pool metrics tracker.
     */
    ConnectionPoolMetricsTracker metricsTrackerForPool( String poolId );
}
