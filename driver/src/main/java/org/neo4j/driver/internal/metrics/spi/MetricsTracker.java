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
package org.neo4j.driver.internal.metrics.spi;

/**
 * Tracks events that happened in connection pool.
 * The methods in this class will be invoked in the critical driver threading and pooling code.
 * We expect no blocking, no error, no deadlock in the your implementation.
 */
public interface MetricsTracker
{
    MetricsTracker DEV_NULL_METRICS_TRACKER = new MetricsTracker()
    {
        @Override
        public void recordAcquisitionTime( String poolId, long timeInMs )
        {

        }

        @Override
        public void recordConnectionTime( String poolId, long timeInMs )
        {

        }

        @Override
        public void recordInUseTime( String poolId, long timeInMs )
        {

        }
    };

    /**
     * Record the connection acquisition time after a connection is acquired.
     * The connection acquisition time could either be spent to establish a new connection or direct grab an existing connection from the pool.
     * Note: This code will be invoked concurrently by multiple threads.
     * @param poolId uniquely identifies a pool. See {@link ConnectionPoolMetrics#id()}.
     * @param timeInMs time spent to acquire or create a connection in millis.
     */
    void recordAcquisitionTime( String poolId, long timeInMs );

    /**
     * Record the time in millis spent to establish a new connection.
     * Note: Method will be invoked concurrently by multiple threads.
     * @param poolId uniquely identifies a pool. See {@link ConnectionPoolMetrics#id()}.
     * @param timeInMs connection establishing time in millis.
     */
    void recordConnectionTime( String poolId, long timeInMs );

    /**
     * Record the time in millis spent outside pool, a.k.a. used by application code to run queries.
     * Note: Method will be invoked concurrently by multiple threads.
     * @param poolId uniquely identifies a pool. See {@link ConnectionPoolMetrics#id()}.
     * @param timeInMs connection is used outside the pool in millis.
     */
    void recordInUseTime( String poolId, long timeInMs );
}
