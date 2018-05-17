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

import java.util.concurrent.TimeUnit;

import org.neo4j.driver.v1.Config;

public interface ConnectionPoolMetrics
{
    /**
     * An unique name that identifies this connection pool metrics among all others
     * @return An unique name
     */
    String uniqueName();

    /**
     * The status of the pool.
     * @return The status of the pool.
     */
    PoolStatus poolStatus();

    /**
     * The amount of channels that are currently in-use (borrowed out of the pool).
     * @return The amount of channels that are currently in-use
     */
    int inUse();

    /**
     * The amount of channels that are currently idle (buffered inside the pool).
     * @return The amount of channels that are currently idle.
     */
    int idle();

    /**
     * The amount of channels that are currently waiting to be created.
     * The amount is increased by one when the pool noticed a request to create a new channel.
     * The amount is decreased by one when the pool noticed a new channel is created successfully or failed to create.
     * @return The amount of channels that are waiting to be created.
     */
    int creating();

    /**
     * An increasing-only number to record how many channels have been created by this pool successfully.
     * @return The amount of channels have ever been created by this pool.
     */
    long created();

    /**
     * An increasing-only number to record how many channels have been failed to create.
     * @return The amount of channels have been failed to create by this pool.
     */
    long failedToCreate();

    /**
     * An increasing-only number to record how many channels have been closed by this pool.
     * @return The amount of channels have been closed by this pool.
     */
    long closed();

    /**
     * The current count of application requests to wait for acquiring a connection from the pool.
     * The reason to wait could be waiting for creating a new channel, or waiting for a channel to be free by application when the pool is full.
     * @return The current amount of application request to wait for acquiring a connection from the pool.
     */
    int acquiring();

    /**
     * An increasing-only number to record how many connections have been acquired from the pool
     * The connections acquired could hold either a newly created channel or a reused channel from the pool.
     * @return The amount of connections that have been acquired from the pool.
     */
    long acquired();

    /**
     * An increasing-only number to record how many times that we've failed to acquire a connection from the pool within configured maximum acquisition timeout
     * set by {@link Config.ConfigBuilder#withConnectionAcquisitionTimeout(long, TimeUnit)}.
     * The connection acquired could hold either a newly created channel or a reused channel from the pool.
     * @return The amount of failures to acquire a connection from the pool within maximum connection acquisition timeout.
     */
    long timedOutToAcquire();

    /**
     * An acquisition time histogram records how long it takes to acquire an connection from this pool.
     * The connection acquired from the pool could contain either a channel idling inside the pool or a channel created by the pool.
     * @return The acquisition time histogram.
     */
    Histogram acquisitionTimeHistogram();
}
