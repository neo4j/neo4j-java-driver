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
     * The status of the pool
     * @return The status of the pool.
     */
    PoolStatus poolStatus();

    /**
     * The amount of channels that are in-use (borrowed out of the pool).
     * The number is changing up and down from time to time.
     * @return The amount of channels that are in-use
     */
    int inUse();

    /**
     * The amount of channels that are idle (buffered inside the pool).
     * The number is changing up and down from time to time.
     * @return The amount of channels that are idle.
     */
    int idle();

    /**
     * The amount of channels that are waiting to be created.
     * The amount is increased by one when the pool noticed a request to create a new connection.
     * The amount is decreased by one when the pool noticed a new connection is created regardless successfully or not.
     * The number is changing up and down from time to time.
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
     * An increasing-only number to record how many connections have been acquired from the pool.
     * @return The amount of connections that have been acquired from the pool.
     */
    long acquired();

    /**
     * An increasing-only number to record how many times that we've failed to acquire a connection from the pool within configured maximum acquisition timeout
     * set by {@link Config.ConfigBuilder#withConnectionAcquisitionTimeout(long, TimeUnit)}.
     * @return The amount of failures to acquire a connection from the pool within maximum connection acquisition timeout.
     */
    long timedOutToAcquire();

    /**
     * An acquisition time histogram records how long it takes to acquire an connection from this pool.
     * The connection acquired from the pool could either be a connection idling inside the pool or a connection created by the pool.
     * @return The acquisition time histogram.
     */
    Histogram acquisitionTimeHistogram();
}
