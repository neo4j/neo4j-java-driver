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

public interface ConnectionPoolMetrics
{
    /**
     * An unique name that identifies this connection pool metrics among all others
     * @return An unique name
     */
    String uniqueName();

    /**
     * The status of the pool
     * @return The status of the pool in a string
     */
    String poolStatus();

    /**
     * The amount of connections that is in-use (borrowed out of the pool).
     * The number is changing up and down from time to time.
     * @return The amount of connections that is in-use
     */
    int inUse();

    /**
     * The amount of connections that is idle (buffered inside the pool).
     * The number is changing up and down from time to time.
     * @return The amount of connections that is idle.
     */
    int idle();

    /**
     * The amount of connections that is going to be created.
     * The amount is increased by one when the pool noticed a request to create a new connection.
     * The amount is decreased by one when the pool noticed a new connection is created regardless successfully or not.
     * The number is changing up and down from time to time.
     * @return The amount of connection that is to be created.
     */
    int toCreate();

    /**
     * An increasing-only number to record how many connections have been created by this pool successfully.
     * @return The amount of connections have ever been created by this pool.
     */
    long created();

    /**
     * An increasing-only number to record how many connections have been failed to create.
     * @return The amount of connections have been failed to create by this pool.
     */
    long failedToCreate();

    /**
     * An increasing-only number to record how many connections have been closed by this pool.
     * @return The amount of connections have been closed by this pool.
     */
    long closed();

    /**
     * An acquisition time histogram records how long it takes to acquire an connection from this pool.
     * The connection acquired from the pool could either be a connection idling inside the pool or a connection created by the pool.
     * @return The acquisition time histogram.
     */
    Histogram acquisitionTimeHistogram();
}
