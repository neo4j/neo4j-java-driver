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
package org.neo4j.driver;

import java.util.concurrent.TimeUnit;

public interface ConnectionPoolMetrics
{
    enum PoolStatus
    {
        OPEN, CLOSED
    }

    /**
     * An unique name that identifies this connection pool metrics among all others
     * @return An unique name
     */
    String id();

    /**
     * The status of the pool.
     * @return The status of the pool.
     */
    PoolStatus poolStatus();

    /**
     * The amount of connections that are currently in-use (borrowed out of the pool).
     * @return The amount of connections that are currently in-use
     */
    int inUse();

    /**
     * The amount of connections that are currently idle (buffered inside the pool).
     * @return The amount of connections that are currently idle.
     */
    int idle();

    /**
     * The amount of connections that are currently waiting to be created.
     * The amount is increased by one when the pool noticed a request to create a new connection.
     * The amount is decreased by one when the pool noticed a new connection is created successfully or failed to create.
     * @return The amount of connections that are waiting to be created.
     */
    int creating();

    /**
     * An increasing-only number to record how many connections have been created by this pool successfully since the pool is created.
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
     * The current count of application requests to wait for acquiring a connection from the pool.
     * The reason to wait could be waiting for creating a new connection, or waiting for a connection to be free by application when the pool is full.
     * @return The current amount of application request to wait for acquiring a connection from the pool.
     */
    int acquiring();

    /**
     * An increasing-only number to record how many connections have been acquired from the pool since the pool is created.
     * The connections acquired could hold either a newly created connection or a reused connection from the pool.
     * @return The amount of connections that have been acquired from the pool.
     */
    long acquired();

    /**
     * An increasing-only number to record how many times that we've failed to acquire a connection from the pool within configured maximum acquisition timeout
     * set by {@link Config.ConfigBuilder#withConnectionAcquisitionTimeout(long, TimeUnit)}.
     * The connection acquired could hold either a newly created connection or a reused connection from the pool.
     * @return The amount of failures to acquire a connection from the pool within maximum connection acquisition timeout.
     */
    long timedOutToAcquire();

    /**
     * The total acquisition time in milliseconds of all connection acquisition requests since the pool is created.
     * See {@link ConnectionPoolMetrics#acquired()} for the total amount of connection acquired since the driver is created.
     * The average acquisition time can be calculated using the code bellow:
     * <h2>Example</h2>
     * <pre>
     * {@code
     * ConnectionPoolMetrics previous = ConnectionPoolMetrics.EMPTY;
     * ...
     * ConnectionPoolMetrics current = poolMetrics.snapshot();
     * double average = computeAverage(current.totalAcquisitionTime(), previous.totalAcquisitionTime(), current.acquired(), previous.acquired());
     * previous = current;
     * ...
     *
     * private static double computeAverage(double currentSum, double previousSum, double currentCount, double previousCount)
     * {
     *     return (currentSum-previousSum)/(currentCount-previousCount);
     * }
     * }
     * </pre>
     * @return The total acquisition time since the driver is created.
     */
    long totalAcquisitionTime();

    /**
     * The total time in milliseconds spent to establishing new socket connections since the pool is created.
     * See {@link ConnectionPoolMetrics#created()} for the total amount of connections established since the pool is created.
     * The average connection time can be calculated using the code bellow:
     * <h2>Example</h2>
     * <pre>
     * {@code
     * ConnectionPoolMetrics previous = ConnectionPoolMetrics.EMPTY;
     * ...
     * ConnectionPoolMetrics current = poolMetrics.snapshot();
     * double average = computeAverage(current.totalConnectionTime(), previous.totalConnectionTime(), current.created(), previous.created());
     * previous = current;
     * ...
     *
     * private static double computeAverage(double currentSum, double previousSum, double currentCount, double previousCount)
     * {
     *     return (currentSum-previousSum)/(currentCount-previousCount);
     * }
     * }
     * </pre>
     * @return The total connection time since the driver is created.
     */
    long totalConnectionTime();

    /**
     * The total time in milliseconds connections are borrowed out of the pool, such as the time spent in user's application code to run cypher queries.
     * See {@link ConnectionPoolMetrics#totalInUseCount()} for the total amount of connections that are borrowed out of the pool.
     * The average in-use time can be calculated using the code bellow:
     * <h2>Example</h2>
     * <pre>
     * {@code
     * ConnectionPoolMetrics previous = ConnectionPoolMetrics.EMPTY;
     * ...
     * ConnectionPoolMetrics current = poolMetrics.snapshot();
     * double average = computeAverage(current.totalInUseTime(), previous.totalInUseTime(), current.totalInUseCount(), previous.totalInUseCount());
     * previous = current;
     * ...
     *
     * private static double computeAverage(double currentSum, double previousSum, double currentCount, double previousCount)
     * {
     *     return (currentSum-previousSum)/(currentCount-previousCount);
     * }
     * }
     * </pre>
     * @return the total time connections are used outside the pool.
     */
    long totalInUseTime();

    /**
     * The total amount of connections that are borrowed outside the pool since the pool is created.
     * @return the total amount of connection that are borrowed outside the pool.
     */
    long totalInUseCount();

    /**
     * Returns a snapshot of this connection pool metrics.
     * @return a snapshot of this connection pool metrics.
     */
    ConnectionPoolMetrics snapshot();
}
