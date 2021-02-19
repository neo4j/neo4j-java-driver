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
package org.neo4j.driver;

import java.util.concurrent.TimeUnit;

import org.neo4j.driver.util.Experimental;

/**
 * Provides connection pool metrics such as connection created, current in use etc.
 * The pool metrics is uniquely identified using {@link ConnectionPoolMetrics#id()}.
 */
@Experimental
public interface ConnectionPoolMetrics
{
    /**
     * An unique id that identifies this pool metrics.
     * @return An unique name
     */
    String id();

    /**
     * The amount of connections that are currently in-use (borrowed out of the pool). The amount can increase or decrease over time.
     * @return The amount of connections that are currently in-use
     */
    int inUse();

    /**
     * The amount of connections that are currently idle (buffered inside the pool). The amount can increase or decrease over time.
     * @return The amount of connections that are currently idle.
     */
    int idle();

    /**
     * The amount of connections that are currently in the process of being created.
     * The amount is increased by one when the pool noticed a request to create a new connection.
     * The amount is decreased by one when the pool noticed a new connection is created successfully or failed to create.
     * The amount can increase or decrease over time.
     * @return The amount of connections that are waiting to be created.
     */
    int creating();

    /**
     * A counter to record how many connections have been successfully created with this pool since the pool is created.
     * This number increases every time when a connection is successfully created.
     * @return The amount of connections have ever been created by this pool.
     */
    long created();

    /**
     * A counter to record how many connections that have failed to be created.
     * This number increases every time when a connection failed to be created.
     * @return The amount of connections have failed to be created by this pool.
     */
    long failedToCreate();

    /**
     * A counter to record how many connections have been closed by this pool.
     * This number increases every time when a connection is closed.
     * @return The amount of connections have been closed by this pool.
     */
    long closed();

    /**
     * The number of connection acquisition requests that are currently in progress.
     * These requests can be waiting or blocked if there are no connections immediately available in the pool.
     * A request will wait for a new connection to be created, or it will be blocked if the pool is at its maximum size but all connections are already in use.
     * The amount can increase or decrease over time.
     * @return The number of connection acquisition requests that are currently in progress.
     */
    int acquiring();

    /**
     * A counter to record how many connections have been acquired from the pool since the pool is created.
     * This number increases every time when a connection is acquired.
     * @return The amount of connections that have been acquired from the pool.
     */
    long acquired();

    /**
     * A counter to record how many times that we've failed to acquire a connection from the pool within configured maximum acquisition timeout
     * set by {@link Config.ConfigBuilder#withConnectionAcquisitionTimeout(long, TimeUnit)}.
     * This number increases every time when a connection is timed out when acquiring.
     * @return The amount of failures to acquire a connection from the pool within maximum connection acquisition timeout.
     */
    long timedOutToAcquire();

    /**
     * A counter to record the total acquisition time in milliseconds of all connection acquisition requests since the pool is created.
     * This number increases every time when a connection is acquired.
     * See {@link ConnectionPoolMetrics#acquired()} for the total amount of connection acquired since the driver is created.
     * The average acquisition time can be calculated using the code below:
     * <h2>Example</h2>
     * <pre>
     * {@code
     * ConnectionPoolMetrics previous, current;
     * ...
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
     * A counter to record the total time in milliseconds spent to establishing new socket connections since the pool is created.
     * This number increases every time when a connection is established.
     * See {@link ConnectionPoolMetrics#created()} for the total amount of connections established since the pool is created.
     * The average connection time can be calculated using the code below:
     * <h2>Example</h2>
     * <pre>
     * {@code
     * ConnectionPoolMetrics previous, current;
     * ...
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
     * A counter to record the total time in milliseconds connections are borrowed out of the pool,
     * such as the time spent in user's application code to run cypher queries.
     * This number increases every time when a connection is returned back to the pool.
     * See {@link ConnectionPoolMetrics#totalInUseCount()} for the total amount of connections that are borrowed out of the pool.
     * The average in-use time can be calculated using the code below:
     * <h2>Example</h2>
     * <pre>
     * {@code
     * ConnectionPoolMetrics previous, current;
     * ...
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
     * This number increases every time when a connection is returned back to the pool.
     * @return the total amount of connection that are borrowed outside the pool.
     */
    long totalInUseCount();
}
