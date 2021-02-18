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
package org.neo4j.driver.summary;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.Query;
import org.neo4j.driver.util.Immutable;

/**
 * The result summary of running a query. The result summary interface can be used to investigate
 * details about the result, like the type of query run, how many and which kinds of updates have been executed,
 * and query plan and profiling information if available.
 *
 * The result summary is only available after all result records have been consumed.
 *
 * Keeping the result summary around does not influence the lifecycle of any associated session and/or transaction.
 * @since 1.0
 */
@Immutable
public interface ResultSummary
{
    /**
     * @return query that has been executed
     */
    Query query();

    /**
     * @return counters for operations the query triggered
     */
    SummaryCounters counters();

    /**
     * @return type of query that has been executed
     */
    QueryType queryType();

    /**
     * @return true if the result contained a query plan, i.e. is the summary of a Cypher "PROFILE" or "EXPLAIN" query
     */
    boolean hasPlan();

    /**
     * @return true if the result contained profiling information, i.e. is the summary of a Cypher "PROFILE" query
     */
    boolean hasProfile();

    /**
     * This describes how the database will execute your query.
     *
     * @return query plan for the executed query if available, otherwise null
     */
    Plan plan();

    /**
     * This describes how the database did execute your query.
     *
     * If the query you executed {@link #hasProfile() was profiled}, the query plan will contain detailed
     * information about what each step of the plan did. That more in-depth version of the query plan becomes
     * available here.
     *
     * @return profiled query plan for the executed query if available, otherwise null
     */
    ProfiledPlan profile();

    /**
     * A list of notifications that might arise when executing the query.
     * Notifications can be warnings about problematic queries or other valuable information that can be presented
     * in a client.
     *
     * Unlike failures or errors, notifications do not affect the execution of a query.
     *
     * @return a list of notifications produced while executing the query. The list will be empty if no
     * notifications produced while executing the query.
     */
    List<Notification> notifications();

    /**
     * The time it took the server to make the result available for consumption.
     *
     * @param unit The unit of the duration.
     * @return The time it took for the server to have the result available in the provided time unit.
     */
    long resultAvailableAfter( TimeUnit unit );

    /**
     * The time it took the server to consume the result.
     *
     * @param unit The unit of the duration.
     * @return The time it took for the server to consume the result in the provided time unit.
     */
    long resultConsumedAfter( TimeUnit unit );

    /**
     * The basic information of the server where the result is obtained from
     * @return basic information of the server where the result is obtained from
     */
    ServerInfo server();

    /**
     * The basic information of the database where the result is obtained from
     * @return the basic information of the database where the result is obtained from
     */
    DatabaseInfo database();
}
