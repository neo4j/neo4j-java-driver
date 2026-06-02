/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.neo4j.driver.Query;
import org.neo4j.driver.util.Immutable;

/**
 * The result summary of running a query. The result summary interface can be used to investigate
 * details about the result, like the type of query run, how many and which kinds of updates have been executed,
 * and query plan and profiling information if available.
 * <p>
 * The result summary is only available after all result records have been consumed.
 * <p>
 * Keeping the result summary around does not influence the lifecycle of any associated session and/or transaction.
 *
 * @since 1.0
 */
@Immutable
public interface ResultSummary {
    /**
     * Returns the query that has been executed.
     *
     * @return the query that has been executed
     */
    Query query();

    /**
     * Returns the counters for operations the query triggered.
     *
     * @return the counters for operations the query triggered
     */
    SummaryCounters counters();

    /**
     * Returns the type of query that has been executed.
     *
     * @return the type of query that has been executed
     */
    QueryType queryType();

    /**
     * Returns {@code true} if the result contained a query plan, i.e. is the summary of a Cypher "PROFILE" or "EXPLAIN" query.
     *
     * @return {@code true} if the result contained a query plan, i.e. is the summary of a Cypher "PROFILE" or "EXPLAIN" query
     * @deprecated use {@link ResultSummary#queryPlan()}{@link Optional#isPresent() .isPresent()} instead.
     */
    @Deprecated
    boolean hasPlan();

    /**
     * Returns {@code true} if the result contained profiling information, i.e. is the summary of a Cypher "PROFILE" query.
     *
     * @return {@code true} if the result contained profiling information, i.e. is the summary of a Cypher "PROFILE" query
     * @deprecated use {@link ResultSummary#queryProfile()}{@link Optional#isPresent() .isPresent()} instead.
     */
    @Deprecated
    boolean hasProfile();

    /**
     * This describes how the database will execute your query.
     *
     * @return query plan for the executed query if available, otherwise null
     * @deprecated superseded by {@link ResultSummary#queryPlan()}.
     */
    @Deprecated
    Plan plan();

    /**
     * This describes how the database did execute your query.
     * <p>
     * If the query you executed {@link #hasProfile() was profiled}, the query plan will contain detailed
     * information about what each step of the plan did. That more in-depth version of the query plan becomes
     * available here.
     *
     * @return profiled query plan for the executed query if available, otherwise null
     * @deprecated superseded by {@link ResultSummary#queryProfile()}
     */
    @Deprecated
    ProfiledPlan profile();

    /**
     * This describes how the database will execute your query.
     *
     * @return query plan for the executed query if available
     * @since 6.2.0
     */
    default Optional<Plan> queryPlan() {
        return Optional.empty();
    }

    /**
     * This describes how the database did execute your query.
     * <p>
     * If the query you executed was profiled, the query plan will contain detailed information about
     * what each step of the plan did. That more in-depth version of the query plan becomes available here.
     *
     * @return profiled query plan for the executed query if available
     * @since 6.2.0
     */
    default Optional<Profile> queryProfile() {
        return Optional.empty();
    }

    /**
     * A list of notifications that might arise when executing the query.
     * Notifications can be warnings about problematic queries or other valuable information that can be presented
     * in a client.
     * <p>
     * Unlike failures or errors, notifications do not affect the execution of a query.
     *
     * @return a list of notifications produced while executing the query. The list will be empty if no
     * notifications produced while executing the query.
     * @deprecated superseded by {@link ResultSummary#gqlStatusObjects()}.
     */
    @Deprecated
    List<Notification> notifications();

    /**
     * Returns a sequenced set of GQL-status objects resulting from the request execution.
     *
     * @return the sequenced set of GQL-status objects
     * @since 5.22.0
     */
    Set<GqlStatusObject> gqlStatusObjects();

    /**
     * The time it took the server to make the result available for consumption.
     *
     * @param unit The unit of the duration.
     * @return The time it took for the server to have the result available in the provided time unit.
     */
    long resultAvailableAfter(TimeUnit unit);

    /**
     * The time it took the server to consume the result.
     *
     * @param unit The unit of the duration.
     * @return The time it took for the server to consume the result in the provided time unit.
     */
    long resultConsumedAfter(TimeUnit unit);

    /**
     * The basic information of the server where the result is obtained from
     *
     * @return basic information of the server where the result is obtained from
     */
    ServerInfo server();

    /**
     * The basic information of the database where the result is obtained from
     *
     * @return the basic information of the database where the result is obtained from
     */
    DatabaseInfo database();
}
