/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.driver;

/**
 * The result summary of running a statement. The result summary interface can be used to investigate
 * details about the result, like the type of query run, how many and which kinds of updates have been executed,
 * and query plan and profiling information if available.
 * <p>
 * The result summary is only available after all result records have been consumed.
 * <p>
 * Keeping the result summary around does not influence the lifecycle of any associated session and/or transaction.
 */
public interface ResultSummary
{
    /**
     * @return statement that has been executed
     */
    Statement statement();

    /**
     * @return update statistics for the statement
     */
    UpdateStatistics updateStatistics();

    /**
     * @return true if the result contained a statement plan, i.e. is the summary of a Cypher "PROFILE" or "EXPLAIN" statement
     */
    boolean hasPlan();

    /**
     * @return true if the result contained profiling information, i.e. is the summary of a Cypher "PROFILE" statement
     */
    boolean hasProfile();

    /**
     * @throws IllegalStateException if {@link #hasPlan()} is false
     * @return statement plan for the executed statement if available
     */
    Plan plan();

    /**
     * @return summary information on how the plan was constructed
     * @throws IllegalStateException if {@link #hasPlan()} is false
     */
    PlanningSummary planningSummary();

    /**
     * @throws IllegalStateException if {@link #hasProfile()} is false
     * @return profiled statement plan for the executed statement if available
     */
    ProfiledPlan profile();

//    /**
//     * @return summary information on how the profile was constructed
//     * @throws IllegalStateException if {@link #hasProfile()} is false
//     */
//    ProfileSummary profileSummary();
}
