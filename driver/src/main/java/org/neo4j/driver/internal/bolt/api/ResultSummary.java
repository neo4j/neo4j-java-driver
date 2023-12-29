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
package org.neo4j.driver.internal.bolt.api;

/**
 * The result summary of running a query. The result summary interface can be used to
 * investigate details about the result, like the type of query run, how many and which
 * kinds of updates have been executed, and query plan and profiling information if
 * available.
 * <p>
 * The result summary is only available after all result records have been consumed.
 * <p>
 * Keeping the result summary around does not influence the lifecycle of any associated
 * session and/or transaction.
 *
 * @author Neo4j Drivers Team
 * @since 1.0.0
 */
public interface ResultSummary {

    /**
     * Returns counters for operations the query triggered.
     * @return counters for operations the query triggered
     */
    SummaryCounters counters();
}
