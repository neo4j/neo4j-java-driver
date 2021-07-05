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

import java.util.Optional;

import org.neo4j.driver.exceptions.BackgroundJobUnfinishedException;
import org.neo4j.driver.summary.ResultSummary;

public interface BackgroundJob
{
    // ------------------- OPTION 1 -------------------

    /**
     * Check whether result summary is available.
     * <p>
     * This makes a request to the server that is executing the job to check its status.
     *
     * @return an {@link Optional} of {@link ResultSummary} when job is completed successfully or empty {@link Optional} if job is still in progress. An
     * exception is thrown is job has failed.
     */
    Optional<ResultSummary> checkSummary();

    // ------------------- OPTION 2 -------------------

    /**
     * Returns {@code true} if completed in any fashion: normally or exceptionally.
     * <p>
     * This makes a request to the server that is executing the job to check its status.
     *
     * @return {@code true} if completed, {@code false} otherwise
     */
    boolean checkCompleted();

    /**
     * Return the result summary.
     *
     * @return a summary for the whole query result.
     * @throws BackgroundJobUnfinishedException if called before {@link BackgroundJob#checkCompleted()} returns {@code true}
     */
    ResultSummary getSummary() throws BackgroundJobUnfinishedException;
}
