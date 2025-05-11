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

/**
 * This is the same as a regular {@link Plan} - except this plan has been executed, meaning it also contains detailed information about how much work each
 * step of the plan incurred on the database.
 * @since 1.0
 */
public interface ProfiledPlan extends Plan {
    /**
     * Returns the number of times this part of the plan touched the underlying data stores.
     *
     * @return the number of times this part of the plan touched the underlying data stores
     */
    long dbHits();

    /**
     * Returns the number of records this part of the plan produced.
     *
     * @return the number of records this part of the plan produced
     */
    long records();

    /**
     * Returns whether the number page cache hits and misses and the ratio was recorded.
     *
     * @return if the number page cache hits and misses and the ratio was recorded
     */
    boolean hasPageCacheStats();

    /**
     * Returns the number of page cache hits caused by executing the associated execution step.
     *
     * @return the number of page cache hits caused by executing the associated execution step
     */
    long pageCacheHits();

    /**
     * Returns the number of page cache misses caused by executing the associated execution step.
     *
     * @return the number of page cache misses caused by executing the associated execution step
     */
    long pageCacheMisses();

    /**
     * Returns the ratio of page cache hits to total number of lookups or 0 if no data is available.
     *
     * @return the ratio of page cache hits to total number of lookups or 0 if no data is available
     */
    double pageCacheHitRatio();

    /**
     * Returns the amount of time spent in the associated execution step.
     *
     * @return the amount of time spent in the associated execution step
     */
    long time();

    @Override
    List<ProfiledPlan> children();
}
