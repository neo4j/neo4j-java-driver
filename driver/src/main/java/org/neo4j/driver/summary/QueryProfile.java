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

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import org.neo4j.driver.internal.summary.InternalQueryProfile;

/**
 * This is the same as a regular {@link Plan} - except this plan has been executed, meaning it also contains detailed information about how much work each
 * step of the plan incurred on the database.
 * @since 6.2.0
 */
public sealed interface QueryProfile extends Plan permits InternalQueryProfile {

    /**
     * Returns the number of times this part of the plan touched the underlying data stores if it was recorded.
     *
     * @return the number of times this part of the plan touched the underlying data stores if it was recorded
     */
    OptionalLong dbHits();

    /**
     * Returns the number of rows processed by the associated execution step if it was recorded.
     *
     * @return the number of rows processed by the associated execution step if it was recorded
     */
    OptionalLong rows();

    /**
     * Returns the number of page cache hits caused by executing the associated execution step if it was recorded.
     *
     * @return the number of page cache hits caused by executing the associated execution step if it was recorded
     */
    OptionalLong pageCacheHits();

    /**
     * Returns the number of page cache misses caused by executing the associated execution step if it was recorded.
     *
     * @return the number of page cache misses caused by executing the associated execution step if it was recorded
     */
    OptionalLong pageCacheMisses();

    /**
     * Returns the ratio of page cache hits to total number of lookups if it was recorded.
     *
     * @return the ratio of page cache hits to total number of lookups if it was recorded
     */
    OptionalDouble pageCacheHitRatio();

    /**
     * Returns the amount of time spent in the associated execution step if it was recorded.
     *
     * @return the amount of time spent in the associated execution step if it was recorded
     */
    Optional<Duration> time();

    @Override
    List<? extends QueryProfile> children();
}
