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
package org.neo4j.driver.internal.summary;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.function.Function;
import org.neo4j.driver.Value;
import org.neo4j.driver.summary.Profile;

public class InternalProfile extends InternalPlan<Profile> implements Profile {

    private final OptionalLong dbHits;
    private final OptionalLong rows;
    private final OptionalLong pageCacheHits;
    private final OptionalLong pageCacheMisses;
    private final OptionalDouble pageCacheHitRatio;
    private final Optional<Duration> time;

    protected InternalProfile(
            String operatorType,
            Map<String, Value> arguments,
            List<String> identifiers,
            List<Profile> children,
            OptionalLong dbHits,
            OptionalLong rows,
            OptionalLong pageCacheHits,
            OptionalLong pageCacheMisses,
            OptionalDouble pageCacheHitRatio,
            Optional<Duration> time) {
        super(operatorType, arguments, identifiers, children);
        this.dbHits = dbHits;
        this.rows = rows;
        this.pageCacheHits = pageCacheHits;
        this.pageCacheMisses = pageCacheMisses;
        this.pageCacheHitRatio = pageCacheHitRatio;
        this.time = time;
    }

    @Override
    public OptionalLong dbHits() {
        return dbHits;
    }

    @Override
    public OptionalLong rows() {
        return rows;
    }

    @Override
    public OptionalLong pageCacheHits() {
        return pageCacheHits;
    }

    @Override
    public OptionalLong pageCacheMisses() {
        return pageCacheMisses;
    }

    @Override
    public OptionalDouble pageCacheHitRatio() {
        return pageCacheHitRatio;
    }

    @Override
    public Optional<Duration> time() {
        return time;
    }

    private static final PlanCreator<Profile> PROFILE =
            (operatorType, arguments, identifiers, children, originalPlanValue) -> new InternalProfile(
                    operatorType,
                    arguments,
                    identifiers,
                    children,
                    originalPlanValue
                            .get("dbHits")
                            .computeOrDefault(v -> OptionalLong.of(v.asLong()), OptionalLong.empty()),
                    originalPlanValue
                            .get("rows")
                            .computeOrDefault(v -> OptionalLong.of(v.asLong()), OptionalLong.empty()),
                    originalPlanValue
                            .get("pageCacheHits")
                            .computeOrDefault(v -> OptionalLong.of(v.asLong()), OptionalLong.empty()),
                    originalPlanValue
                            .get("pageCacheMisses")
                            .computeOrDefault(v -> OptionalLong.of(v.asLong()), OptionalLong.empty()),
                    originalPlanValue
                            .get("pageCacheHitRatio")
                            .computeOrDefault(v -> OptionalDouble.of(v.asDouble()), OptionalDouble.empty()),
                    originalPlanValue
                            .get("time")
                            .computeOrDefault(v -> Optional.of(Duration.ofNanos(v.asLong())), Optional.empty()));

    /**
     * Builds a regular plan without profiling information - eg. a plan that came as a result of an `EXPLAIN` query
     */
    public static final Function<Value, Profile> PROFILE_FROM_VALUE = new Converter<>(PROFILE);
}
