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

    private final Long dbHits;
    private final Long rows;
    private final Long pageCacheHits;
    private final Long pageCacheMisses;
    private final Double pageCacheHitRatio;
    private final Duration time;

    protected InternalProfile(
            String operatorType,
            Map<String, Value> arguments,
            List<String> identifiers,
            List<Profile> children,
            Long dbHits,
            Long rows,
            Long pageCacheHits,
            Long pageCacheMisses,
            Double pageCacheHitRatio,
            Duration time) {
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
        return dbHits == null ? OptionalLong.empty() : OptionalLong.of(dbHits);
    }

    @Override
    public OptionalLong rows() {
        return rows == null ? OptionalLong.empty() : OptionalLong.of(rows);
    }

    @Override
    public OptionalLong pageCacheHits() {
        return pageCacheHits == null ? OptionalLong.empty() : OptionalLong.of(pageCacheHits);
    }

    @Override
    public OptionalLong pageCacheMisses() {
        return pageCacheMisses == null ? OptionalLong.empty() : OptionalLong.of(pageCacheMisses);
    }

    @Override
    public OptionalDouble pageCacheHitRatio() {
        return pageCacheHitRatio == null ? OptionalDouble.empty() : OptionalDouble.of(pageCacheHitRatio);
    }

    @Override
    public Optional<Duration> time() {
        return Optional.ofNullable(time);
    }

    private static final PlanCreator<Profile> PROFILE =
            (operatorType, arguments, identifiers, children, originalPlanValue) -> new InternalProfile(
                    operatorType,
                    arguments,
                    identifiers,
                    children,
                    originalPlanValue.get("dbHits").computeOrDefault(Value::asLong, null),
                    originalPlanValue.get("rows").computeOrDefault(Value::asLong, null),
                    originalPlanValue.get("pageCacheHits").computeOrDefault(Value::asLong, null),
                    originalPlanValue.get("pageCacheMisses").computeOrDefault(Value::asLong, null),
                    originalPlanValue.get("pageCacheHitRatio").computeOrDefault(Value::asDouble, null),
                    originalPlanValue.get("time").computeOrDefault(v -> Duration.ofNanos(v.asLong()), null));

    /**
     * Builds a regular plan without profiling information - eg. a plan that came as a result of an `EXPLAIN` query
     */
    public static final Function<Value, Profile> PROFILE_FROM_VALUE = new Converter<>(PROFILE);
}
