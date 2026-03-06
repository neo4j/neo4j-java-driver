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

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;
import org.neo4j.driver.Value;
import org.neo4j.driver.summary.ProfiledPlan;

public class InternalProfiledPlan extends InternalPlan<ProfiledPlan> implements ProfiledPlan {

    private final boolean hasDbHits;
    private final long dbHits;
    private final boolean hasRecords;
    private final long records;
    private final boolean hasPageCacheStats;
    private final long pageCacheHits;
    private final long pageCacheMisses;
    private final double pageCacheHitRatio;
    private final boolean hasTime;
    private final long time;

    protected InternalProfiledPlan(
            String operatorType,
            Map<String, Value> arguments,
            List<String> identifiers,
            List<ProfiledPlan> children,
            boolean hasDbHits,
            long dbHits,
            boolean hasRecords,
            long records,
            boolean hasPageCacheStats,
            long pageCacheHits,
            long pageCacheMisses,
            double pageCacheHitRatio,
            boolean hasTime,
            long time) {
        super(operatorType, arguments, identifiers, children);
        this.hasDbHits = hasDbHits;
        this.dbHits = dbHits;
        this.hasRecords = hasRecords;
        this.records = records;
        this.hasPageCacheStats = hasPageCacheStats;
        this.pageCacheHits = pageCacheHits;
        this.pageCacheMisses = pageCacheMisses;
        this.pageCacheHitRatio = pageCacheHitRatio;
        this.hasTime = hasTime;
        this.time = time;
    }

    @Override
    public boolean hasDbHits() {
        return hasDbHits;
    }

    @Override
    public long dbHits() {
        return dbHits;
    }

    @Override
    public boolean hasRecords() {
        return hasRecords;
    }

    @Override
    public long records() {
        return records;
    }

    @Override
    public boolean hasPageCacheStats() {
        return hasPageCacheStats;
    }

    @Override
    public long pageCacheHits() {
        return pageCacheHits;
    }

    @Override
    public long pageCacheMisses() {
        return pageCacheMisses;
    }

    @Override
    public double pageCacheHitRatio() {
        return pageCacheHitRatio;
    }

    @Override
    public boolean hasTime() {
        return hasTime;
    }

    @Override
    public long time() {
        return time;
    }

    private static final PlanCreator<ProfiledPlan> PROFILED_PLAN =
            (operatorType, arguments, identifiers, children, originalPlanValue) -> new InternalProfiledPlan(
                    operatorType,
                    arguments,
                    identifiers,
                    children,
                    originalPlanValue.containsKey("dbHits"),
                    originalPlanValue.get("dbHits").asLong(0),
                    originalPlanValue.containsKey("rows"),
                    originalPlanValue.get("rows").asLong(0),
                    Stream.of("pageCacheHits", "pageCacheMisses", "pageCacheHitRatio")
                            .anyMatch(originalPlanValue::containsKey),
                    originalPlanValue.get("pageCacheHits").asLong(0),
                    originalPlanValue.get("pageCacheMisses").asLong(0),
                    originalPlanValue.get("pageCacheHitRatio").asDouble(0),
                    originalPlanValue.containsKey("time"),
                    originalPlanValue.get("time").asLong(0));

    /**
     * Builds a regular plan without profiling information - eg. a plan that came as a result of an `EXPLAIN` query
     */
    public static final Function<Value, ProfiledPlan> PROFILED_PLAN_FROM_VALUE = new Converter<>(PROFILED_PLAN);
}
