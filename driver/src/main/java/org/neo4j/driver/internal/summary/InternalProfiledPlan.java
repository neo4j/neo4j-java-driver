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
import org.neo4j.driver.Value;
import org.neo4j.driver.summary.Profile;
import org.neo4j.driver.summary.ProfiledPlan;

@SuppressWarnings("deprecation")
public class InternalProfiledPlan extends InternalPlan<ProfiledPlan> implements ProfiledPlan {
    private final long dbHits;
    private final long records;
    private final long pageCacheHits;
    private final long pageCacheMisses;
    private final double pageCacheHitRatio;
    private final long time;

    protected InternalProfiledPlan(
            String operatorType,
            Map<String, Value> arguments,
            List<String> identifiers,
            List<ProfiledPlan> children,
            long dbHits,
            long records,
            long pageCacheHits,
            long pageCacheMisses,
            double pageCacheHitRatio,
            long time) {
        super(operatorType, arguments, identifiers, children);
        this.dbHits = dbHits;
        this.records = records;
        this.pageCacheHits = pageCacheHits;
        this.pageCacheMisses = pageCacheMisses;
        this.pageCacheHitRatio = pageCacheHitRatio;
        this.time = time;
    }

    @Override
    public long dbHits() {
        return dbHits;
    }

    @Override
    public long records() {
        return records;
    }

    @Override
    public boolean hasPageCacheStats() {
        return pageCacheHits > 0 || pageCacheMisses > 0 || pageCacheHitRatio > 0;
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
    public long time() {
        return time;
    }

    protected static InternalProfiledPlan wrapProfile(Profile profile) {
        if (profile == null) {
            return null;
        }
        return new InternalProfiledPlan(
                profile.operatorType(),
                profile.arguments(),
                profile.identifiers(),
                profile.children().stream()
                        .map(child -> (ProfiledPlan) InternalProfiledPlan.wrapProfile(child))
                        .toList(),
                profile.dbHits().orElse(0L),
                profile.rows().orElse(0L),
                profile.pageCacheHits().orElse(0L),
                profile.pageCacheMisses().orElse(0L),
                profile.pageCacheHitRatio().orElse(0.0),
                profile.time().map(Duration::toNanos).orElse(0L));
    }
}
