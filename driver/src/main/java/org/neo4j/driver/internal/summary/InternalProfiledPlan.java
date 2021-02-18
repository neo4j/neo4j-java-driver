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
package org.neo4j.driver.internal.summary;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.driver.Value;
import org.neo4j.driver.summary.ProfiledPlan;

public class InternalProfiledPlan extends InternalPlan<ProfiledPlan> implements ProfiledPlan
{
    private final long dbHits;
    private final long records;
    private final long pageCacheHits;
    private final long pageCacheMisses;
    private final double pageCacheHitRatio;
    private final long time;

    protected InternalProfiledPlan( String operatorType, Map<String,Value> arguments, List<String> identifiers, List<ProfiledPlan> children, long dbHits,
            long records, long pageCacheHits, long pageCacheMisses, double pageCacheHitRatio, long time )
    {
        super( operatorType, arguments, identifiers, children );
        this.dbHits = dbHits;
        this.records = records;
        this.pageCacheHits = pageCacheHits;
        this.pageCacheMisses = pageCacheMisses;
        this.pageCacheHitRatio = pageCacheHitRatio;
        this.time = time;
    }

    @Override
    public long dbHits()
    {
        return dbHits;
    }

    @Override
    public long records()
    {
        return records;
    }

    @Override
    public boolean hasPageCacheStats()
    {
        return pageCacheHits > 0 || pageCacheMisses > 0 || pageCacheHitRatio > 0;
    }

    @Override
    public long pageCacheHits()
    {
        return pageCacheHits;
    }

    @Override
    public long pageCacheMisses()
    {
        return pageCacheMisses;
    }

    @Override
    public double pageCacheHitRatio()
    {
        return pageCacheHitRatio;
    }

    @Override
    public long time()
    {
        return time;
    }

    public static final PlanCreator<ProfiledPlan> PROFILED_PLAN = new PlanCreator<ProfiledPlan>()
    {
        @Override
        public ProfiledPlan create( String operatorType, Map<String,Value> arguments, List<String> identifiers, List<ProfiledPlan> children,
                Value originalPlanValue )
        {
            return new InternalProfiledPlan( operatorType, arguments, identifiers, children, originalPlanValue.get( "dbHits" ).asLong( 0 ),
                    originalPlanValue.get( "rows" ).asLong( 0 ), originalPlanValue.get( "pageCacheHits" ).asLong( 0 ),
                    originalPlanValue.get( "pageCacheMisses" ).asLong( 0 ), originalPlanValue.get( "pageCacheHitRatio" ).asDouble( 0 ),
                    originalPlanValue.get( "time" ).asLong( 0 ) );
        }
    };

    /**
     * Builds a regular plan without profiling information - eg. a plan that came as a result of an `EXPLAIN` query
     */
    public static final Function<Value,ProfiledPlan> PROFILED_PLAN_FROM_VALUE = new Converter<>( PROFILED_PLAN );
}
