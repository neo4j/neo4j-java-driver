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
package org.neo4j.driver.v1.internal.summary;

import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.Function;
import org.neo4j.driver.v1.ProfiledPlan;
import org.neo4j.driver.v1.Value;

public class SimpleProfiledPlan extends SimplePlan<ProfiledPlan> implements ProfiledPlan
{
    private final long dbHits;
    private final long records;

    protected SimpleProfiledPlan( String operatorType, Map<String,Value> arguments,
            List<String> identifiers, List<ProfiledPlan> children, long dbHits, long records )
    {
        super( operatorType, arguments, identifiers, children );
        this.dbHits = dbHits;
        this.records = records;
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

    public static final PlanCreator<ProfiledPlan> PROFILED_PLAN = new PlanCreator<ProfiledPlan>()
    {
        @Override
        public ProfiledPlan create( String operatorType, Map<String,Value> arguments, List<String> identifiers, List<ProfiledPlan> children, Value originalPlanValue )
        {
            return new SimpleProfiledPlan( operatorType, arguments, identifiers, children,
                    originalPlanValue.get( "dbHits" ).javaLong(),
                    originalPlanValue.get( "rows" ).javaLong() );
        }
    };

    /** Builds a regular plan without profiling information - eg. a plan that came as a result of an `EXPLAIN` statement */
    public static final Function<Value, ProfiledPlan> PROFILED_PLAN_FROM_VALUE = new Converter<>(PROFILED_PLAN);
}
