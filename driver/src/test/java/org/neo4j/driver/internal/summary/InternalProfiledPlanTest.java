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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.internal.value.FloatValue;
import org.neo4j.driver.internal.value.IntegerValue;
import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.driver.internal.value.MapValue;
import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.driver.Value;
import org.neo4j.driver.summary.ProfiledPlan;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InternalProfiledPlanTest
{

    @Test
    void shouldHandlePlanWithNoChildren()
    {
        // GIVEN
        Value value = new MapValue( createPlanMap() );

        // WHEN
        ProfiledPlan plan = InternalProfiledPlan.PROFILED_PLAN_FROM_VALUE.apply( value );

        // THEN
        verifyPlan( plan );
    }

    @Test
    void shouldHandlePlanWithChildren()
    {
        // GIVEN
        Map<String,Value> planMap = createPlanMap();
        planMap.put( "children", new ListValue( new MapValue( createPlanMap() ), new MapValue( createPlanMap() ) ) );
        Value value = new MapValue( planMap );

        // WHEN
        ProfiledPlan plan = InternalProfiledPlan.PROFILED_PLAN_FROM_VALUE.apply( value );

        // THEN
        for ( ProfiledPlan child : plan.children() )
        {
            verifyPlan( child );
        }
    }

    private Map<String,Value> createPlanMap()
    {
        Map<String,Value> map = new HashMap<>();
        map.put( "operatorType", new StringValue( "AwesomeOperator" ) );
        map.put( "rows", new IntegerValue( 1337L ) );
        map.put( "dbHits", new IntegerValue( 42 ) );
        map.put( "pageCacheHits", new IntegerValue( 1234 ) );
        map.put( "pageCacheMisses", new IntegerValue( 3456 ) );
        map.put( "pageCacheHitRatio", new FloatValue( 0.123 ) );
        map.put( "time", new IntegerValue( 999 ) );
        map.put( "identifiers", new ListValue( new StringValue( "n1" ), new StringValue( "n2" ) ) );
        Map<String,Value> args = new HashMap<>();
        args.put( "version", new StringValue( "CYPHER 1337" ) );
        map.put( "args", new MapValue( args ) );
        return map;
    }

    private void verifyPlan( ProfiledPlan plan )
    {
        assertThat( plan.dbHits(), equalTo( 42L ) );
        assertThat( plan.records(), equalTo( 1337L ) );
        assertTrue( plan.hasPageCacheStats() );
        assertThat( plan.pageCacheHits(), equalTo( 1234L ) );
        assertThat( plan.pageCacheMisses(), equalTo( 3456L ) );
        assertThat( plan.pageCacheHitRatio(), equalTo( 0.123 ) );
        assertThat( plan.time(), equalTo( 999L ) );
        assertThat( plan.operatorType(), equalTo( "AwesomeOperator" ) );
        assertThat( plan.identifiers(), equalTo( asList( "n1", "n2" ) ) );
        assertThat( plan.arguments().values(), hasItem( new StringValue( "CYPHER 1337" ) ) );
        assertThat( plan.children(), empty() );
    }
}
