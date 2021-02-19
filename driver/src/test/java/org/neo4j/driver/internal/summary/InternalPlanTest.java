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

import java.util.Collections;
import java.util.List;

import org.neo4j.driver.Value;
import org.neo4j.driver.summary.Plan;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.neo4j.driver.Values.ofValue;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.Values.values;

class InternalPlanTest
{
    @Test
    void shouldConvertFromEmptyMapValue()
    {
        // Given
        Value value = value( parameters( "operatorType", "X" ) );

        // When
        Plan plan = InternalPlan.EXPLAIN_PLAN_FROM_VALUE.apply( value );

        // Then
        assertThat( plan.operatorType(), equalTo( "X") );
        assertThat( plan.arguments(), equalTo( parameters().asMap( ofValue()) ) );
        assertThat( plan.identifiers(), equalTo( Collections.emptyList() ) );
        assertThat( plan.children(), equalTo( Collections.emptyList() ) );
    }

    @Test
    void shouldConvertFromSimpleMapValue()
    {
        // Given
        Value value = value( parameters(
            "operatorType", "X",
            "args", parameters( "a", 1 ),
            "identifiers", values(),
            "children", values()
        ) );

        // When
        Plan plan = InternalPlan.EXPLAIN_PLAN_FROM_VALUE.apply( value );

        // Then
        assertThat( plan.operatorType(), equalTo( "X") );
        assertThat( plan.arguments(), equalTo( parameters( "a", 1 ).asMap( ofValue()) ) );
        assertThat( plan.identifiers(), equalTo( Collections.emptyList() ) );
        assertThat( plan.children(), equalTo( Collections.emptyList() ) );
    }

    @Test
    void shouldConvertFromNestedMapValue()
    {
        // Given
        Value value = value( parameters(
                "operatorType", "X",
                "args", parameters( "a", 1 ),
                "identifiers", values(),
                "children", values(
                    parameters(
                        "operatorType", "Y"
                    )
                )
        ) );

        // When
        Plan plan = InternalPlan.EXPLAIN_PLAN_FROM_VALUE.apply( value );

        // Then
        assertThat( plan.operatorType(), equalTo( "X") );
        assertThat( plan.arguments(), equalTo( parameters( "a", 1 ).asMap( ofValue() ) ) );
        assertThat( plan.identifiers(), equalTo( Collections.emptyList() ) );
        List<? extends Plan> children = plan.children();
        assertThat( children.size(), equalTo( 1 ) );
        assertThat( children.get( 0 ).operatorType(), equalTo( "Y" ) );
    }
}
