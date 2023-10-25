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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.driver.Values.ofValue;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.Values.values;

import java.util.Collections;
import org.junit.jupiter.api.Test;

class InternalPlanTest {
    @Test
    void shouldConvertFromEmptyMapValue() {
        // Given
        var value = value(parameters("operatorType", "X"));

        // When
        var plan = InternalPlan.EXPLAIN_PLAN_FROM_VALUE.apply(value);

        // Then
        assertThat(plan.operatorType(), equalTo("X"));
        assertThat(plan.arguments(), equalTo(parameters().asMap(ofValue())));
        assertThat(plan.identifiers(), equalTo(Collections.emptyList()));
        assertThat(plan.children(), equalTo(Collections.emptyList()));
    }

    @Test
    void shouldConvertFromSimpleMapValue() {
        // Given
        var value = value(parameters(
                "operatorType", "X",
                "args", parameters("a", 1),
                "identifiers", values(),
                "children", values()));

        // When
        var plan = InternalPlan.EXPLAIN_PLAN_FROM_VALUE.apply(value);

        // Then
        assertThat(plan.operatorType(), equalTo("X"));
        assertThat(plan.arguments(), equalTo(parameters("a", 1).asMap(ofValue())));
        assertThat(plan.identifiers(), equalTo(Collections.emptyList()));
        assertThat(plan.children(), equalTo(Collections.emptyList()));
    }

    @Test
    void shouldConvertFromNestedMapValue() {
        // Given
        var value = value(parameters(
                "operatorType", "X",
                "args", parameters("a", 1),
                "identifiers", values(),
                "children", values(parameters("operatorType", "Y"))));

        // When
        var plan = InternalPlan.EXPLAIN_PLAN_FROM_VALUE.apply(value);

        // Then
        assertThat(plan.operatorType(), equalTo("X"));
        assertThat(plan.arguments(), equalTo(parameters("a", 1).asMap(ofValue())));
        assertThat(plan.identifiers(), equalTo(Collections.emptyList()));
        var children = plan.children();
        assertThat(children.size(), equalTo(1));
        assertThat(children.get(0).operatorType(), equalTo("Y"));
    }
}
