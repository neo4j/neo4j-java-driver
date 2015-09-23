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
package org.neo4j.driver.internal;

import java.util.Collections;
import java.util.List;

import org.junit.Test;

import org.neo4j.driver.PlanTreeNode;
import org.neo4j.driver.Value;
import static org.neo4j.driver.Values.*;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.*;

@SuppressWarnings("unchecked")
public class SimplePlanTreeNodeTest
{
    @Test
    public void shouldConvertFromEmptyMapValue()
    {
        // Given
        Value value = value( parameters( "operatorType", "X" ) );

        // When
        PlanTreeNode plan = SimplePlanTreeNode.FROM_VALUE.apply( value );

        // Then
        assertThat( plan.operatorType(), equalTo( "X") );
        assertThat( plan.arguments(), equalTo( parameters() ) );
        assertThat( plan.identifiers(), equalTo( Collections.<String>emptyList() ) );
        assertThat( (List<PlanTreeNode>) plan.children(), equalTo( Collections.<PlanTreeNode>emptyList() ) );
    }

    @Test
    public void shouldConvertFromSimpleMapValue()
    {
        // Given
        Value value = value( parameters(
            "operatorType", "X",
            "args", parameters( "a", 1 ),
            "identifiers", values(),
            "children", values()
        ) );

        // When
        PlanTreeNode plan = SimplePlanTreeNode.FROM_VALUE.apply( value );

        // Then
        assertThat( plan.operatorType(), equalTo( "X") );
        assertThat( plan.arguments(), equalTo( parameters( "a", 1 ) ) );
        assertThat( plan.identifiers(), equalTo( Collections.<String>emptyList() ) );
        assertThat( (List<PlanTreeNode>) plan.children(), equalTo( Collections.<PlanTreeNode>emptyList() ) );
    }

    @Test
    public void shouldConvertFromNestedMapValue()
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
        PlanTreeNode plan = SimplePlanTreeNode.FROM_VALUE.apply( value );

        // Then
        assertThat( plan.operatorType(), equalTo( "X") );
        assertThat( plan.arguments(), equalTo( parameters( "a", 1 ) ) );
        assertThat( plan.identifiers(), equalTo( Collections.<String>emptyList() ) );
        List<? extends PlanTreeNode> children = plan.children();
        assertThat( children.size(), equalTo( 1 ) );
        assertThat( children.get( 0 ).operatorType(), equalTo( "Y" ) );
    }
}
