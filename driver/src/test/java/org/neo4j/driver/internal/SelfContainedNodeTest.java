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
package org.neo4j.driver.internal;

import org.junit.jupiter.api.Test;

import java.util.List;

import org.neo4j.driver.internal.util.Iterables;
import org.neo4j.driver.Values;
import org.neo4j.driver.types.Node;

import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.neo4j.driver.Values.ofValue;
import static org.neo4j.driver.Values.parameters;

class SelfContainedNodeTest
{
    private Node adamTheNode()
    {
        return new InternalNode( 1, singletonList( "Person" ),
                parameters( "name", Values.value( "Adam" ) ).asMap( ofValue()) );
    }

    @Test
    void testIdentity()
    {
        // Given
        Node node = adamTheNode();

        // Then
        assertThat( node.id(), equalTo( 1l ) );
    }

    @Test
    void testLabels()
    {
        // Given
        Node node = adamTheNode();

        // Then
        List<String> labels = Iterables.asList( node.labels() );
        assertThat( labels.size(), equalTo( 1 ) );
        assertThat( labels.contains( "Person" ), equalTo( true ) );
    }

    @Test
    void testKeys()
    {
        // Given
        Node node = adamTheNode();

        // Then
        List<String> keys = Iterables.asList( node.keys() );
        assertThat( keys.size(), equalTo( 1 ) );
        assertThat( keys.contains( "name" ), equalTo( true ) );
    }

    @Test
    void testValue()
    {
        // Given
        Node node = adamTheNode();

        // Then
        assertThat( node.get( "name" ).asString(), equalTo( "Adam" ) );
    }
}
