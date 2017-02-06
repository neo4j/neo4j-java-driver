/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.driver.hydration;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;
import org.neo4j.driver.internal.util.Iterables;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.SelfContainedNode;
import org.neo4j.driver.v1.util.Function;

import java.util.*;

import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertFalse;
import static org.neo4j.driver.v1.Values.*;

public class SelfContainedNodeTest
{

    private Node adamTheNode()
    {
        return new SelfContainedNode( 1, singletonList( "Person" ),
                parameters( "name", Values.value( "Adam" ) ).asMap( ofValue()) );
    }

    @Test
    public void testIdentity()
    {
        // Given
        Node node = adamTheNode();

        // Then
        assertThat( node.id(), equalTo( 1l ) );
    }

    @Test
    public void testLabels()
    {
        // Given
        Node node = adamTheNode();

        // Then
        List<String> labels = Iterables.asList( node.labels() );
        assertThat( labels.size(), equalTo( 1 ) );
        assertThat( labels.contains( "Person" ), equalTo( true ) );
    }

    @Test
    public void testKeys()
    {
        // Given
        Node node = adamTheNode();

        // Then
        List<String> keys = Iterables.asList( node.keys() );
        assertThat( keys.size(), equalTo( 1 ) );
        assertThat( keys.contains( "name" ), equalTo( true ) );
    }

    @Test
    public void testValue()
    {
        // Given
        Node node = adamTheNode();

        // Then
        assertThat( node.get( "name" ).asString(), equalTo( "Adam" ) );
    }

    @Test
    public void extractValuesFromNode()
    {
        // GIVEN
        SelfContainedNode node = createNode();
        Function<Value,Integer> extractor = new Function<Value,Integer>()
        {
            @Override
            public Integer apply( Value value )
            {
                return value.asInt();
            }
        };

        //WHEN
        Iterable<Integer> values = node.values( extractor );

        //THEN
        Iterator<Integer> iterator = values.iterator();
        Assert.assertThat( iterator.next(), CoreMatchers.equalTo( 1 ) );
        Assert.assertThat( iterator.next(), CoreMatchers.equalTo( 2 ) );
        assertFalse( iterator.hasNext() );
    }

    @Test
    public void accessUnknownKeyShouldBeNull()
    {
        SelfContainedNode node = createNode();

        Assert.assertThat( node.get( "k1" ), CoreMatchers.equalTo( value( 1 ) ) );
        Assert.assertThat( node.get( "k2" ), CoreMatchers.equalTo( value( 2 ) ) );
        Assert.assertThat( node.get( "k3" ), CoreMatchers.equalTo( NULL ) );
    }

    private SelfContainedNode createNode()
    {
        Map<String,Value> props = new HashMap<>();
        props.put( "k1", value( 1 ) );
        props.put( "k2", value( 2 ) );
        return new SelfContainedNode( 42L, Collections.singletonList( "L" ), props );
    }

}
