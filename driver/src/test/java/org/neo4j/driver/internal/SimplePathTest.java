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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;

import org.neo4j.driver.Node;
import org.neo4j.driver.Path;
import org.neo4j.driver.Relationship;
import org.neo4j.driver.util.Lists;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class SimplePathTest
{
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    // (A)-[AB:KNOWS]->(B)<-[CB:KNOWS]-(C)-[CD:KNOWS]->(D)
    private SimplePath testPath()
    {
        return new SimplePath(
                new SimpleNode( 1 ),
                new SimpleRelationship( -1, 1, 2, "KNOWS" ),
                new SimpleNode( 2 ),
                new SimpleRelationship( -2, 3, 2, "KNOWS" ),
                new SimpleNode( 3 ),
                new SimpleRelationship( -3, 3, 4, "KNOWS" ),
                new SimpleNode( 4 )
        );
    }

    @Test
    public void pathSizeShouldReturnNumberOfRelationships()
    {
        // When
        SimplePath path = testPath();

        // Then
        assertThat( path.length(), equalTo( 3L ) );
    }

    @Test
    public void shouldBeAbleToCreatePathWithSingleNode()
    {
        // When
        SimplePath path = new SimplePath( new SimpleNode( 1 ) );

        // Then
        assertThat( path.length(), equalTo( 0L ) );
    }

    @Test
    public void shouldBeAbleToIterateOverPathAsSegments() throws Exception
    {
        // Given
        SimplePath path = testPath();

        // When
        List<Path.Segment> segments = Lists.asList( path );

        // Then
        assertThat( segments, equalTo( Arrays.asList( (Path.Segment)
                        new SimplePath.SelfContainedSegment(
                                new SimpleNode( 1 ),
                                new SimpleRelationship( -1, 1, 2, "KNOWS" ),
                                new SimpleNode( 2 )
                        ),
                        new SimplePath.SelfContainedSegment(
                                new SimpleNode( 2 ),
                                new SimpleRelationship( -2, 3, 2, "KNOWS" ),
                                new SimpleNode( 3 )
                        ),
                        new SimplePath.SelfContainedSegment(
                                new SimpleNode( 3 ),
                                new SimpleRelationship( -3, 3, 4, "KNOWS" ),
                                new SimpleNode( 4 )
                        )
                )
        ) );
    }

    @Test
    public void shouldBeAbleToIterateOverPathNodes() throws Exception
    {
        // Given
        SimplePath path = testPath();

        // When
        List<Node> segments = Lists.asList( path.nodes() );

        // Then
        assertThat( segments, equalTo( Arrays.asList( (Node)
                new SimpleNode( 1 ),
                new SimpleNode( 2 ),
                new SimpleNode( 3 ),
                new SimpleNode( 4 ) ) ) );
    }

    @Test
    public void shouldBeAbleToIterateOverPathRelationships() throws Exception
    {
        // Given
        SimplePath path = testPath();

        // When
        List<Relationship> segments = Lists.asList( path.relationships() );

        // Then
        assertThat( segments, equalTo( Arrays.asList( (Relationship)
                new SimpleRelationship( -1, 1, 2, "KNOWS" ),
                new SimpleRelationship( -2, 3, 2, "KNOWS" ),
                new SimpleRelationship( -3, 3, 4, "KNOWS" ) ) ) );
    }

    @Test
    public void shouldNotBeAbleToCreatePathWithNoEntities()
    {
        // Expect
        thrown.expect( IllegalArgumentException.class );

        // When
        new SimplePath();

    }

    @Test
    public void shouldNotBeAbleToCreatePathWithEvenNumberOfEntities()
    {
        // Expect
        thrown.expect( IllegalArgumentException.class );

        // When
        new SimplePath(
                new SimpleNode( 1 ),
                new SimpleRelationship( 2, 3, 4, "KNOWS" ) );

    }

    @Test
    public void shouldNotBeAbleToCreatePathWithNullEntities()
    {
        // Expect
        thrown.expect( IllegalArgumentException.class );

        // When
        SimpleNode nullNode = null;
        //noinspection ConstantConditions
        new SimplePath( nullNode );

    }

    @Test
    public void shouldNotBeAbleToCreatePathWithNodeThatDoesNotConnect()
    {
        // Expect
        thrown.expect( IllegalArgumentException.class );

        // When
        new SimplePath(
                new SimpleNode( 1 ),
                new SimpleRelationship( 2, 1, 3, "KNOWS" ),
                new SimpleNode( 4 ) );

    }

    @Test
    public void shouldNotBeAbleToCreatePathWithRelationshipThatDoesNotConnect()
    {
        // Expect
        thrown.expect( IllegalArgumentException.class );

        // When
        new SimplePath(
                new SimpleNode( 1 ),
                new SimpleRelationship( 2, 3, 4, "KNOWS" ),
                new SimpleNode( 3 ) );

    }

}