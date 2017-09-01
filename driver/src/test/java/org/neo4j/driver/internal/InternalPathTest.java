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
package org.neo4j.driver.internal;

import org.hamcrest.MatcherAssert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;

import org.neo4j.driver.internal.util.Iterables;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Path;
import org.neo4j.driver.v1.types.Relationship;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class InternalPathTest
{
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    // (A)-[AB:KNOWS]->(B)<-[CB:KNOWS]-(C)-[CD:KNOWS]->(D)
    private InternalPath testPath()
    {
        return new InternalPath(
                new InternalNode( 1 ),
                new InternalRelationship( -1, 1, 2, "KNOWS" ),
                new InternalNode( 2 ),
                new InternalRelationship( -2, 3, 2, "KNOWS" ),
                new InternalNode( 3 ),
                new InternalRelationship( -3, 3, 4, "KNOWS" ),
                new InternalNode( 4 )
        );
    }

    @Test
    public void pathSizeShouldReturnNumberOfRelationships()
    {
        // When
        InternalPath path = testPath();

        // Then
        assertThat( path.length(), equalTo( 3 ) );
    }

    @Test
    public void shouldBeAbleToCreatePathWithSingleNode()
    {
        // When
        InternalPath path = new InternalPath( new InternalNode( 1 ) );

        // Then
        assertThat( path.length(), equalTo( 0 ) );
    }

    @Test
    public void shouldBeAbleToIterateOverPathAsSegments() throws Exception
    {
        // Given
        InternalPath path = testPath();

        // When
        List<Path.Segment> segments = Iterables.asList( path );

        // Then
        MatcherAssert.assertThat( segments, equalTo( Arrays.asList( (Path.Segment)
                                new InternalPath.SelfContainedSegment(
                                        new InternalNode( 1 ),
                                        new InternalRelationship( -1, 1, 2, "KNOWS" ),
                                        new InternalNode( 2 )
                                ),
                        new InternalPath.SelfContainedSegment(
                                new InternalNode( 2 ),
                                new InternalRelationship( -2, 3, 2, "KNOWS" ),
                                new InternalNode( 3 )
                        ),
                        new InternalPath.SelfContainedSegment(
                                new InternalNode( 3 ),
                                new InternalRelationship( -3, 3, 4, "KNOWS" ),
                                new InternalNode( 4 )
                        )
                )
        ) );
    }

    @Test
    public void shouldBeAbleToIterateOverPathNodes() throws Exception
    {
        // Given
        InternalPath path = testPath();

        // When
        List<Node> segments = Iterables.asList( path.nodes() );

        // Then
        assertThat( segments, equalTo( Arrays.asList( (Node)
                new InternalNode( 1 ),
                new InternalNode( 2 ),
                new InternalNode( 3 ),
                new InternalNode( 4 ) ) ) );
    }

    @Test
    public void shouldBeAbleToIterateOverPathRelationships() throws Exception
    {
        // Given
        InternalPath path = testPath();

        // When
        List<Relationship> segments = Iterables.asList( path.relationships() );

        // Then
        assertThat( segments, equalTo( Arrays.asList( (Relationship)
                new InternalRelationship( -1, 1, 2, "KNOWS" ),
                new InternalRelationship( -2, 3, 2, "KNOWS" ),
                new InternalRelationship( -3, 3, 4, "KNOWS" ) ) ) );
    }

    @Test
    public void shouldNotBeAbleToCreatePathWithNoEntities()
    {
        // Expect
        thrown.expect( IllegalArgumentException.class );

        // When
        new InternalPath();

    }

    @Test
    public void shouldNotBeAbleToCreatePathWithEvenNumberOfEntities()
    {
        // Expect
        thrown.expect( IllegalArgumentException.class );

        // When
        new InternalPath(
                new InternalNode( 1 ),
                new InternalRelationship( 2, 3, 4, "KNOWS" ) );

    }

    @Test
    public void shouldNotBeAbleToCreatePathWithNullEntities()
    {
        // Expect
        thrown.expect( IllegalArgumentException.class );

        // When
        InternalNode nullNode = null;
        //noinspection ConstantConditions
        new InternalPath( nullNode );

    }

    @Test
    public void shouldNotBeAbleToCreatePathWithNodeThatDoesNotConnect()
    {
        // Expect
        thrown.expect( IllegalArgumentException.class );

        // When
        new InternalPath(
                new InternalNode( 1 ),
                new InternalRelationship( 2, 1, 3, "KNOWS" ),
                new InternalNode( 4 ) );

    }

    @Test
    public void shouldNotBeAbleToCreatePathWithRelationshipThatDoesNotConnect()
    {
        // Expect
        thrown.expect( IllegalArgumentException.class );

        // When
        new InternalPath(
                new InternalNode( 1 ),
                new InternalRelationship( 2, 3, 4, "KNOWS" ),
                new InternalNode( 3 ) );

    }

}
