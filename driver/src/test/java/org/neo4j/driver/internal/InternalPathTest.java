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

import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import org.neo4j.driver.internal.util.Iterables;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Path;
import org.neo4j.driver.types.Relationship;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class InternalPathTest
{
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
    void pathSizeShouldReturnNumberOfRelationships()
    {
        // When
        InternalPath path = testPath();

        // Then
        assertThat( path.length(), equalTo( 3 ) );
    }

    @Test
    void shouldBeAbleToCreatePathWithSingleNode()
    {
        // When
        InternalPath path = new InternalPath( new InternalNode( 1 ) );

        // Then
        assertThat( path.length(), equalTo( 0 ) );
    }

    @Test
    void shouldBeAbleToIterateOverPathAsSegments()
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
    void shouldBeAbleToIterateOverPathNodes()
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
    void shouldBeAbleToIterateOverPathRelationships()
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
    void shouldNotBeAbleToCreatePathWithNoEntities()
    {
        assertThrows( IllegalArgumentException.class, InternalPath::new );
    }

    @Test
    void shouldNotBeAbleToCreatePathWithEvenNumberOfEntities()
    {
        assertThrows( IllegalArgumentException.class,
                () -> new InternalPath(
                        new InternalNode( 1 ),
                        new InternalRelationship( 2, 3, 4, "KNOWS" ) ) );
    }

    @Test
    void shouldNotBeAbleToCreatePathWithNullEntities()
    {
        InternalNode nullNode = null;
        assertThrows( IllegalArgumentException.class, () -> new InternalPath( nullNode ) );
    }

    @Test
    void shouldNotBeAbleToCreatePathWithNodeThatDoesNotConnect()
    {
        assertThrows( IllegalArgumentException.class,
                () -> new InternalPath(
                        new InternalNode( 1 ),
                        new InternalRelationship( 2, 1, 3, "KNOWS" ),
                        new InternalNode( 4 ) ) );
    }

    @Test
    void shouldNotBeAbleToCreatePathWithRelationshipThatDoesNotConnect()
    {
        assertThrows( IllegalArgumentException.class,
                () -> new InternalPath(
                        new InternalNode( 1 ),
                        new InternalRelationship( 2, 3, 4, "KNOWS" ),
                        new InternalNode( 3 ) ) );
    }

}
