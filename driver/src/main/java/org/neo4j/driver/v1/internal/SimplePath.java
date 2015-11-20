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
package org.neo4j.driver.v1.internal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.neo4j.driver.v1.Entity;
import org.neo4j.driver.v1.Node;
import org.neo4j.driver.v1.Path;
import org.neo4j.driver.v1.Relationship;

/**
 * {@link Path} implementation that directly contains all nodes and relationships.
 */
public class SimplePath implements Path
{
    public static class SelfContainedSegment implements Segment
    {
        private final Node start;
        private final Relationship relationship;
        private final Node end;

        public SelfContainedSegment( Node start, Relationship relationship, Node end )
        {
            this.start = start;
            this.relationship = relationship;
            this.end = end;
        }

        @Override
        public Node start()
        {
            return start;
        }

        @Override
        public Relationship relationship()
        {
            return relationship;
        }

        @Override
        public Node end()
        {
            return end;
        }

        @Override
        public boolean equals( Object other )
        {
            if ( this == other )
            {
                return true;
            }
            if ( other == null || getClass() != other.getClass() )
            {
                return false;
            }

            SelfContainedSegment that = (SelfContainedSegment) other;

            return start.equals( that.start ) && end.equals( that.end ) && relationship.equals( that.relationship );

        }

        @Override
        public int hashCode()
        {
            int result = start.hashCode();
            result = 31 * result + relationship.hashCode();
            result = 31 * result + end.hashCode();
            return result;
        }

        @Override
        public String toString()
        {
            return String.format( relationship.start().equals( start.identity() ) ?
                                  "(%s)-[%s:%s]->(%s)" : "(%s)<-[%s:%s]-(%s)",
                    start.identity(), relationship.identity(), relationship.type(), end.identity() );
        }
    }

    private static boolean isEndpoint( Node node, Relationship relationship )
    {
        return node.identity().equals( relationship.start() ) || node.identity().equals( relationship.end() );
    }

    private final List<Node> nodes;
    private final List<Relationship> relationships;
    private final List<Segment> segments;

    public SimplePath( List<Entity> alternatingNodeAndRel )
    {
        nodes = new ArrayList<>(alternatingNodeAndRel.size() / 2 + 1);
        relationships = new ArrayList<>(alternatingNodeAndRel.size() / 2);
        segments = new ArrayList<>(alternatingNodeAndRel.size() / 2);

        if ( alternatingNodeAndRel.size() % 2 == 0 )
        {
            throw new IllegalArgumentException( "An odd number of entities are required to build a path" );
        }
        Node lastNode = null;
        Relationship lastRelationship = null;
        int index = 0;
        for ( Entity entity : alternatingNodeAndRel )
        {
            if ( entity == null )
            {
                throw new IllegalArgumentException( "Path entities cannot be null" );
            }
            if ( index % 2 == 0 )
            {
                // even index - this should be a node
                try
                {
                    lastNode = (Node) entity;
                    if ( nodes.isEmpty() || isEndpoint( lastNode, lastRelationship ) )
                    {
                        nodes.add( lastNode );
                    }
                    else
                    {
                        throw new IllegalArgumentException(
                                "Node argument " + index + " is not an endpoint of relationship argument " + (index -
                                                                                                              1) );
                    }
                }
                catch ( ClassCastException e )
                {
                    String cls = entity.getClass().getName();
                    throw new IllegalArgumentException(
                            "Expected argument " + index + " to be a node " + index + " but found a " + cls + " " +
                            "instead" );
                }
            }
            else
            {
                // odd index - this should be a relationship
                try
                {
                    lastRelationship = (Relationship) entity;
                    if ( isEndpoint( lastNode, lastRelationship ) )
                    {
                        relationships.add( lastRelationship );
                    }
                    else
                    {
                        throw new IllegalArgumentException(
                                "Node argument " + (index - 1) + " is not an endpoint of relationship argument " +
                                index );
                    }
                }
                catch ( ClassCastException e )
                {
                    String cls = entity.getClass().getName();
                    throw new IllegalArgumentException(
                            "Expected argument " + index + " to be a relationship but found a " + cls + " instead" );
                }
            }
            index += 1;
        }
        buildSegments();
    }

    public SimplePath( Entity... alternatingNodeAndRel )
    {
        this( Arrays.asList( alternatingNodeAndRel ) );
    }

    public SimplePath( List<Segment> segments, List<Node> nodes, List<Relationship> relationships )
    {
        this.segments = segments;
        this.nodes = nodes;
        this.relationships = relationships;
    }

    @Override
    public long length()
    {
        return relationships.size();
    }

    @Override
    public boolean contains( Node node )
    {
        return nodes.contains( node );
    }

    @Override
    public boolean contains( Relationship relationship )
    {
        return relationships.contains( relationship );
    }

    @Override
    public Iterable<Node> nodes()
    {
        return nodes;
    }

    @Override
    public Iterable<Relationship> relationships()
    {
        return relationships;
    }

    @Override
    public Node start()
    {
        return nodes.get( 0 );
    }

    @Override
    public Node end()
    {
        return nodes.get( nodes.size() - 1 );
    }

    @Override
    public Iterator<Segment> iterator()
    {
        return segments.iterator();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        SimplePath segments1 = (SimplePath) o;

        return segments.equals( segments1.segments );

    }

    @Override
    public int hashCode()
    {
        return segments.hashCode();
    }

    @Override
    public String toString()
    {

        return "Path[" + segments + ']';
    }

    private void buildSegments()
    {
        for ( int i = 0; i < relationships.size(); i++ )
        {
            segments.add( new SelfContainedSegment( nodes.get( i ), relationships.get( i ), nodes.get( i + 1 ) ) );
        }
    }
}
