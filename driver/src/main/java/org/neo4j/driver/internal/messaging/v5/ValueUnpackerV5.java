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
package org.neo4j.driver.internal.messaging.v5;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.Value;
import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.internal.InternalPath;
import org.neo4j.driver.internal.InternalRelationship;
import org.neo4j.driver.internal.messaging.common.CommonValueUnpacker;
import org.neo4j.driver.internal.packstream.PackInput;
import org.neo4j.driver.internal.types.TypeConstructor;
import org.neo4j.driver.internal.util.Iterables;
import org.neo4j.driver.internal.value.PathValue;
import org.neo4j.driver.internal.value.RelationshipValue;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Path;
import org.neo4j.driver.types.Relationship;

public class ValueUnpackerV5 extends CommonValueUnpacker
{
    private static final int NODE_FIELDS = 4;
    private static final int RELATIONSHIP_FIELDS = 8;

    public ValueUnpackerV5( PackInput input )
    {
        super( input );
    }

    @Override
    protected int getNodeFields()
    {
        return NODE_FIELDS;
    }

    @Override
    protected int getRelationshipFields()
    {
        return RELATIONSHIP_FIELDS;
    }

    @Override
    protected InternalNode unpackNode() throws IOException
    {
        Long urn = unpacker.unpackLongOrNull();

        int numLabels = (int) unpacker.unpackListHeader();
        List<String> labels = new ArrayList<>( numLabels );
        for ( int i = 0; i < numLabels; i++ )
        {
            labels.add( unpacker.unpackString() );
        }
        int numProps = (int) unpacker.unpackMapHeader();
        Map<String,Value> props = Iterables.newHashMapWithSize( numProps );
        for ( int j = 0; j < numProps; j++ )
        {
            String key = unpacker.unpackString();
            props.put( key, unpack() );
        }

        String elementId = unpacker.unpackString();

        return urn == null ? new InternalNode( -1, elementId, labels, props, false ) : new InternalNode( urn, elementId, labels, props, true );
    }

    @Override
    protected Value unpackPath() throws IOException
    {
        // List of unique nodes
        InternalNode[] uniqNodes = new InternalNode[(int) unpacker.unpackListHeader()];
        for ( int i = 0; i < uniqNodes.length; i++ )
        {
            ensureCorrectStructSize( TypeConstructor.NODE, getNodeFields(), unpacker.unpackStructHeader() );
            ensureCorrectStructSignature( "NODE", NODE, unpacker.unpackStructSignature() );
            uniqNodes[i] = unpackNode();
        }

        // List of unique relationships, without start/end information
        InternalRelationship[] uniqRels = new InternalRelationship[(int) unpacker.unpackListHeader()];
        for ( int i = 0; i < uniqRels.length; i++ )
        {
            ensureCorrectStructSize( TypeConstructor.RELATIONSHIP, 4, unpacker.unpackStructHeader() );
            ensureCorrectStructSignature( "UNBOUND_RELATIONSHIP", UNBOUND_RELATIONSHIP, unpacker.unpackStructSignature() );
            Long id = unpacker.unpackLongOrNull();
            String relType = unpacker.unpackString();
            Map<String,Value> props = unpackMap();
            String elementId = unpacker.unpackString();
            uniqRels[i] = id == null
                          ? new InternalRelationship( -1, elementId, -1, String.valueOf( -1 ), -1, String.valueOf( -1 ), relType, props, false )
                          : new InternalRelationship( id, elementId, -1, String.valueOf( -1 ), -1, String.valueOf( -1 ), relType, props, true );
        }

        // Path sequence
        int length = (int) unpacker.unpackListHeader();

        // Knowing the sequence length, we can create the arrays that will represent the nodes, rels and segments in their "path order"
        Path.Segment[] segments = new Path.Segment[length / 2];
        Node[] nodes = new Node[segments.length + 1];
        Relationship[] rels = new Relationship[segments.length];

        InternalNode prevNode = uniqNodes[0], nextNode; // Start node is always 0, and isn't encoded in the sequence
        nodes[0] = prevNode;
        InternalRelationship rel;
        for ( int i = 0; i < segments.length; i++ )
        {
            int relIdx = (int) unpacker.unpackLong();
            nextNode = uniqNodes[(int) unpacker.unpackLong()];
            // Negative rel index means this rel was traversed "inversed" from its direction
            if ( relIdx < 0 )
            {
                rel = uniqRels[(-relIdx) - 1]; // -1 because rel idx are 1-indexed
                setStartAndEnd( rel, nextNode, prevNode );
            }
            else
            {
                rel = uniqRels[relIdx - 1];
                setStartAndEnd( rel, prevNode, nextNode );
            }

            nodes[i + 1] = nextNode;
            rels[i] = rel;
            segments[i] = new InternalPath.SelfContainedSegment( prevNode, rel, nextNode );
            prevNode = nextNode;
        }
        return new PathValue( new InternalPath( Arrays.asList( segments ), Arrays.asList( nodes ), Arrays.asList( rels ) ) );
    }

    private void setStartAndEnd( InternalRelationship rel, InternalNode start, InternalNode end )
    {
        if ( rel.isNumericIdAvailable() && start.isNumericIdAvailable() && end.isNumericIdAvailable() )
        {
            rel.setStartAndEnd( start.id(), start.elementId(), end.id(), end.elementId() );
        }
        else
        {
            rel.setStartAndEnd( -1, start.elementId(), -1, end.elementId() );
        }
    }

    @Override
    protected Value unpackRelationship() throws IOException
    {
        Long urn = unpacker.unpackLongOrNull();
        Long startUrn = unpacker.unpackLongOrNull();
        Long endUrn = unpacker.unpackLongOrNull();
        String relType = unpacker.unpackString();
        Map<String,Value> props = unpackMap();
        String elementId = unpacker.unpackString();
        String startElementId = unpacker.unpackString();
        String endElementId = unpacker.unpackString();

        InternalRelationship adapted = urn == null
                                       ? new InternalRelationship( -1, elementId, -1, startElementId, -1, endElementId, relType, props, false )
                                       : new InternalRelationship( urn, elementId, startUrn, startElementId, endUrn, endElementId, relType, props, true );
        return new RelationshipValue( adapted );
    }
}
