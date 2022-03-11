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
    public ValueUnpackerV5( PackInput input )
    {
        super( input );
    }

    @Override
    protected int getNodeFields()
    {
        return 4;
    }

    @Override
    protected int getRelationshipFields()
    {
        return 8;
    }

    @Override
    protected InternalNode unpackNode() throws IOException
    {
        long urn = unpacker.unpackLongOrDefaultOnNull( -1 );

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

        return new InternalNode( urn, elementId, labels, props );
    }

    @Override
    protected Value unpackPath() throws IOException
    {
        // List of unique nodes
        Node[] uniqNodes = new Node[(int) unpacker.unpackListHeader()];
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
            Long id = unpacker.unpackLongOrDefaultOnNull( -1 );
            String relType = unpacker.unpackString();
            Map<String,Value> props = unpackMap();
            String elementId = unpacker.unpackString();
            uniqRels[i] = new InternalRelationship( id, elementId, -1, String.valueOf( -1 ), -1, String.valueOf( -1 ), relType, props );
        }

        // Path sequence
        int length = (int) unpacker.unpackListHeader();

        // Knowing the sequence length, we can create the arrays that will represent the nodes, rels and segments in their "path order"
        Path.Segment[] segments = new Path.Segment[length / 2];
        Node[] nodes = new Node[segments.length + 1];
        Relationship[] rels = new Relationship[segments.length];

        Node prevNode = uniqNodes[0], nextNode; // Start node is always 0, and isn't encoded in the sequence
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
                rel.setStartAndEnd( nextNode.id(), nextNode.elementId(), prevNode.id(), prevNode.elementId() );
            }
            else
            {
                rel = uniqRels[relIdx - 1];
                rel.setStartAndEnd( prevNode.id(), prevNode.elementId(), nextNode.id(), nextNode.elementId() );
            }

            nodes[i + 1] = nextNode;
            rels[i] = rel;
            segments[i] = new InternalPath.SelfContainedSegment( prevNode, rel, nextNode );
            prevNode = nextNode;
        }
        return new PathValue( new InternalPath( Arrays.asList( segments ), Arrays.asList( nodes ), Arrays.asList( rels ) ) );
    }

    @Override
    protected Value unpackRelationship() throws IOException
    {
        long urn = unpacker.unpackLongOrDefaultOnNull( -1 );
        long startUrn = unpacker.unpackLongOrDefaultOnNull( -1 );
        long endUrn = unpacker.unpackLongOrDefaultOnNull( -1 );
        String relType = unpacker.unpackString();
        Map<String,Value> props = unpackMap();
        String elementId = unpacker.unpackString();
        String startElementId = unpacker.unpackString();
        String endElementId = unpacker.unpackString();

        InternalRelationship adapted = new InternalRelationship( urn, elementId, startUrn, startElementId, endUrn, endElementId, relType, props );
        return new RelationshipValue( adapted );
    }
}
