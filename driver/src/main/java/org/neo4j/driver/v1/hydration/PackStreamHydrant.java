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

package org.neo4j.driver.v1.hydration;

import org.neo4j.driver.internal.packstream.PackStream.Unpacker;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Path;
import org.neo4j.driver.v1.types.Relationship;

import java.io.IOException;
import java.util.*;

import static java.lang.String.format;

public class PackStreamHydrant
{
    public static final byte NODE = 'N';
    public static final byte RELATIONSHIP = 'R';
    public static final byte RELATIONSHIP_DETAIL = 'r';
    public static final byte PATH = 'P';

    private final Unpacker unpacker;

    public PackStreamHydrant(Unpacker unpacker)
    {
        this.unpacker = unpacker;
    }

    protected void checkSize(long size, long validSize)
    {
        if (size != validSize)
        {
            throw new HydrationException(format("Invalid structure size %d", size));
        }
    }

    protected void checkSignature(byte signature, byte expectedSignature)
    {
        if (signature != expectedSignature)
        {
            throw new HydrationException(format("Expected structure signature %d, found signature %d", expectedSignature, signature));
        }
    }

    protected Node hydrateNode(long size) throws IOException
    {
        checkSize(size, 3);

        long id = unpacker.unpackLong();

        int numLabels = (int) unpacker.unpackListHeader();
        List<String> labels = new ArrayList<>(numLabels);
        for (int i = 0; i < numLabels; i++)
        {
            labels.add(unpacker.unpackString());
        }
        int numProps = (int) unpacker.unpackMapHeader();
        Map<String, Value> props = new HashMap<>();
        for (int j = 0; j < numProps; j++)
        {
            String key = unpacker.unpackString();
            props.put(key, unpacker.unpackValue());
        }

        return new SelfContainedNode(id, labels, props);
    }

    protected Relationship hydrateRelationship(long size) throws IOException
    {
        checkSize(size, 5);

        long id = unpacker.unpackLong();
        long startNodeId = unpacker.unpackLong();
        long endNodeId = unpacker.unpackLong();
        String type = unpacker.unpackString();
        Map<String, Value> properties = unpacker.unpackMap();

        return new SelfContainedRelationship(id, startNodeId, endNodeId, type, properties);
    }

    private RelationshipDetail hydrateRelationshipDetail(long size) throws IOException
    {
        checkSize(size, 3);

        long id = unpacker.unpackLong();
        String type = unpacker.unpackString();
        Map<String, Value> properties = unpacker.unpackMap();

        return new RelationshipDetail(id, type, properties);
    }

    protected Path hydratePath(long size) throws IOException
    {
        checkSize(size, 3);

        // List of unique nodes
        Node[] uniqueNodes = new Node[(int) unpacker.unpackListHeader()];
        for (int i = 0; i < uniqueNodes.length; i++)
        {
            long entitySize = unpacker.unpackStructHeader();
            checkSignature(unpacker.unpackStructSignature(), NODE);
            uniqueNodes[i] = hydrateNode(entitySize);
        }

        // List of unique relationships, without start/end information
        RelationshipDetail[] uniqueRelationshipDetails = new RelationshipDetail[(int) unpacker.unpackListHeader()];
        for (int i = 0; i < uniqueRelationshipDetails.length; i++)
        {
            long entitySize = unpacker.unpackStructHeader();
            checkSignature(unpacker.unpackStructSignature(), RELATIONSHIP_DETAIL);
            uniqueRelationshipDetails[i] = hydrateRelationshipDetail(entitySize);
        }

        // Path sequence
        int length = (int) unpacker.unpackListHeader();

        // Knowing the sequence length, we can create the arrays that will represent the nodes, rels and segments in their "path order"
        Path.Segment[] segments = new Path.Segment[length / 2];
        Node[] nodes = new Node[segments.length + 1];
        Relationship[] relationships = new Relationship[segments.length];

        Node lastNode = uniqueNodes[0], nextNode; // Start node is always 0, and isn't encoded in the sequence
        nodes[0] = lastNode;
        Relationship relationship;
        for (int i = 0; i < segments.length; i++)
        {
            int relationshipIndex = (int) unpacker.unpackLong();
            nextNode = uniqueNodes[(int) unpacker.unpackLong()];
            // Negative rel index means this rel was traversed "inversed" from its direction
            if (relationshipIndex < 0)
            {
                RelationshipDetail rel = uniqueRelationshipDetails[(-relationshipIndex) - 1];
                relationship = new SelfContainedRelationship(rel.id(), nextNode.id(), lastNode.id(), rel.type(), rel.properties());
            }
            else
            {
                RelationshipDetail rel = uniqueRelationshipDetails[relationshipIndex - 1];
                relationship = new SelfContainedRelationship(rel.id(), lastNode.id(), nextNode.id(), rel.type(), rel.properties());
            }
            nodes[i + 1] = nextNode;
            relationships[i] = relationship;
            segments[i] = new SelfContainedPath.Segment(lastNode, relationship, nextNode);
            lastNode = nextNode;
        }
        return new SelfContainedPath(Arrays.asList(segments), Arrays.asList(nodes), Arrays.asList(relationships));
    }

    public Value hydrateStructure(long size, byte signature) throws IOException
    {
        switch (signature)
        {
        case NODE:
            return new NodeValue(hydrateNode(size));
        case RELATIONSHIP:
            return new RelationshipValue(hydrateRelationship(size));
        case PATH:
            return new PathValue(hydratePath(size));
        default:
            throw new HydrationException(format("Unknown signature '%s'", signature));
        }
    }

}
