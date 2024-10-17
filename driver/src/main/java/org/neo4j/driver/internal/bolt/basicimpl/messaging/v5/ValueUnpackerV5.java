/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.driver.internal.bolt.basicimpl.messaging.v5;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.internal.InternalPath;
import org.neo4j.driver.internal.InternalRelationship;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.common.CommonValueUnpacker;
import org.neo4j.driver.internal.bolt.basicimpl.packstream.PackInput;
import org.neo4j.driver.internal.types.TypeConstructor;
import org.neo4j.driver.internal.value.PathValue;
import org.neo4j.driver.internal.value.RelationshipValue;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Path;
import org.neo4j.driver.types.Relationship;

public class ValueUnpackerV5 extends CommonValueUnpacker {
    private static final int NODE_FIELDS = 4;
    private static final int RELATIONSHIP_FIELDS = 8;

    public ValueUnpackerV5(PackInput input) {
        super(input, true);
    }

    @Override
    protected int getNodeFields() {
        return NODE_FIELDS;
    }

    @Override
    protected int getRelationshipFields() {
        return RELATIONSHIP_FIELDS;
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    protected InternalNode unpackNode() throws IOException {
        var urn = unpacker.unpackLong();

        var numLabels = (int) unpacker.unpackListHeader();
        List<String> labels = new ArrayList<>(numLabels);
        for (var i = 0; i < numLabels; i++) {
            labels.add(unpacker.unpackString());
        }
        var numProps = (int) unpacker.unpackMapHeader();
        Map<String, Value> props = new HashMap<>(numProps);
        for (var j = 0; j < numProps; j++) {
            var key = unpacker.unpackString();
            props.put(key, unpack());
        }

        var elementId = unpacker.unpackString();

        return new InternalNode(urn, elementId, labels, props);
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    protected Value unpackPath() throws IOException {
        // List of unique nodes
        var uniqNodes = new InternalNode[(int) unpacker.unpackListHeader()];
        for (var i = 0; i < uniqNodes.length; i++) {
            ensureCorrectStructSize(TypeConstructor.NODE, getNodeFields(), unpacker.unpackStructHeader());
            ensureCorrectStructSignature("NODE", NODE, unpacker.unpackStructSignature());
            uniqNodes[i] = unpackNode();
        }

        // List of unique relationships, without start/end information
        var uniqRels = new InternalRelationship[(int) unpacker.unpackListHeader()];
        for (var i = 0; i < uniqRels.length; i++) {
            ensureCorrectStructSize(TypeConstructor.RELATIONSHIP, 4, unpacker.unpackStructHeader());
            ensureCorrectStructSignature(
                    "UNBOUND_RELATIONSHIP", UNBOUND_RELATIONSHIP, unpacker.unpackStructSignature());
            var id = unpacker.unpackLong();
            var relType = unpacker.unpackString();
            var props = unpackMap();
            var elementId = unpacker.unpackString();
            uniqRels[i] = new InternalRelationship(
                    id, elementId, -1, String.valueOf(-1), -1, String.valueOf(-1), relType, props);
        }

        // Path sequence
        var length = (int) unpacker.unpackListHeader();

        // Knowing the sequence length, we can create the arrays that will represent the nodes, rels and segments in
        // their "path order"
        var segments = new Path.Segment[length / 2];
        var nodes = new Node[segments.length + 1];
        var rels = new Relationship[segments.length];

        InternalNode prevNode = uniqNodes[0], nextNode; // Start node is always 0, and isn't encoded in the sequence
        nodes[0] = prevNode;
        InternalRelationship rel;
        for (var i = 0; i < segments.length; i++) {
            var relIdx = (int) unpacker.unpackLong();
            nextNode = uniqNodes[(int) unpacker.unpackLong()];
            // Negative rel index means this rel was traversed "inversed" from its direction
            if (relIdx < 0) {
                rel = uniqRels[(-relIdx) - 1]; // -1 because rel idx are 1-indexed
                setStartAndEnd(rel, nextNode, prevNode);
            } else {
                rel = uniqRels[relIdx - 1];
                setStartAndEnd(rel, prevNode, nextNode);
            }

            nodes[i + 1] = nextNode;
            rels[i] = rel;
            segments[i] = new InternalPath.SelfContainedSegment(prevNode, rel, nextNode);
            prevNode = nextNode;
        }
        return new PathValue(new InternalPath(Arrays.asList(segments), Arrays.asList(nodes), Arrays.asList(rels)));
    }

    @SuppressWarnings("deprecation")
    private void setStartAndEnd(InternalRelationship rel, InternalNode start, InternalNode end) {
        rel.setStartAndEnd(start.id(), start.elementId(), end.id(), end.elementId());
    }

    @Override
    protected Value unpackRelationship() throws IOException {
        var urn = unpacker.unpackLong();
        var startUrn = unpacker.unpackLong();
        var endUrn = unpacker.unpackLong();
        var relType = unpacker.unpackString();
        var props = unpackMap();
        var elementId = unpacker.unpackString();
        var startElementId = unpacker.unpackString();
        var endElementId = unpacker.unpackString();

        var adapted = new InternalRelationship(
                urn, elementId, startUrn, startElementId, endUrn, endElementId, relType, props);
        return new RelationshipValue(adapted);
    }
}
