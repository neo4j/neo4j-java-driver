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
package org.neo4j.driver.internal.bolt.basicimpl.util.messaging;

import java.io.IOException;
import java.util.Map;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.AbstractMessageWriter;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.MessageEncoder;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.common.CommonValuePacker;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.common.CommonValueUnpacker;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.encode.DiscardAllMessageEncoder;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.encode.PullAllMessageEncoder;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.encode.ResetMessageEncoder;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.request.DiscardAllMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.request.PullAllMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.request.ResetMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.response.FailureMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.response.IgnoredMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.response.RecordMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.response.SuccessMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v3.MessageFormatV3;
import org.neo4j.driver.internal.bolt.basicimpl.packstream.PackOutput;
import org.neo4j.driver.internal.util.Iterables;
import org.neo4j.driver.internal.value.InternalValue;
import org.neo4j.driver.types.Entity;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Path;
import org.neo4j.driver.types.Relationship;

/**
 * This class provides the missing server side packing methods to serialize Node, Relationship and Path. It also allows writing of server side messages like
 * SUCCESS, FAILURE, IGNORED and RECORD.
 */
public class KnowledgeableMessageFormat extends MessageFormatV3 {
    private final boolean elementIdEnabled;
    private boolean dateTimeUtcEnabled;

    public KnowledgeableMessageFormat(boolean elementIdEnabled) {
        this.elementIdEnabled = elementIdEnabled;
    }

    @Override
    public Writer newWriter(PackOutput output) {
        return new KnowledgeableMessageWriter(output, elementIdEnabled, dateTimeUtcEnabled);
    }

    @Override
    public void enableDateTimeUtc() {
        dateTimeUtcEnabled = true;
    }

    private static class KnowledgeableMessageWriter extends AbstractMessageWriter {
        KnowledgeableMessageWriter(PackOutput output, boolean enableElementId, boolean dateTimeUtcEnabled) {
            super(new KnowledgeableValuePacker(output, enableElementId, dateTimeUtcEnabled), buildEncoders());
        }

        static Map<Byte, MessageEncoder> buildEncoders() {
            Map<Byte, MessageEncoder> result = Iterables.newHashMapWithSize(10);
            // request message encoders
            result.put(DiscardAllMessage.SIGNATURE, new DiscardAllMessageEncoder());
            result.put(PullAllMessage.SIGNATURE, new PullAllMessageEncoder());
            result.put(ResetMessage.SIGNATURE, new ResetMessageEncoder());
            // response message encoders
            result.put(FailureMessage.SIGNATURE, new FailureMessageEncoder());
            result.put(IgnoredMessage.SIGNATURE, new IgnoredMessageEncoder());
            result.put(RecordMessage.SIGNATURE, new RecordMessageEncoder());
            result.put(SuccessMessage.SIGNATURE, new SuccessMessageEncoder());
            return result;
        }
    }

    private static class KnowledgeableValuePacker extends CommonValuePacker {
        private final boolean elementIdEnabled;

        KnowledgeableValuePacker(PackOutput output, boolean elementIdEnabled, boolean dateTimeUtcEnabled) {
            super(output, dateTimeUtcEnabled);
            this.elementIdEnabled = elementIdEnabled;
        }

        @Override
        protected void packInternalValue(InternalValue value) throws IOException {
            var typeConstructor = value.typeConstructor();
            switch (typeConstructor) {
                case NODE -> {
                    var node = value.asNode();
                    packNode(node);
                }
                case RELATIONSHIP -> {
                    var rel = value.asRelationship();
                    packRelationship(rel);
                }
                case PATH -> {
                    var path = value.asPath();
                    packPath(path);
                }
                default -> super.packInternalValue(value);
            }
        }

        @SuppressWarnings("deprecation")
        private void packPath(Path path) throws IOException {
            packer.packStructHeader(3, CommonValueUnpacker.PATH);

            // Unique nodes
            Map<Node, Integer> nodeIdx = Iterables.newLinkedHashMapWithSize(path.length() + 1);
            for (var node : path.nodes()) {
                if (!nodeIdx.containsKey(node)) {
                    nodeIdx.put(node, nodeIdx.size());
                }
            }
            packer.packListHeader(nodeIdx.size());
            for (var node : nodeIdx.keySet()) {
                packNode(node);
            }

            // Unique rels
            Map<Relationship, Integer> relIdx = Iterables.newLinkedHashMapWithSize(path.length());
            for (var rel : path.relationships()) {
                if (!relIdx.containsKey(rel)) {
                    relIdx.put(rel, relIdx.size() + 1);
                }
            }
            packer.packListHeader(relIdx.size());
            for (var rel : relIdx.keySet()) {
                packer.packStructHeader(elementIdEnabled ? 4 : 3, CommonValueUnpacker.UNBOUND_RELATIONSHIP);
                packer.pack(rel.id());
                packer.pack(rel.type());
                packProperties(rel);
                if (elementIdEnabled) {
                    packer.pack(rel.elementId());
                }
            }

            // Sequence
            packer.packListHeader(path.length() * 2);
            for (var seg : path) {
                var rel = seg.relationship();
                var relEndId = rel.endNodeId();
                var segEndId = seg.end().id();
                var size = relEndId == segEndId ? relIdx.get(rel) : -relIdx.get(rel);
                packer.pack(size);
                packer.pack(nodeIdx.get(seg.end()));
            }
        }

        @SuppressWarnings("deprecation")
        private void packRelationship(Relationship rel) throws IOException {
            packer.packStructHeader(elementIdEnabled ? 8 : 5, CommonValueUnpacker.RELATIONSHIP);
            packer.pack(rel.id());
            packer.pack(rel.startNodeId());
            packer.pack(rel.endNodeId());

            packer.pack(rel.type());

            packProperties(rel);

            if (elementIdEnabled) {
                packer.pack(rel.elementId());
                packer.pack(rel.startNodeElementId());
                packer.pack(rel.endNodeElementId());
            }
        }

        @SuppressWarnings("deprecation")
        private void packNode(Node node) throws IOException {
            packer.packStructHeader(elementIdEnabled ? 4 : 3, CommonValueUnpacker.NODE);
            packer.pack(node.id());

            var labels = node.labels();
            packer.packListHeader(Iterables.count(labels));
            for (var label : labels) {
                packer.pack(label);
            }

            packProperties(node);

            if (elementIdEnabled) {
                packer.pack(node.elementId());
            }
        }

        private void packProperties(Entity entity) throws IOException {
            var keys = entity.keys();
            packer.packMapHeader(entity.size());
            for (var propKey : keys) {
                packer.pack(propKey);
                packInternalValue((InternalValue) entity.get(propKey));
            }
        }
    }
}
