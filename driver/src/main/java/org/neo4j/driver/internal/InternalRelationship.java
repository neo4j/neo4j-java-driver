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
package org.neo4j.driver.internal;

import java.util.Collections;
import java.util.Map;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.value.RelationshipValue;
import org.neo4j.driver.types.Relationship;

/**
 * {@link Relationship} implementation that directly contains type and properties.
 */
public class InternalRelationship extends InternalEntity
        implements Relationship, org.neo4j.bolt.connection.values.Relationship {
    private long start;
    private String startElementId;
    private long end;
    private String endElementId;
    private final String type;

    public InternalRelationship(long id, long start, long end, String type) {
        this(id, start, end, type, Collections.emptyMap());
    }

    public InternalRelationship(long id, long start, long end, String type, Map<String, Value> properties) {
        this(id, String.valueOf(id), start, String.valueOf(start), end, String.valueOf(end), type, properties);
    }

    public InternalRelationship(
            long id,
            String elementId,
            long start,
            String startElementId,
            long end,
            String endElementId,
            String type,
            Map<String, Value> properties) {
        super(id, elementId, properties);
        this.start = start;
        this.startElementId = startElementId;
        this.end = end;
        this.endElementId = endElementId;
        this.type = type;
    }

    @Override
    public boolean hasType(String relationshipType) {
        return type().equals(relationshipType);
    }

    /**
     * Modify the start/end identities of this relationship
     */
    public void setStartAndEnd(long start, String startElementId, long end, String endElementId) {
        this.start = start;
        this.startElementId = startElementId;
        this.end = end;
        this.endElementId = endElementId;
    }

    @Override
    @Deprecated
    public long startNodeId() {
        return start;
    }

    @Override
    public String startNodeElementId() {
        return startElementId;
    }

    @Override
    @Deprecated
    public long endNodeId() {
        return end;
    }

    @Override
    public String endNodeElementId() {
        return endElementId;
    }

    @Override
    public String type() {
        return type;
    }

    @Override
    public Value asValue() {
        return new RelationshipValue(this);
    }

    @Override
    @SuppressWarnings("deprecation")
    public String toString() {
        return String.format("relationship<%s>", id());
    }
}
