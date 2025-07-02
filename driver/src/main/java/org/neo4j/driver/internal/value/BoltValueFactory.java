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
package org.neo4j.driver.internal.value;

import java.time.DateTimeException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.neo4j.bolt.connection.values.Node;
import org.neo4j.bolt.connection.values.Path;
import org.neo4j.bolt.connection.values.Relationship;
import org.neo4j.bolt.connection.values.Segment;
import org.neo4j.bolt.connection.values.Value;
import org.neo4j.bolt.connection.values.ValueFactory;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.internal.InternalPath;
import org.neo4j.driver.internal.InternalRelationship;

public class BoltValueFactory implements ValueFactory {
    private static final BoltValueFactory INSTANCE = new BoltValueFactory();

    public static BoltValueFactory getInstance() {
        return INSTANCE;
    }

    private BoltValueFactory() {}

    @Override
    public Value value(Object value) {
        return ((InternalValue) Values.value(value));
    }

    @Override
    public Value value(boolean value) {
        return ((InternalValue) Values.value(value));
    }

    @Override
    public Value value(long value) {
        return ((InternalValue) Values.value(value));
    }

    @Override
    public Value value(double value) {
        return ((InternalValue) Values.value(value));
    }

    @Override
    public Value value(byte[] values) {
        if (values == null) {
            return value((Object) null);
        }
        return ((InternalValue) Values.value(values));
    }

    @Override
    public Value value(String value) {
        if (value == null) {
            return value((Object) null);
        }
        return ((InternalValue) Values.value(value));
    }

    @SuppressWarnings("unchecked")
    @Override
    public Value value(Map<String, Value> stringToValue) {
        if (stringToValue == null) {
            return value((Object) null);
        }
        return new MapValue((Map<String, org.neo4j.driver.Value>) (Map<?, ?>) stringToValue);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Value value(List<Value> values) {
        if (values == null) {
            return value((Object) null);
        }
        return new ListValue((List<org.neo4j.driver.Value>) (List<?>) values);
    }

    @Override
    public Value value(Node node) {
        if (node == null) {
            return value((Object) null);
        }
        return (InternalValue) ((InternalNode) node).asValue();
    }

    @Override
    public Value value(Relationship relationship) {
        if (relationship == null) {
            return value((Object) null);
        }
        return (InternalValue) ((InternalRelationship) relationship).asValue();
    }

    @Override
    public Value value(Path path) {
        if (path == null) {
            return value((Object) null);
        }
        return (InternalValue) ((InternalPath) path).asValue();
    }

    @Override
    public Value value(LocalDate localDate) {
        if (localDate == null) {
            return value((Object) null);
        }
        return ((InternalValue) Values.value(localDate));
    }

    @Override
    public Value value(OffsetTime offsetTime) {
        if (offsetTime == null) {
            return value((Object) null);
        }
        return ((InternalValue) Values.value(offsetTime));
    }

    @Override
    public Value value(LocalTime localTime) {
        if (localTime == null) {
            return value((Object) null);
        }
        return ((InternalValue) Values.value(localTime));
    }

    @Override
    public Value value(LocalDateTime localDateTime) {
        if (localDateTime == null) {
            return value((Object) null);
        }
        return ((InternalValue) Values.value(localDateTime));
    }

    @Override
    public Value value(OffsetDateTime offsetDateTime) {
        if (offsetDateTime == null) {
            return value((Object) null);
        }
        return ((InternalValue) Values.value(offsetDateTime));
    }

    @Override
    public Value value(ZonedDateTime zonedDateTime) {
        if (zonedDateTime == null) {
            return value((Object) null);
        }
        return ((InternalValue) Values.value(zonedDateTime));
    }

    @Override
    public Value value(Period period) {
        if (period == null) {
            return value((Object) null);
        }
        return ((InternalValue) Values.value(period));
    }

    @Override
    public Value value(Duration duration) {
        if (duration == null) {
            return value((Object) null);
        }
        return ((InternalValue) Values.value(duration));
    }

    @Override
    public Node node(long id, String elementId, Collection<String> labels, Map<String, Value> properties) {
        return new InternalNode(id, elementId, labels, toDriverMap(properties));
    }

    @Override
    public Relationship relationship(
            long id,
            String elementId,
            long start,
            String startElementId,
            long end,
            String endElementId,
            String type,
            Map<String, Value> properties) {
        return new InternalRelationship(
                id, elementId, start, startElementId, end, endElementId, type, toDriverMap(properties));
    }

    @Override
    public Segment segment(Node start, Relationship relationship, Node end) {
        return new InternalPath.SelfContainedSegment(
                (InternalNode) start, (InternalRelationship) relationship, (InternalNode) end);
    }

    @Override
    public Path path(List<Segment> segments, List<Node> nodes, List<Relationship> relationships) {
        var segments0 = segments.stream()
                .map(segment -> (org.neo4j.driver.types.Path.Segment) segment)
                .toList();
        var nodes0 =
                nodes.stream().map(node -> (org.neo4j.driver.types.Node) node).toList();
        var relationships0 = relationships.stream()
                .map(relationship -> (org.neo4j.driver.types.Relationship) relationship)
                .toList();
        return new InternalPath(segments0, nodes0, relationships0);
    }

    @Override
    public Value isoDuration(long months, long days, long seconds, int nanoseconds) {
        return ((InternalValue) Values.isoDuration(months, days, seconds, nanoseconds));
    }

    @Override
    public Value point(int srid, double x, double y) {
        return ((InternalValue) Values.point(srid, x, y));
    }

    @Override
    public Value point(int srid, double x, double y, double z) {
        return ((InternalValue) Values.point(srid, x, y, z));
    }

    @Override
    public Value vector(Class<?> elementType, Object elements) {
        throw new UnsupportedOperationException("Vector is not supported");
    }

    @Override
    public Value unsupportedDateTimeValue(DateTimeException e) {
        return new UnsupportedDateTimeValue(e);
    }

    @SuppressWarnings("unchecked")
    public Map<String, Value> toBoltMap(Map<String, org.neo4j.driver.Value> map) {
        return Collections.unmodifiableMap((Map<String, Value>) (Map<?, ?>) map);
    }

    @SuppressWarnings("unchecked")
    public Map<String, org.neo4j.driver.Value> toDriverMap(Map<String, Value> map) {
        return Collections.unmodifiableMap((Map<String, org.neo4j.driver.Value>) (Map<?, ?>) map);
    }

    @SuppressWarnings("unchecked")
    public List<org.neo4j.driver.Value> toDriverList(List<Value> list) {
        return (List<org.neo4j.driver.Value>) (List<?>) list;
    }
}
