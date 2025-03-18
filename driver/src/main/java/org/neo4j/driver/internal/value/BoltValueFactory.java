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
import java.util.HashMap;
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
        return ((InternalValue) Values.value(value)).asBoltValue();
    }

    @Override
    public Value value(boolean value) {
        return ((InternalValue) Values.value(value)).asBoltValue();
    }

    @Override
    public Value value(long value) {
        return ((InternalValue) Values.value(value)).asBoltValue();
    }

    @Override
    public Value value(double value) {
        return ((InternalValue) Values.value(value)).asBoltValue();
    }

    @Override
    public Value value(byte[] values) {
        if (values == null) {
            return value((Object) null);
        }
        return ((InternalValue) Values.value(values)).asBoltValue();
    }

    @Override
    public Value value(String value) {
        if (value == null) {
            return value((Object) null);
        }
        return ((InternalValue) Values.value(value)).asBoltValue();
    }

    @Override
    public Value value(Map<String, Value> stringToValue) {
        if (stringToValue == null) {
            return value((Object) null);
        }
        return ((InternalValue) Values.value(stringToValue)).asBoltValue();
    }

    @Override
    public Value value(Value[] values) {
        if (values == null) {
            return value((Object) null);
        }
        return ((InternalValue) Values.value(values)).asBoltValue();
    }

    @Override
    public Value value(Node node) {
        if (node == null) {
            return value((Object) null);
        }
        return ((InternalValue) Values.value(node)).asBoltValue();
    }

    @Override
    public Value value(Relationship relationship) {
        if (relationship == null) {
            return value((Object) null);
        }
        return ((InternalValue) Values.value(relationship)).asBoltValue();
    }

    @Override
    public Value value(Path path) {
        if (path == null) {
            return value((Object) null);
        }
        return ((InternalValue) Values.value(path)).asBoltValue();
    }

    @Override
    public Value value(LocalDate localDate) {
        if (localDate == null) {
            return value((Object) null);
        }
        return ((InternalValue) Values.value(localDate)).asBoltValue();
    }

    @Override
    public Value value(OffsetTime offsetTime) {
        if (offsetTime == null) {
            return value((Object) null);
        }
        return ((InternalValue) Values.value(offsetTime)).asBoltValue();
    }

    @Override
    public Value value(LocalTime localTime) {
        if (localTime == null) {
            return value((Object) null);
        }
        return ((InternalValue) Values.value(localTime)).asBoltValue();
    }

    @Override
    public Value value(LocalDateTime localDateTime) {
        if (localDateTime == null) {
            return value((Object) null);
        }
        return ((InternalValue) Values.value(localDateTime)).asBoltValue();
    }

    @Override
    public Value value(OffsetDateTime offsetDateTime) {
        if (offsetDateTime == null) {
            return value((Object) null);
        }
        return ((InternalValue) Values.value(offsetDateTime)).asBoltValue();
    }

    @Override
    public Value value(ZonedDateTime zonedDateTime) {
        if (zonedDateTime == null) {
            return value((Object) null);
        }
        return ((InternalValue) Values.value(zonedDateTime)).asBoltValue();
    }

    @Override
    public Value value(Period period) {
        if (period == null) {
            return value((Object) null);
        }
        return ((InternalValue) Values.value(period)).asBoltValue();
    }

    @Override
    public Value value(Duration duration) {
        if (duration == null) {
            return value((Object) null);
        }
        return ((InternalValue) Values.value(duration)).asBoltValue();
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
        return ((InternalValue) Values.isoDuration(months, days, seconds, nanoseconds)).asBoltValue();
    }

    @Override
    public Value point(int srid, double x, double y) {
        return ((InternalValue) Values.point(srid, x, y)).asBoltValue();
    }

    @Override
    public Value point(int srid, double x, double y, double z) {
        return ((InternalValue) Values.point(srid, x, y, z)).asBoltValue();
    }

    @Override
    public Value unsupportedDateTimeValue(DateTimeException e) {
        return new UnsupportedDateTimeValue(e).asBoltValue();
    }

    public Map<String, Value> toBoltMap(Map<String, org.neo4j.driver.Value> map) {
        var result = new HashMap<String, Value>(map.size());
        for (var entry : map.entrySet()) {
            var boltValue = ((InternalValue) entry.getValue()).asBoltValue();
            result.put(entry.getKey(), boltValue);
        }
        return Collections.unmodifiableMap(result);
    }

    public Map<String, org.neo4j.driver.Value> toDriverMap(Map<String, Value> map) {
        var result = new HashMap<String, org.neo4j.driver.Value>(map.size());
        for (var entry : map.entrySet()) {
            var boltValue = ((BoltValue) entry.getValue()).asDriverValue();
            result.put(entry.getKey(), boltValue);
        }
        return Collections.unmodifiableMap(result);
    }
}
