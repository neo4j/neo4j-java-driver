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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import org.neo4j.bolt.connection.values.IsoDuration;
import org.neo4j.bolt.connection.values.Point;
import org.neo4j.bolt.connection.values.Type;
import org.neo4j.bolt.connection.values.Value;
import org.neo4j.driver.exceptions.value.Uncoercible;
import org.neo4j.driver.internal.InternalIsoDuration;
import org.neo4j.driver.internal.InternalPoint2D;
import org.neo4j.driver.internal.InternalPoint3D;

public class BoltValue implements org.neo4j.bolt.connection.values.Value {
    private final InternalValue value;
    private final Type type;

    public BoltValue(InternalValue value, Type type) {
        this.value = Objects.requireNonNull(value);
        this.type = Objects.requireNonNull(type);
    }

    public org.neo4j.driver.Value asDriverValue() {
        return value;
    }

    @Override
    public Type type() {
        return type;
    }

    @Override
    public boolean asBoolean() {
        return value.asBoolean();
    }

    @Override
    public byte[] asByteArray() {
        return value.asByteArray();
    }

    @Override
    public String asString() {
        return value.asString();
    }

    @Override
    public long asLong() {
        return value.asLong();
    }

    @Override
    public double asDouble() {
        return value.asDouble();
    }

    @Override
    public LocalDate asLocalDate() {
        return value.asLocalDate();
    }

    @Override
    public OffsetTime asOffsetTime() {
        return value.asOffsetTime();
    }

    @Override
    public LocalTime asLocalTime() {
        return value.asLocalTime();
    }

    @Override
    public LocalDateTime asLocalDateTime() {
        return value.asLocalDateTime();
    }

    @Override
    public ZonedDateTime asZonedDateTime() {
        return value.asZonedDateTime();
    }

    @Override
    public IsoDuration asIsoDuration() {
        return (InternalIsoDuration) value.asIsoDuration();
    }

    @Override
    public Point asPoint() {
        var point = value.asPoint();
        if (point instanceof InternalPoint2D internalPoint2D) {
            return internalPoint2D;
        } else if (point instanceof InternalPoint3D internalPoint3D) {
            return internalPoint3D;
        }
        throw new Uncoercible(value.type().name(), "Bolt IsoDuration");
    }

    @Override
    public boolean isNull() {
        return value.isNull();
    }

    @Override
    public boolean isEmpty() {
        return value.isEmpty();
    }

    @Override
    public Iterable<String> keys() {
        return value.keys();
    }

    @Override
    public int size() {
        return value.size();
    }

    @Override
    public org.neo4j.bolt.connection.values.Value get(String key) {
        return ((InternalValue) value.get(key)).asBoltValue();
    }

    @Override
    public Iterable<org.neo4j.bolt.connection.values.Value> values() {
        return () -> new Iterator<>() {
            private final Iterator<org.neo4j.driver.Value> iterator =
                    value.values().iterator();

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Value next() {
                return ((InternalValue) iterator.next()).asBoltValue();
            }
        };
    }

    @Override
    public boolean containsKey(String key) {
        return value.containsKey(key);
    }

    @Override
    public <T> Map<String, T> asMap(Function<org.neo4j.bolt.connection.values.Value, T> mapFunction) {
        return value.asMap(v -> mapFunction.apply(((InternalValue) v).asBoltValue()));
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        var boltValue = (BoltValue) o;
        return Objects.equals(value, boltValue.value) && type == boltValue.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, type);
    }

    @Override
    public String toString() {
        return value.toString();
    }
}
