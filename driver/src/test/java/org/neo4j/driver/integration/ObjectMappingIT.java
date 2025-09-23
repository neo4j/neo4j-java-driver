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
package org.neo4j.driver.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.exceptions.value.ValueException;
import org.neo4j.driver.internal.InternalFloat64Vector;
import org.neo4j.driver.internal.InternalIsoDuration;
import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.internal.InternalPoint2D;
import org.neo4j.driver.internal.InternalPoint3D;
import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.InternalRelationship;
import org.neo4j.driver.internal.InternalUnsupportedType;
import org.neo4j.driver.internal.value.NodeValue;
import org.neo4j.driver.internal.value.RelationshipValue;
import org.neo4j.driver.mapping.Property;
import org.neo4j.driver.types.Float64Vector;
import org.neo4j.driver.types.IsoDuration;
import org.neo4j.driver.types.Point;
import org.neo4j.driver.types.UnsupportedType;

final class ObjectMappingIT {
    @ParameterizedTest
    @MethodSource("shouldMapValueArgs")
    void shouldMapValue(Function<Map<String, Value>, ValueHolder> valueFunction) {
        // given
        var string = "string";
        var listWithString = List.of(string, string);
        var listWithStringValueHolder = List.of(new StringValueHolder(string), new StringValueHolder(string));
        var bytes = new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        var bool = false;
        var boltInteger = Long.MIN_VALUE;
        var boltFloat = Double.MIN_VALUE;
        var date = LocalDate.now();
        var time = OffsetTime.now();
        var dateTime = ZonedDateTime.now();
        var localDateTime = LocalDateTime.now();
        var duration = new InternalIsoDuration(Duration.ZERO);
        var period = Period.ofYears(1000);
        var javaDuration = Duration.of(1000, ChronoUnit.MINUTES);
        var point2d = (Point) new InternalPoint2D(0, 0, 0);
        var point3d = (Point) new InternalPoint3D(0, 0, 0, 0);
        var vector = new InternalFloat64Vector(new double[] {0.0, 100.0});
        var unsupportedType = new InternalUnsupportedType("name", "99.99", "message");

        var properties = Map.ofEntries(
                Map.entry("string", Values.value(string)),
                Map.entry("nullValue", Values.value((Object) null)),
                Map.entry("listWithString", Values.value(listWithString)),
                // also verifies MapValue
                Map.entry(
                        "listWithStringValueHolder",
                        Values.value(listWithStringValueHolder.stream()
                                .map(v -> Map.of("string", Values.value(v.string())))
                                .toList())),
                Map.entry("bytes", Values.value(bytes)),
                Map.entry("bool", Values.value(bool)),
                Map.entry("boltInteger", Values.value(boltInteger)),
                Map.entry("boltFloat", Values.value(boltFloat)),
                Map.entry("date", Values.value(date)),
                Map.entry("time", Values.value(time)),
                Map.entry("dateTime", Values.value(dateTime)),
                Map.entry("localDateTime", Values.value(localDateTime)),
                Map.entry("duration", Values.value(duration)),
                Map.entry("period", Values.value(period)),
                Map.entry("javaDuration", Values.value(javaDuration)),
                Map.entry("point2d", Values.value(point2d)),
                Map.entry("point3d", Values.value(point3d)),
                Map.entry("vector", Values.value(vector)),
                Map.entry("unsupportedType", Values.value(unsupportedType)));

        // when
        var valueHolder = valueFunction.apply(properties);

        // then
        assertEquals(string, valueHolder.string());
        assertNull(valueHolder.nullValue());
        assertEquals(listWithString, valueHolder.listWithString());
        assertEquals(listWithStringValueHolder, valueHolder.listWithStringValueHolder());
        assertEquals(bytes, valueHolder.bytes());
        assertEquals(bool, valueHolder.bool());
        assertEquals(boltInteger, valueHolder.boltInteger());
        assertEquals(boltFloat, valueHolder.boltFloat());
        assertEquals(date, valueHolder.date());
        assertEquals(time, valueHolder.time());
        assertEquals(dateTime, valueHolder.dateTime());
        assertEquals(localDateTime, valueHolder.localDateTime());
        assertEquals(duration, valueHolder.duration());
        assertEquals(period, valueHolder.period());
        assertEquals(javaDuration, valueHolder.javaDuration());
        assertEquals(point2d, valueHolder.point2d());
        assertEquals(point3d, valueHolder.point3d());
        assertEquals(vector, valueHolder.vector());
        assertEquals(unsupportedType, valueHolder.unsupportedType());
    }

    static Stream<Arguments> shouldMapValueArgs() {
        return Stream.of(
                Arguments.of(Named.<Function<Map<String, Value>, ValueHolder>>of(
                        "node", properties -> new NodeValue(new InternalNode(0L, Set.of("Product"), properties))
                                .as(ValueHolder.class))),
                Arguments.of(Named.<Function<Map<String, Value>, ValueHolder>>of(
                        "relationship",
                        properties -> new RelationshipValue(new InternalRelationship(0L, 0L, 0L, "Product", properties))
                                .as(ValueHolder.class))),
                Arguments.of(Named.<Function<Map<String, Value>, ValueHolder>>of(
                        "map", properties -> Values.value(properties).as(ValueHolder.class))),
                Arguments.of(Named.<Function<Map<String, Value>, ValueHolder>>of("record", properties -> {
                    var keys = new ArrayList<String>();
                    var values = new ArrayList<Value>();
                    for (var entry : properties.entrySet()) {
                        keys.add(entry.getKey());
                        values.add(entry.getValue());
                    }
                    return new InternalRecord(keys, values).as(ValueHolder.class);
                })));
    }

    public record ValueHolder(
            String string,
            Object nullValue,
            List<String> listWithString,
            List<StringValueHolder> listWithStringValueHolder,
            byte[] bytes,
            boolean bool,
            long boltInteger,
            double boltFloat,
            LocalDate date,
            OffsetTime time,
            ZonedDateTime dateTime,
            LocalDateTime localDateTime,
            IsoDuration duration,
            Period period,
            Duration javaDuration,
            Point point2d,
            Point point3d,
            Float64Vector vector,
            UnsupportedType unsupportedType) {}

    public record StringValueHolder(String string) {}

    @Test
    void shouldUseConstructorWithMaxMatchesAndMinMismatches() {
        // given
        var string = "string";
        var bytes = new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        var bool = false;

        var properties = Map.ofEntries(
                Map.entry("string", Values.value(string)),
                Map.entry("bytes", Values.value(bytes)),
                Map.entry("bool", Values.value(bool)));

        // when
        var valueHolder = Values.value(properties).as(ValueHolderWithOptionalNumber.class);

        // then
        assertEquals(string, valueHolder.string());
        assertEquals(bytes, valueHolder.bytes());
        assertEquals(bool, valueHolder.bool());
        assertEquals(Long.MIN_VALUE, valueHolder.number());
    }

    public record ValueHolderWithOptionalNumber(String string, byte[] bytes, boolean bool, long number) {
        public ValueHolderWithOptionalNumber(
                @Property("string") String string, @Property("bytes") byte[] bytes, @Property("bool") boolean bool) {
            this(string, bytes, bool, Long.MIN_VALUE);
        }
    }

    @Test
    void shouldWorkWithLocalRecord() {
        // given
        var string = "string";
        var bool = false;

        var properties =
                Map.ofEntries(Map.entry("string", Values.value(string)), Map.entry("bool", Values.value(bool)));

        record LocalRecord(String string, boolean bool) {}

        // when
        var valueHolder = Values.value(properties).as(LocalRecord.class);

        // then
        assertEquals(string, valueHolder.string());
        assertEquals(bool, valueHolder.bool());
    }

    @Test
    void shouldAcceptLocalRecordAsValue() {
        // given
        var string = "string";
        var bool = false;
        record LocalRecord(String string, boolean bool) {}
        var recordValue = new LocalRecord(string, bool);

        // when
        var value = Values.value(recordValue);

        // then
        assertEquals(string, value.get("string").asString());
        assertEquals(bool, value.get("bool").asBoolean());
    }

    @Test
    void shouldWorkWithPrivateRecord() {
        // given
        var string = "string";
        var bool = false;

        var properties =
                Map.ofEntries(Map.entry("string", Values.value(string)), Map.entry("bool", Values.value(bool)));

        // when
        var valueHolder = Values.value(properties).as(PrivateRecord.class);

        // then
        assertEquals(string, valueHolder.string());
        assertEquals(bool, valueHolder.bool());
    }

    @Test
    void shouldAcceptPrivateRecordAsValue() {
        // given
        var string = "string";
        var bool = false;
        var recordValue = new PrivateRecord(string, bool);

        // when
        var value = Values.value(recordValue);

        // then
        assertEquals(string, value.get("string").asString());
        assertEquals(bool, value.get("bool").asBoolean());
    }

    private record PrivateRecord(String string, boolean bool) {}

    @Test
    void shouldSelectAccessibleOnIdenticalMatch() {
        // given
        var string = "string";
        var bool = false;
        var number = 0;

        var properties = Map.ofEntries(
                Map.entry("string", Values.value(string)),
                Map.entry("bool", Values.value(bool)),
                Map.entry("number", Values.value(number)));

        // when
        var valueHolder = Values.value(properties).as(IdenticalMatch.class);

        // then
        assertEquals(string, valueHolder.string);
        assertEquals(number, valueHolder.number);
    }

    public static class IdenticalMatch {
        String string;
        boolean bool;
        long number;

        private IdenticalMatch(@Property("string") String string, @Property("bool") boolean bool) {
            this.string = string;
            this.bool = bool;
        }

        public IdenticalMatch(@Property("string") String string, @Property("number") int number) {
            this.string = string;
            this.number = number;
        }
    }

    @Test
    void shouldFindNoMatch() {
        // given
        var properties = Map.ofEntries(Map.entry("value", Values.value("value")));

        // when & then
        var exception = assertThrows(
                ValueException.class, () -> Values.value(properties).as(ValueHolder.class));
    }
}
