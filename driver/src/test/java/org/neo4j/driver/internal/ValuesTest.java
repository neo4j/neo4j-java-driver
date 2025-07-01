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

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.driver.Values.isoDuration;
import static org.neo4j.driver.Values.ofDouble;
import static org.neo4j.driver.Values.ofFloat;
import static org.neo4j.driver.Values.ofInteger;
import static org.neo4j.driver.Values.ofList;
import static org.neo4j.driver.Values.ofLong;
import static org.neo4j.driver.Values.ofMap;
import static org.neo4j.driver.Values.ofNumber;
import static org.neo4j.driver.Values.ofObject;
import static org.neo4j.driver.Values.ofString;
import static org.neo4j.driver.Values.ofToString;
import static org.neo4j.driver.Values.point;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.internal.util.ValueFactory.emptyNodeValue;
import static org.neo4j.driver.internal.util.ValueFactory.emptyRelationshipValue;
import static org.neo4j.driver.internal.util.ValueFactory.filledPathValue;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.value.DateTimeValue;
import org.neo4j.driver.internal.value.DateValue;
import org.neo4j.driver.internal.value.DurationValue;
import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.driver.internal.value.LocalDateTimeValue;
import org.neo4j.driver.internal.value.LocalTimeValue;
import org.neo4j.driver.internal.value.MapValue;
import org.neo4j.driver.internal.value.TimeValue;
import org.neo4j.driver.mapping.Vector;
import org.neo4j.driver.types.ByteVector;
import org.neo4j.driver.types.DoubleVector;
import org.neo4j.driver.types.FloatVector;
import org.neo4j.driver.types.IntVector;
import org.neo4j.driver.types.IsoDuration;
import org.neo4j.driver.types.LongVector;
import org.neo4j.driver.types.Point;
import org.neo4j.driver.types.ShortVector;

class ValuesTest {
    @Test
    @SuppressWarnings("RedundantArrayCreation")
    void shouldConvertPrimitiveArrays() {

        assertThat(
                value(new short[] {1, 2, 3}),
                equalTo(new ListValue(Values.value(1, 2, 3).asList(Function.identity()))));

        assertThat(
                value(new int[] {1, 2, 3}),
                equalTo(new ListValue(Values.value(1, 2, 3).asList(Function.identity()))));

        assertThat(
                value(new long[] {1, 2, 3}),
                equalTo(new ListValue(Values.value(1, 2, 3).asList(Function.identity()))));

        assertThat(
                value(new float[] {1.1f, 2.2f, 3.3f}),
                equalTo(new ListValue(Values.value(1.1f, 2.2f, 3.3f).asList(Function.identity()))));

        assertThat(
                value(new double[] {1.1, 2.2, 3.3}),
                equalTo(new ListValue(Values.value(1.1, 2.2, 3.3).asList(Function.identity()))));

        assertThat(
                value(new boolean[] {true, false, true}),
                equalTo(new ListValue(Values.value(true, false, true).asList(Function.identity()))));

        assertThat(
                value(new char[] {'a', 'b', 'c'}),
                equalTo(new ListValue(Values.value('a', 'b', 'c').asList(Function.identity()))));

        assertThat(
                value(new String[] {"a", "b", "c"}),
                equalTo(new ListValue(Values.value("a", "b", "c").asList(Function.identity()))));
    }

    @Test
    void shouldConvertPrimitiveArraysFromObject() {
        assertThat(
                value((Object) new short[] {1, 2, 3}),
                equalTo(new ListValue(Values.value(1, 2, 3).asList(Function.identity()))));

        assertThat(
                value((Object) new int[] {1, 2, 3}),
                equalTo(new ListValue(Values.value(1, 2, 3).asList(Function.identity()))));

        assertThat(
                value((Object) new long[] {1, 2, 3}),
                equalTo(new ListValue(Values.value(1, 2, 3).asList(Function.identity()))));

        assertThat(
                value((Object) new float[] {1.1f, 2.2f, 3.3f}),
                equalTo(new ListValue(Values.value(1.1f, 2.2f, 3.3f).asList(Function.identity()))));

        assertThat(
                value((Object) new double[] {1.1, 2.2, 3.3}),
                equalTo(new ListValue(Values.value(1.1, 2.2, 3.3).asList(Function.identity()))));

        assertThat(
                value((Object) new boolean[] {true, false, true}),
                equalTo(new ListValue(Values.value(true, false, true).asList(Function.identity()))));

        assertThat(
                value((Object) new char[] {'a', 'b', 'c'}),
                equalTo(new ListValue(Values.value('a', 'b', 'c').asList(Function.identity()))));

        assertThat(
                value((Object) new String[] {"a", "b", "c"}),
                equalTo(new ListValue(Values.value("a", "b", "c").asList(Function.identity()))));
    }

    @Test
    void shouldComplainAboutStrangeTypes() {
        var e = assertThrows(ClientException.class, () -> value(new Object()));
        assertEquals("Unable to convert java.lang.Object to Neo4j Value.", e.getMessage());
    }

    @Test
    @SuppressWarnings("EqualsWithItself")
    void equalityRules() {
        assertEquals(value(1), value(1));
        assertEquals(value(Long.MAX_VALUE), value(Long.MAX_VALUE));
        assertEquals(value(Long.MIN_VALUE), value(Long.MIN_VALUE));
        assertNotEquals(value(1), value(2));

        assertEquals(value(1.1337), value(1.1337));
        assertEquals(value(Double.MAX_VALUE), value(Double.MAX_VALUE));
        assertEquals(value(Double.MIN_VALUE), value(Double.MIN_VALUE));

        assertEquals(value(true), value(true));
        assertEquals(value(false), value(false));
        assertNotEquals(value(true), value(false));

        assertEquals(value("Hello"), value("Hello"));
        assertEquals(value("This åäö string ?? contains strange Ü"), value("This åäö string ?? contains strange Ü"));
        assertEquals(value(""), value(""));
        assertNotEquals(value("Hello"), value("hello"));
        assertNotEquals(value("This åäö string ?? contains strange "), value("This åäö string ?? contains strange Ü"));

        assertEquals(value('A'), value('A'));
        assertEquals(value('A'), value("A"));
    }

    @Test
    void shouldMapDriverComplexTypesToListOfJavaPrimitiveTypes() {
        // Given
        Map<String, Value> map = new HashMap<>();
        map.put("Cat", new ListValue(Values.value("meow", "miaow").asList(Function.identity())));
        map.put("Dog", new ListValue(List.of(Values.value("wow"))));
        map.put("Wrong", new ListValue(List.of(Values.value(-1))));
        var mapValue = new MapValue(map);

        // When
        var list = mapValue.values(ofList(ofToString()));

        // Then
        assertEquals(3, mapValue.size());
        var listIterator = list.iterator();
        Set<String> setA = new HashSet<>(3);
        Set<String> setB = new HashSet<>(3);
        for (var value : mapValue.values()) {
            var a = value.get(0).toString();
            var b = listIterator.next().get(0);
            setA.add(a);
            setB.add(b);
        }
        assertThat(setA, equalTo(setB));
    }

    @Test
    void shouldMapDriverMapsToJavaMaps() {
        // Given
        Map<String, Value> map = new HashMap<>();
        map.put("Cat", value(1));
        map.put("Dog", value(2));
        var values = new MapValue(map);

        // When
        var result = values.asMap(ofToString());

        // Then
        assertThat(result.size(), equalTo(2));
        assertThat(result.get("Dog"), equalTo("2"));
        assertThat(result.get("Cat"), equalTo("1"));
    }

    @Test
    void shouldNotBeAbleToGetKeysFromNonKeyedValue() {
        assertThrows(ClientException.class, () -> value("asd").get(1));
    }

    @Test
    void shouldNotBeAbleToDoCrazyCoercions() {
        assertThrows(ClientException.class, () -> value(1).asPath());
    }

    @Test
    void shouldNotBeAbleToGetSizeOnNonSizedValues() {
        assertThrows(ClientException.class, () -> value(1).size());
    }

    @Test
    void shouldMapInteger() {
        // Given
        var val = value(1, 2, 3);

        // When/Then
        assertThat(val.asList(ofInteger()), contains(1, 2, 3));
        assertThat(val.asList(ofLong()), contains(1L, 2L, 3L));
        assertThat(val.asList(ofNumber()), contains(1L, 2L, 3L));
        assertThat(val.asList(ofObject()), contains(1L, 2L, 3L));
    }

    @Test
    void shouldMapFloat() {
        // Given
        var val = value(1.0, 1.2, 3.2);

        // When/Then
        assertThat(val.asList(ofDouble()), contains(1.0, 1.2, 3.2));
        assertThat(val.asList(ofNumber()), contains(1.0, 1.2, 3.2));
        assertThat(val.asList(ofObject()), contains(1.0, 1.2, 3.2));
    }

    @Test
    void shouldMapFloatToJavaFloat() {
        // Given all double -> float conversions other than integers
        //       loose precision, as far as java is concerned, so we
        //       can only convert integer numbers to float.
        var val = value(1.0, 2.0, 3.0);

        // When/Then
        assertThat(val.asList(ofFloat()), contains(1.0F, 2.0F, 3.0F));
    }

    @Test
    void shouldMapString() {
        // Given
        var val = value("hello", "world");

        // When/Then
        assertThat(val.asList(ofString()), contains("hello", "world"));
        assertThat(val.asList(ofObject()), contains("hello", "world"));
    }

    @Test
    void shouldMapMapOfString() {
        // Given
        Map<String, Object> map = new HashMap<>();
        map.put("hello", "world");
        var val = value(asList(map, map));

        // When/Then
        assertThat(val.asList(ofMap()), contains(map, map));
        assertThat(val.asList(ofObject()), contains(map, map));
    }

    @Test
    void shouldHandleCollection() {
        // Given
        Collection<String> collection = new ArrayDeque<>();
        collection.add("hello");
        collection.add("world");
        var val = value(collection);

        // When/Then
        assertThat(val.asList(), Matchers.containsInAnyOrder("hello", "world"));
    }

    @Test
    void shouldHandleIterator() {
        // Given
        var iterator = asList("hello", "world").iterator();
        var val = value(iterator);

        // When/Then
        assertThat(val.asList(), Matchers.containsInAnyOrder("hello", "world"));
    }

    @Test
    void shouldCreateDateValueFromLocalDate() {
        var localDate = LocalDate.now();
        var value = value(localDate);

        assertThat(value, instanceOf(DateValue.class));
        assertEquals(localDate, value.asLocalDate());
    }

    @Test
    void shouldCreateDateValue() {
        Object localDate = LocalDate.now();
        var value = value(localDate);

        assertThat(value, instanceOf(DateValue.class));
        assertEquals(localDate, value.asObject());
    }

    @Test
    void shouldCreateTimeValueFromOffsetTime() {
        var offsetTime = OffsetTime.now();
        var value = value(offsetTime);

        assertThat(value, instanceOf(TimeValue.class));
        assertEquals(offsetTime, value.asOffsetTime());
    }

    @Test
    void shouldCreateTimeValue() {
        var offsetTime = OffsetTime.now();
        var value = value(offsetTime);

        assertThat(value, instanceOf(TimeValue.class));
        assertEquals(offsetTime, value.asObject());
    }

    @Test
    void shouldCreateLocalTimeValueFromLocalTime() {
        var localTime = LocalTime.now();
        var value = value(localTime);

        assertThat(value, instanceOf(LocalTimeValue.class));
        assertEquals(localTime, value.asLocalTime());
    }

    @Test
    void shouldCreateLocalTimeValue() {
        var localTime = LocalTime.now();
        var value = value(localTime);

        assertThat(value, instanceOf(LocalTimeValue.class));
        assertEquals(localTime, value.asObject());
    }

    @Test
    void shouldCreateLocalDateTimeValueFromLocalDateTime() {
        var localDateTime = LocalDateTime.now();
        var value = value(localDateTime);

        assertThat(value, instanceOf(LocalDateTimeValue.class));
        assertEquals(localDateTime, value.asLocalDateTime());
    }

    @Test
    void shouldCreateLocalDateTimeValue() {
        var localDateTime = LocalDateTime.now();
        var value = value(localDateTime);

        assertThat(value, instanceOf(LocalDateTimeValue.class));
        assertEquals(localDateTime, value.asObject());
    }

    @Test
    void shouldCreateDateTimeValueFromOffsetDateTime() {
        var offsetDateTime = OffsetDateTime.now();
        var value = value(offsetDateTime);

        assertThat(value, instanceOf(DateTimeValue.class));
        assertEquals(offsetDateTime, value.asOffsetDateTime());
        assertEquals(offsetDateTime.toZonedDateTime(), value.asZonedDateTime());
        assertEquals(offsetDateTime.toZonedDateTime(), value.asObject());
    }

    @Test
    void shouldCreateDateTimeValueFromZonedDateTime() {
        var zonedDateTime = ZonedDateTime.now();
        var value = value(zonedDateTime);

        assertThat(value, instanceOf(DateTimeValue.class));
        assertEquals(zonedDateTime, value.asZonedDateTime());
    }

    @Test
    void shouldCreateDateTimeValue() {
        var zonedDateTime = ZonedDateTime.now();
        var value = value(zonedDateTime);

        assertThat(value, instanceOf(DateTimeValue.class));
        assertEquals(zonedDateTime, value.asObject());
    }

    @Test
    void shouldCreateIsoDurationValue() {
        var value = isoDuration(42_1, 42_2, 42_3, 42_4);

        assertThat(value, instanceOf(DurationValue.class));
        var duration = value.asIsoDuration();

        assertEquals(42_1, duration.months());
        assertEquals(42_2, duration.days());
        assertEquals(42_3, duration.seconds());
        assertEquals(42_4, duration.nanoseconds());
    }

    @Test
    void shouldCreateValueFromIsoDuration() {
        var durationValue1 = isoDuration(1, 2, 3, 4);
        var duration = durationValue1.asIsoDuration();
        var durationValue2 = value(duration);

        assertEquals(duration, durationValue1.asIsoDuration());
        assertEquals(duration, durationValue2.asIsoDuration());
        assertEquals(durationValue1, durationValue2);
    }

    @Test
    void shouldCreateValueFromPeriod() {
        var period = Period.of(5, 11, 190);

        var value = value(period);
        var isoDuration = value.asIsoDuration();

        assertEquals(period.toTotalMonths(), isoDuration.months());
        assertEquals(period.getDays(), isoDuration.days());
        assertEquals(0, isoDuration.seconds());
        assertEquals(0, isoDuration.nanoseconds());
    }

    @Test
    void shouldCreateValueFromDuration() {
        var duration = Duration.ofSeconds(183951, 4384718937L);

        var value = value(duration);
        var isoDuration = value.asIsoDuration();

        assertEquals(0, isoDuration.months());
        assertEquals(0, isoDuration.days());
        assertEquals(duration.getSeconds(), isoDuration.seconds());
        assertEquals(duration.getNano(), isoDuration.nanoseconds());
    }

    @Test
    void shouldCreateValueFromPoint2D() {
        var point2DValue1 = point(1, 2, 3);
        var point2D = point2DValue1.asPoint();
        var point2DValue2 = value(point2D);

        assertEquals(point2D, point2DValue1.asPoint());
        assertEquals(point2D, point2DValue2.asPoint());
        assertEquals(point2DValue1, point2DValue2);
    }

    @Test
    void shouldCreateValueFromPoint3D() {
        var point3DValue1 = point(1, 2, 3, 4);
        var point3D = point3DValue1.asPoint();
        var point3DValue2 = value(point3D);

        assertEquals(point3D, point3DValue1.asPoint());
        assertEquals(point3D, point3DValue2.asPoint());
        assertEquals(point3DValue1, point3DValue2);
    }

    @Test
    void shouldCreateValueFromNodeValue() {
        var node = emptyNodeValue();
        var value = value(node);
        assertEquals(node, value);
    }

    @Test
    void shouldCreateValueFromNode() {
        var node = emptyNodeValue().asNode();
        var value = value(node);
        assertEquals(node, value.asNode());
    }

    @Test
    void shouldCreateValueFromRelationshipValue() {
        var rel = emptyRelationshipValue();
        var value = value(rel);
        assertEquals(rel, value);
    }

    @Test
    void shouldCreateValueFromRelationship() {
        var rel = emptyRelationshipValue().asRelationship();
        var value = value(rel);
        assertEquals(rel, value.asRelationship());
    }

    @Test
    void shouldCreateValueFromPathValue() {
        var path = filledPathValue();
        var value = value(path);
        assertEquals(path, value);
    }

    @Test
    void shouldCreateValueFromPath() {
        var path = filledPathValue().asPath();
        var value = value(path);
        assertEquals(path, value.asPath());
    }

    @Test
    void shouldCreateValueFromStream() {
        var stream = Stream.of("foo", "bar", "baz", "qux");
        var value = value(stream);
        assertEquals(asList("foo", "bar", "baz", "qux"), value.asObject());
    }

    @Test
    void shouldFailToConvertStreamOfUnsupportedTypeToValue() {
        var stream = Stream.of(new Object(), new Object());
        var e = assertThrows(ClientException.class, () -> value(stream));
        assertEquals("Unable to convert java.lang.Object to Neo4j Value.", e.getMessage());
    }

    @Test
    void shouldCreateValueFromStreamOfStreams() {
        var stream = Stream.of(Stream.of("foo", "bar"), Stream.of("baz", "qux"));
        var value = value(stream);
        assertEquals(asList(asList("foo", "bar"), asList("baz", "qux")), value.asObject());
    }

    @Test
    void shouldCreateValueFromStreamOfNulls() {
        var stream = Stream.of(null, null, null);
        var value = value(stream);
        assertEquals(asList(null, null, null), value.asObject());
    }

    @Test
    void shouldMapJavaRecordToMap() {
        // given
        var string = "string";
        var listWithString = List.of(string, string);
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
        var point2d = new InternalPoint2D(0, 0, 0);
        var point3d = new InternalPoint3D(0, 0, 0, 0);
        var byteVector = new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        var byteVectorObject = Values.vector(byteVector).as(ByteVector.class);
        var shortVector = new short[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        var shortVectorObject = Values.vector(shortVector).as(ShortVector.class);
        var intVector = new int[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        var intVectorObject = Values.vector(intVector).as(IntVector.class);
        var longVector = new long[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        var longVectorObject = Values.vector(longVector).as(LongVector.class);
        var floatVector = new float[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        var floatVectorObject = Values.vector(floatVector).as(FloatVector.class);
        var doubleVector = new double[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        var doubleVectorObject = Values.vector(doubleVector).as(DoubleVector.class);
        var valueHolder = new ValueHolder(
                string,
                null,
                listWithString,
                bytes,
                bool,
                boltInteger,
                boltFloat,
                date,
                time,
                dateTime,
                localDateTime,
                duration,
                period,
                javaDuration,
                point2d,
                point3d,
                byteVector,
                byteVectorObject,
                shortVector,
                shortVectorObject,
                intVector,
                intVectorObject,
                longVector,
                longVectorObject,
                floatVector,
                floatVectorObject,
                doubleVector,
                doubleVectorObject);

        // when
        var mapValue = Values.value(valueHolder);

        // then
        assertEquals(27, mapValue.size());
        assertEquals(string, mapValue.get("string").as(String.class));
        assertFalse(mapValue.containsKey("nullValue"));
        assertEquals(listWithString, mapValue.get("listWithString").as(List.class));
        assertArrayEquals(bytes, mapValue.get("bytes").as(byte[].class));
        assertEquals(bool, mapValue.get("bool").as(boolean.class));
        assertEquals(boltInteger, mapValue.get("boltInteger").as(long.class));
        assertEquals(boltFloat, mapValue.get("boltFloat").as(double.class));
        assertEquals(date, mapValue.get("date").as(LocalDate.class));
        assertEquals(time, mapValue.get("time").as(OffsetTime.class));
        assertEquals(dateTime, mapValue.get("dateTime").as(ZonedDateTime.class));
        assertEquals(localDateTime, mapValue.get("localDateTime").as(LocalDateTime.class));
        assertEquals(duration, mapValue.get("duration").as(IsoDuration.class));
        assertEquals(period, mapValue.get("period").as(Period.class));
        assertEquals(javaDuration, mapValue.get("javaDuration").as(Duration.class));
        assertEquals(point2d, mapValue.get("point2d").as(Point.class));
        assertEquals(point3d, mapValue.get("point3d").as(Point.class));
        assertArrayEquals(
                byteVector, mapValue.get("byteVector").as(ByteVector.class).toArray());
        assertArrayEquals(
                byteVector,
                mapValue.get("byteVectorObject").as(ByteVector.class).toArray());
        assertArrayEquals(
                shortVector, mapValue.get("shortVector").as(ShortVector.class).toArray());
        assertArrayEquals(
                shortVector,
                mapValue.get("shortVectorObject").as(ShortVector.class).toArray());
        assertArrayEquals(
                intVector, mapValue.get("intVector").as(IntVector.class).toArray());
        assertArrayEquals(
                intVector, mapValue.get("intVectorObject").as(IntVector.class).toArray());
        assertArrayEquals(
                longVector, mapValue.get("longVector").as(LongVector.class).toArray());
        assertArrayEquals(
                longVector,
                mapValue.get("longVectorObject").as(LongVector.class).toArray());
        assertArrayEquals(
                floatVector, mapValue.get("floatVector").as(FloatVector.class).toArray());
        assertArrayEquals(
                floatVector,
                mapValue.get("floatVectorObject").as(FloatVector.class).toArray());
        assertArrayEquals(
                doubleVector,
                mapValue.get("doubleVector").as(DoubleVector.class).toArray());
        assertArrayEquals(
                doubleVector,
                mapValue.get("doubleVectorObject").as(DoubleVector.class).toArray());
        assertEquals(valueHolder, mapValue.as(ValueHolder.class));
    }

    public record ValueHolder(
            String string,
            Object nullValue,
            List<String> listWithString,
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
            @Vector byte[] byteVector,
            ByteVector byteVectorObject,
            @Vector short[] shortVector,
            ShortVector shortVectorObject,
            @Vector int[] intVector,
            IntVector intVectorObject,
            @Vector long[] longVector,
            LongVector longVectorObject,
            @Vector float[] floatVector,
            FloatVector floatVectorObject,
            @Vector double[] doubleVector,
            DoubleVector doubleVectorObject) {
        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            ValueHolder that = (ValueHolder) o;
            return bool == that.bool
                    && boltInteger == that.boltInteger
                    && Double.compare(boltFloat, that.boltFloat) == 0
                    && Objects.deepEquals(bytes, that.bytes)
                    && Objects.equals(string, that.string)
                    && Objects.equals(period, that.period)
                    && Objects.equals(point2d, that.point2d)
                    && Objects.equals(point3d, that.point3d)
                    && Objects.equals(date, that.date)
                    && Objects.equals(time, that.time)
                    && Objects.deepEquals(intVector, that.intVector)
                    && Objects.equals(nullValue, that.nullValue)
                    && Objects.deepEquals(byteVector, that.byteVector)
                    && Objects.deepEquals(longVector, that.longVector)
                    && Objects.deepEquals(shortVector, that.shortVector)
                    && Objects.deepEquals(floatVector, that.floatVector)
                    && Objects.equals(duration, that.duration)
                    && Objects.equals(javaDuration, that.javaDuration)
                    && Objects.deepEquals(doubleVector, that.doubleVector)
                    && Objects.equals(dateTime, that.dateTime)
                    && Objects.equals(intVectorObject, that.intVectorObject)
                    && Objects.equals(listWithString, that.listWithString)
                    && Objects.equals(localDateTime, that.localDateTime)
                    && Objects.equals(byteVectorObject, that.byteVectorObject)
                    && Objects.equals(longVectorObject, that.longVectorObject)
                    && Objects.equals(shortVectorObject, that.shortVectorObject)
                    && Objects.equals(floatVectorObject, that.floatVectorObject)
                    && Objects.equals(doubleVectorObject, that.doubleVectorObject);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    string,
                    nullValue,
                    listWithString,
                    Arrays.hashCode(bytes),
                    bool,
                    boltInteger,
                    boltFloat,
                    date,
                    time,
                    dateTime,
                    localDateTime,
                    duration,
                    period,
                    javaDuration,
                    point2d,
                    point3d,
                    Arrays.hashCode(byteVector),
                    byteVectorObject,
                    Arrays.hashCode(shortVector),
                    shortVectorObject,
                    Arrays.hashCode(intVector),
                    intVectorObject,
                    Arrays.hashCode(longVector),
                    longVectorObject,
                    Arrays.hashCode(floatVector),
                    floatVectorObject,
                    Arrays.hashCode(doubleVector),
                    doubleVectorObject);
        }
    }
}
