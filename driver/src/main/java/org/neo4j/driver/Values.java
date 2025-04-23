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
package org.neo4j.driver;

import static org.neo4j.driver.internal.util.Extract.assertParameter;
import static org.neo4j.driver.internal.util.Iterables.newHashMapWithSize;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.AsValue;
import org.neo4j.driver.internal.GqlStatusError;
import org.neo4j.driver.internal.InternalIsoDuration;
import org.neo4j.driver.internal.InternalPoint2D;
import org.neo4j.driver.internal.InternalPoint3D;
import org.neo4j.driver.internal.value.BoltValue;
import org.neo4j.driver.internal.value.BooleanValue;
import org.neo4j.driver.internal.value.BytesValue;
import org.neo4j.driver.internal.value.DateTimeValue;
import org.neo4j.driver.internal.value.DateValue;
import org.neo4j.driver.internal.value.DurationValue;
import org.neo4j.driver.internal.value.FloatValue;
import org.neo4j.driver.internal.value.IntegerValue;
import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.driver.internal.value.LocalDateTimeValue;
import org.neo4j.driver.internal.value.LocalTimeValue;
import org.neo4j.driver.internal.value.MapValue;
import org.neo4j.driver.internal.value.NullValue;
import org.neo4j.driver.internal.value.PointValue;
import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.driver.internal.value.TimeValue;
import org.neo4j.driver.mapping.Property;
import org.neo4j.driver.types.Entity;
import org.neo4j.driver.types.IsoDuration;
import org.neo4j.driver.types.MapAccessor;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Path;
import org.neo4j.driver.types.Point;
import org.neo4j.driver.types.Relationship;
import org.neo4j.driver.types.TypeSystem;
import org.neo4j.driver.util.Preview;

/**
 * Utility for wrapping regular Java types and exposing them as {@link Value}
 * objects, and vice versa.
 * <p>
 * The long set of {@code ofXXX} methods in this class are meant to be used as
 * arguments for methods like {@link Value#asList(Function)}, {@link Value#asMap(Function)},
 * {@link Record#asMap(Function)} and so on.
 *
 * @since 1.0
 */
public final class Values {
    /**
     * The value instance of an empty map.
     */
    public static final Value EmptyMap = value(Collections.emptyMap());
    /**
     * The value instance of {@code NULL}.
     */
    public static final Value NULL = NullValue.NULL;

    private Values() {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns a value from object.
     * <p>
     * <b>Note that mapping from {@link java.lang.Record java.lang.Record} is in {@link Preview} status and should not
     * be assumed to be in GA status.</b>
     *
     * @param value the object value
     * @return the array of values
     */
    @SuppressWarnings("unchecked")
    public static Value value(Object value) {
        if (value == null) {
            return NullValue.NULL;
        }

        if (value instanceof BoltValue boltValue) {
            return boltValue.asDriverValue();
        }
        if (value instanceof AsValue) {
            return ((AsValue) value).asValue();
        }
        if (value instanceof Boolean) {
            return value((boolean) value);
        }
        if (value instanceof String) {
            return value((String) value);
        }
        if (value instanceof Character) {
            return value((char) value);
        }
        if (value instanceof Long) {
            return value((long) value);
        }
        if (value instanceof Short) {
            return value((short) value);
        }
        if (value instanceof Byte) {
            return value((byte) value);
        }
        if (value instanceof Integer) {
            return value((int) value);
        }
        if (value instanceof Double) {
            return value((double) value);
        }
        if (value instanceof Float) {
            return value((float) value);
        }
        if (value instanceof LocalDate) {
            return value((LocalDate) value);
        }
        if (value instanceof OffsetTime) {
            return value((OffsetTime) value);
        }
        if (value instanceof LocalTime) {
            return value((LocalTime) value);
        }
        if (value instanceof LocalDateTime) {
            return value((LocalDateTime) value);
        }
        if (value instanceof OffsetDateTime) {
            return value((OffsetDateTime) value);
        }
        if (value instanceof ZonedDateTime) {
            return value((ZonedDateTime) value);
        }
        if (value instanceof IsoDuration) {
            return value((IsoDuration) value);
        }
        if (value instanceof Period) {
            return value((Period) value);
        }
        if (value instanceof Duration) {
            return value((Duration) value);
        }
        if (value instanceof Point) {
            return value((Point) value);
        }

        if (value instanceof List<?>) {
            return value((List<Object>) value);
        }
        if (value instanceof Map<?, ?>) {
            return value((Map<String, Object>) value);
        }
        if (value instanceof Iterable<?>) {
            return value((Iterable<Object>) value);
        }
        if (value instanceof Iterator<?>) {
            return value((Iterator<Object>) value);
        }
        if (value instanceof Stream<?>) {
            return value((Stream<Object>) value);
        }
        if (value instanceof java.lang.Record record) {
            return value(record);
        }

        if (value instanceof char[]) {
            return value((char[]) value);
        }
        if (value instanceof byte[]) {
            return value((byte[]) value);
        }
        if (value instanceof boolean[]) {
            return value((boolean[]) value);
        }
        if (value instanceof String[]) {
            return value((String[]) value);
        }
        if (value instanceof long[]) {
            return value((long[]) value);
        }
        if (value instanceof int[]) {
            return value((int[]) value);
        }
        if (value instanceof short[]) {
            return value((short[]) value);
        }
        if (value instanceof double[]) {
            return value((double[]) value);
        }
        if (value instanceof float[]) {
            return value((float[]) value);
        }
        if (value instanceof Value[]) {
            return value((Value[]) value);
        }
        if (value instanceof Object[]) {
            return value(Arrays.asList((Object[]) value));
        }

        var message = "Unable to convert " + value.getClass().getName() + " to Neo4j Value.";
        throw new ClientException(
                GqlStatusError.UNKNOWN.getStatus(),
                GqlStatusError.UNKNOWN.getStatusDescription(message),
                "N/A",
                message,
                GqlStatusError.DIAGNOSTIC_RECORD,
                null);
    }

    /**
     * Returns an array of values from object vararg.
     * @param input the object value(s)
     * @return the array of values
     */
    public static Value[] values(final Object... input) {
        return Arrays.stream(input).map(Values::value).toArray(Value[]::new);
    }

    /**
     * Returns a value from value vararg.
     * @param input the value(s)
     * @return the value
     */
    public static Value value(Value... input) {
        var size = input.length;
        var values = new Value[size];
        System.arraycopy(input, 0, values, 0, size);
        return new ListValue(values);
    }

    /**
     * Returns a value from byte vararg.
     * @param input the byte value(s)
     * @return the value
     */
    public static Value value(byte... input) {
        return new BytesValue(input);
    }

    /**
     * Returns a value from string vararg.
     * @param input the string value(s)
     * @return the value
     */
    public static Value value(String... input) {
        var values = Arrays.stream(input).map(StringValue::new).toArray(StringValue[]::new);
        return new ListValue(values);
    }

    /**
     * Returns a value from boolean vararg.
     * @param input the boolean value(s)
     * @return the value
     */
    public static Value value(boolean... input) {
        var values =
                IntStream.range(0, input.length).mapToObj(i -> value(input[i])).toArray(Value[]::new);
        return new ListValue(values);
    }

    /**
     * Returns a value from char vararg.
     * @param input the char value(s)
     * @return the value
     */
    public static Value value(char... input) {
        var values =
                IntStream.range(0, input.length).mapToObj(i -> value(input[i])).toArray(Value[]::new);
        return new ListValue(values);
    }

    /**
     * Returns a value from long vararg.
     * @param input the long value(s)
     * @return the value
     */
    public static Value value(long... input) {
        var values = Arrays.stream(input).mapToObj(Values::value).toArray(Value[]::new);
        return new ListValue(values);
    }

    /**
     * Returns a value from short vararg.
     * @param input the short value(s)
     * @return the value
     */
    public static Value value(short... input) {
        var values =
                IntStream.range(0, input.length).mapToObj(i -> value(input[i])).toArray(Value[]::new);
        return new ListValue(values);
    }
    /**
     * Returns a value from int vararg.
     * @param input the int value(s)
     * @return the value
     */
    public static Value value(int... input) {
        var values = Arrays.stream(input).mapToObj(Values::value).toArray(Value[]::new);
        return new ListValue(values);
    }
    /**
     * Returns a value from double vararg.
     * @param input the double value(s)
     * @return the value
     */
    public static Value value(double... input) {
        var values = Arrays.stream(input).mapToObj(Values::value).toArray(Value[]::new);
        return new ListValue(values);
    }

    /**
     * Returns a value from float vararg.
     * @param input the float value(s)
     * @return the value
     */
    public static Value value(float... input) {
        var values =
                IntStream.range(0, input.length).mapToObj(i -> value(input[i])).toArray(Value[]::new);
        return new ListValue(values);
    }

    /**
     * Returns a value from list of objects.
     * @param vals the list of objects
     * @return the value
     */
    public static Value value(List<Object> vals) {
        var values = new Value[vals.size()];
        var i = 0;
        for (var val : vals) {
            values[i++] = value(val);
        }
        return new ListValue(values);
    }

    /**
     * Returns a value from iterable of objects.
     * @param val the iterable of objects
     * @return the value
     */
    public static Value value(Iterable<Object> val) {
        return value(val.iterator());
    }

    /**
     * Returns a value from iterator of objects.
     * @param val the iterator of objects
     * @return the value
     */
    public static Value value(Iterator<Object> val) {
        List<Value> values = new ArrayList<>();
        while (val.hasNext()) {
            values.add(value(val.next()));
        }
        return new ListValue(values.toArray(new Value[0]));
    }

    /**
     * Returns a value from stream of objects.
     * @param stream the stream of objects
     * @return the value
     */
    public static Value value(Stream<Object> stream) {
        var values = stream.map(Values::value).toArray(Value[]::new);
        return new ListValue(values);
    }

    /**
     * Returns a {@link TypeSystem#MAP() map} value based on {@link java.lang.reflect.RecordComponent record components}
     * of a given {@link java.lang.Record Java Record}.
     * <p>
     * Example (similar to the <a href=https://github.com/neo4j-graph-examples/movies>Neo4j Movies Database</a>):
     * <pre>
     * {@code
     * // assuming the following Java record
     * public record Movie(String title, String tagline, long released) {}
     * // a new movie may be created in the following way
     * var movie = new Movie("title", "tagline", 2025);
     * var movieValue = Values.value(movie);
     * driver.executableQuery("CREATE (:Movie $movie)")
     *         .withParameters(Map.of("movie", movieValue))
     *         .execute();
     * }
     * </pre>
     * Because the driver methods accepting a {@code Map<String, Object>} as query parameters automatically map values
     * to {@link Value} instances, it is possible to avoid mapping movie explicitly:
     * <pre>
     * {@code
     * var movie = new Movie("title", "tagline", 2025);
     * driver.executableQuery("CREATE (:Movie $movie)")
     *         .withParameters(Map.of("movie", movie))
     *         .execute();
     * }
     * </pre>
     * Assuming movie titles being unique, it is possible to update the created movie in the following way:
     * <pre>
     * {@code
     * var updatedMovie = new Movie("title", "updated tagline", 2024);
     * driver.executableQuery("""
     *                 MATCH (movie:Movie {title: $movie.title})
     *                 SET movie += $movie
     *                 """)
     *         .withParameters(Map.of("movie", updatedMovie))
     *         .execute();
     * }
     * </pre>
     * The {@link Property} annotation may be used to override the record component name.
     * <pre>
     * {@code
     * public record Movie(String title, String tagline, @Property("releasedYear") long released) {}
     * }
     * </pre>
     * Note that those record components that have {@code null} value will be excluded from the map value.
     * <p>
     * It is also important to understand that sending all properties over network may not always be desirable and will
     * depend on a use-case.
     * <p>
     * In addition, please note that while this mapping allows nested structures, like map of maps, there may be
     * limitations on how those are supported by the database. Please read the Neo4j Cypher Manual for more up-to-date
     * details. For example, at the time of writing, it is not possible to store maps as properties
     * (see the following <a href="https://neo4j.com/docs/cypher-manual/current/values-and-types/property-structural-constructed/#constructed-types">page</a>).
     *
     * @param record the record to map
     * @return the map value
     * @see TypeSystem#MAP()
     * @see java.lang.Record
     * @see java.lang.reflect.RecordComponent
     * @see Property
     * @throws ClientException when mapping fails
     * @since 5.28.5
     */
    @Preview(name = "Object mapping")
    public static Value value(java.lang.Record record) {
        var recordComponents = record.getClass().getRecordComponents();
        Map<String, Value> val = new HashMap<>(recordComponents.length);
        for (var recordComponent : recordComponents) {
            var propertyAnnotation = recordComponent.getAnnotation(Property.class);
            var property = propertyAnnotation != null ? propertyAnnotation.value() : recordComponent.getName();
            Value value;
            try {
                var objectValue = recordComponent.getAccessor().invoke(record);
                value = (objectValue != null) ? value(objectValue) : null;
            } catch (Throwable throwable) {
                var message = "Failed to map '%s' property to value during mapping '%s' to map value"
                        .formatted(property, record.getClass().getCanonicalName());
                throw new ClientException(
                        GqlStatusError.UNKNOWN.getStatus(),
                        GqlStatusError.UNKNOWN.getStatusDescription(message),
                        "N/A",
                        message,
                        GqlStatusError.DIAGNOSTIC_RECORD,
                        throwable);
            }
            if (value != null) {
                val.put(property, value);
            }
        }
        return new MapValue(val);
    }

    /**
     * Returns a value from char.
     * @param val the char value
     * @return the value
     */
    public static Value value(final char val) {
        return new StringValue(String.valueOf(val));
    }

    /**
     * Returns a value from string.
     * @param val the string value
     * @return the value
     */
    public static Value value(final String val) {
        return new StringValue(val);
    }

    /**
     * Returns a value from long.
     * @param val the long value
     * @return the value
     */
    public static Value value(final long val) {
        return new IntegerValue(val);
    }

    /**
     * Returns a value from int.
     * @param val the int value
     * @return the value
     */
    public static Value value(final int val) {
        return new IntegerValue(val);
    }

    /**
     * Returns a value from double.
     * @param val the double value
     * @return the value
     */
    public static Value value(final double val) {
        return new FloatValue(val);
    }

    /**
     * Returns a value from boolean.
     * @param val the boolean value
     * @return the value
     */
    public static Value value(final boolean val) {
        return BooleanValue.fromBoolean(val);
    }

    /**
     * Returns a value from string to object map.
     * @param val the string to object map
     * @return the value
     */
    public static Value value(final Map<String, Object> val) {
        Map<String, Value> asValues = newHashMapWithSize(val.size());
        for (var entry : val.entrySet()) {
            asValues.put(entry.getKey(), value(entry.getValue()));
        }
        return new MapValue(asValues);
    }

    /**
     * Returns a value from local date.
     * @param localDate the local date value
     * @return the value
     */
    public static Value value(LocalDate localDate) {
        return new DateValue(localDate);
    }

    /**
     * Returns a value from offset time.
     * @param offsetTime the offset time value
     * @return the value
     */
    public static Value value(OffsetTime offsetTime) {
        return new TimeValue(offsetTime);
    }

    /**
     * Returns a value from local time.
     * @param localTime the local time value
     * @return the value
     */
    public static Value value(LocalTime localTime) {
        return new LocalTimeValue(localTime);
    }

    /**
     * Returns a value from local date time.
     * @param localDateTime the local date time value
     * @return the value
     */
    public static Value value(LocalDateTime localDateTime) {
        return new LocalDateTimeValue(localDateTime);
    }

    /**
     * Returns a value from offset date time.
     * @param offsetDateTime the offset date time value
     * @return the value
     */
    public static Value value(OffsetDateTime offsetDateTime) {
        return new DateTimeValue(offsetDateTime.toZonedDateTime());
    }

    /**
     * Returns a value from zoned date time.
     * @param zonedDateTime the zoned date time value
     * @return the value
     */
    public static Value value(ZonedDateTime zonedDateTime) {
        return new DateTimeValue(zonedDateTime);
    }

    /**
     * Returns a value from period.
     * @param period the period value
     * @return the value
     */
    public static Value value(Period period) {
        return value(new InternalIsoDuration(period));
    }

    /**
     * Returns a value from duration.
     * @param duration the duration value
     * @return the value
     */
    public static Value value(Duration duration) {
        return value(new InternalIsoDuration(duration));
    }

    /**
     * Returns a value from month, day, seconds and nanoseconds values.
     * @param months the month value
     * @param days the day value
     * @param seconds the seconds value
     * @param nanoseconds the nanoseconds value
     * @return the value
     */
    public static Value isoDuration(long months, long days, long seconds, int nanoseconds) {
        return value(new InternalIsoDuration(months, days, seconds, nanoseconds));
    }

    /**
     * Returns a value from ISO duration.
     * @param duration the ISO duration value
     * @return the value
     */
    private static Value value(IsoDuration duration) {
        return new DurationValue(duration);
    }

    /**
     * Returns a value from SRID, x and y values.
     * @param srid the SRID value
     * @param x the x value
     * @param y the y value
     * @return the value
     */
    public static Value point(int srid, double x, double y) {
        return value((Point) new InternalPoint2D(srid, x, y));
    }

    /**
     * Returns a value from point.
     * @param point the point value
     * @return the value
     */
    private static Value value(Point point) {
        return new PointValue(point);
    }

    /**
     * Returns a value from SRID, x ,y and z values.
     * @param srid the SRID value
     * @param x the x value
     * @param y the y value
     * @param z the z value
     * @return the value
     */
    public static Value point(int srid, double x, double y, double z) {
        return value((Point) new InternalPoint3D(srid, x, y, z));
    }

    /**
     * Helper function for creating a map of parameters, this can be used when you {@link
     * QueryRunner#run(String, Value) run} queries.
     * <p>
     * Allowed parameter types are:
     * <ul>
     * <li>{@link Integer}</li>
     * <li>{@link Long}</li>
     * <li>{@link Boolean}</li>
     * <li>{@link Double}</li>
     * <li>{@link Float}</li>
     * <li>{@link String}</li>
     * <li>{@link Map} with String keys and values being any type in this list</li>
     * <li>{@link Collection} of any type in this list</li>
     * </ul>
     *
     * @param keysAndValues alternating sequence of keys and values
     * @return Map containing all parameters specified
     * @see QueryRunner#run(String, Value)
     */
    public static Value parameters(Object... keysAndValues) {
        if (keysAndValues.length % 2 != 0) {
            var message = "Parameters function requires an even number " + "of arguments, "
                    + "alternating key and value. Arguments were: "
                    + Arrays.toString(keysAndValues)
                    + ".";
            throw new ClientException(
                    GqlStatusError.UNKNOWN.getStatus(),
                    GqlStatusError.UNKNOWN.getStatusDescription(message),
                    "N/A",
                    message,
                    GqlStatusError.DIAGNOSTIC_RECORD,
                    null);
        }
        HashMap<String, Value> map = newHashMapWithSize(keysAndValues.length / 2);
        for (var i = 0; i < keysAndValues.length; i += 2) {
            var value = keysAndValues[i + 1];
            assertParameter(value);
            map.put(keysAndValues[i].toString(), value(value));
        }
        return value(map);
    }

    /**
     * The identity function for value conversion - returns the value untouched.
     *
     * @return a function that returns the value passed into it - the identity function
     */
    public static Function<Value, Value> ofValue() {
        return Function.identity();
    }

    /**
     * Converts values to objects using {@link Value#asObject()}.
     *
     * @return a function that returns {@link Value#asObject()} of a {@link Value}
     */
    public static Function<Value, Object> ofObject() {
        return Value::asObject;
    }

    /**
     * Converts values to {@link Number}.
     *
     * @return a function that returns {@link Value#asNumber()} of a {@link Value}
     */
    public static Function<Value, Number> ofNumber() {
        return Value::asNumber;
    }

    /**
     * Converts values to {@link String}.
     * <p>
     * If you want to access a string you've retrieved from the database, this is
     * the right choice. If you want to print any value for human consumption, for
     * instance in a log, {@link #ofToString()} is the right choice.
     *
     * @return a function that returns {@link Value#asString()} of a {@link Value}
     */
    public static Function<Value, String> ofString() {
        return Value::asString;
    }

    /**
     * Converts values using {@link Value#toString()}, a human-readable string
     * description of any value.
     * <p>
     * This is different from {@link #ofString()}, which returns a java
     * {@link String} value from a database {@link TypeSystem#STRING()}.
     * <p>
     * If you are wanting to print any value for human consumption, this is the
     * right choice. If you are wanting to access a string value stored in the
     * database, you should use {@link #ofString()}.
     *
     * @return a function that returns {@link Value#toString()} of a {@link Value}
     */
    public static Function<Value, String> ofToString() {
        return Value::toString;
    }

    /**
     * Converts values to {@link Integer}.
     *
     * @return a function that returns {@link Value#asInt()} of a {@link Value}
     */
    public static Function<Value, Integer> ofInteger() {
        return Value::asInt;
    }

    /**
     * Converts values to {@link Long}.
     *
     * @return a function that returns {@link Value#asLong()} of a {@link Value}
     */
    public static Function<Value, Long> ofLong() {
        return Value::asLong;
    }

    /**
     * Converts values to {@link Float}.
     *
     * @return a function that returns {@link Value#asFloat()} of a {@link Value}
     */
    public static Function<Value, Float> ofFloat() {
        return Value::asFloat;
    }

    /**
     * Converts values to {@link Double}.
     *
     * @return a function that returns {@link Value#asDouble()} of a {@link Value}
     */
    public static Function<Value, Double> ofDouble() {
        return Value::asDouble;
    }

    /**
     * Converts values to {@link Boolean}.
     *
     * @return a function that returns {@link Value#asBoolean()} of a {@link Value}
     */
    public static Function<Value, Boolean> ofBoolean() {
        return Value::asBoolean;
    }

    /**
     * Converts values to {@link Map}.
     *
     * @return a function that returns {@link Value#asMap()} of a {@link Value}
     */
    public static Function<Value, Map<String, Object>> ofMap() {
        return MapAccessor::asMap;
    }

    /**
     * Converts values to {@link Map}, with the map values further converted using
     * the provided converter.
     *
     * @param valueConverter converter to use for the values of the map
     * @param <T> the type of values in the returned map
     * @return a function that returns {@link Value#asMap(Function)} of a {@link Value}
     */
    public static <T> Function<Value, Map<String, T>> ofMap(final Function<Value, T> valueConverter) {
        return val -> val.asMap(valueConverter);
    }

    /**
     * Converts values to {@link Entity}.
     *
     * @return a function that returns {@link Value#asEntity()} of a {@link Value}
     */
    public static Function<Value, Entity> ofEntity() {
        return Value::asEntity;
    }

    /**
     * Converts values to {@link Long entity id}.
     *
     * @deprecated superseded by {@link #ofEntityElementId()}.
     * @return a function that returns the id an entity {@link Value}
     */
    @Deprecated
    public static Function<Value, Long> ofEntityId() {
        return val -> val.asEntity().id();
    }

    /**
     * Converts values to {@link String element id}.
     *
     * @return a function that returns the element id of an entity {@link Value}
     */
    public static Function<Value, String> ofEntityElementId() {
        return val -> val.asEntity().elementId();
    }

    /**
     * Converts values to {@link Node}.
     *
     * @return a function that returns {@link Value#asNode()} of a {@link Value}
     */
    public static Function<Value, Node> ofNode() {
        return Value::asNode;
    }

    /**
     * Converts values to {@link Relationship}.
     *
     * @return a function that returns {@link Value#asRelationship()} of a {@link Value}
     */
    public static Function<Value, Relationship> ofRelationship() {
        return Value::asRelationship;
    }

    /**
     * Converts values to {@link Path}.
     *
     * @return a function that returns {@link Value#asPath()} of a {@link Value}
     */
    public static Function<Value, Path> ofPath() {
        return Value::asPath;
    }

    /**
     * Converts values to {@link LocalDate}.
     *
     * @return a function that returns {@link Value#asLocalDate()} of a {@link Value}
     */
    public static Function<Value, LocalDate> ofLocalDate() {
        return Value::asLocalDate;
    }

    /**
     * Converts values to {@link OffsetTime}.
     *
     * @return a function that returns {@link Value#asOffsetTime()} of a {@link Value}
     */
    public static Function<Value, OffsetTime> ofOffsetTime() {
        return Value::asOffsetTime;
    }

    /**
     * Converts values to {@link LocalTime}.
     *
     * @return a function that returns {@link Value#asLocalTime()} of a {@link Value}
     */
    public static Function<Value, LocalTime> ofLocalTime() {
        return Value::asLocalTime;
    }

    /**
     * Converts values to {@link LocalDateTime}.
     *
     * @return a function that returns {@link Value#asLocalDateTime()} of a {@link Value}
     */
    public static Function<Value, LocalDateTime> ofLocalDateTime() {
        return Value::asLocalDateTime;
    }

    /**
     * Converts values to {@link OffsetDateTime}.
     *
     * @return a function that returns {@link Value#asOffsetDateTime()} of a {@link Value}
     */
    public static Function<Value, OffsetDateTime> ofOffsetDateTime() {
        return Value::asOffsetDateTime;
    }

    /**
     * Converts values to {@link ZonedDateTime}.
     *
     * @return a function that returns {@link Value#asZonedDateTime()} of a {@link Value}
     */
    public static Function<Value, ZonedDateTime> ofZonedDateTime() {
        return Value::asZonedDateTime;
    }

    /**
     * Converts values to {@link IsoDuration}.
     *
     * @return a function that returns {@link Value#asIsoDuration()} of a {@link Value}
     */
    public static Function<Value, IsoDuration> ofIsoDuration() {
        return Value::asIsoDuration;
    }

    /**
     * Converts values to {@link Point}.
     *
     * @return a function that returns {@link Value#asPoint()} of a {@link Value}
     */
    public static Function<Value, Point> ofPoint() {
        return Value::asPoint;
    }

    /**
     * Converts values to {@link List} of {@link Object}.
     *
     * @return a function that returns {@link Value#asList()} of a {@link Value}
     */
    public static Function<Value, List<Object>> ofList() {
        return Value::asList;
    }

    /**
     * Converts values to {@link List} of {@code T}.
     *
     * @param innerMap converter for the values inside the list
     * @param <T> the type of values inside the list
     * @return a function that returns {@link Value#asList(Function)} of a {@link Value}
     */
    public static <T> Function<Value, List<T>> ofList(final Function<Value, T> innerMap) {
        return value -> value.asList(innerMap);
    }
}
