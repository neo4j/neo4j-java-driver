/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.driver;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.internal.value.NullValue;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.value.LossyCoercion;
import org.neo4j.driver.exceptions.value.Uncoercible;
import org.neo4j.driver.types.Entity;
import org.neo4j.driver.types.IsoDuration;
import org.neo4j.driver.types.MapAccessor;
import org.neo4j.driver.types.MapAccessorWithDefaultValue;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Path;
import org.neo4j.driver.types.Point;
import org.neo4j.driver.types.Relationship;
import org.neo4j.driver.types.Type;
import org.neo4j.driver.types.TypeSystem;
import org.neo4j.driver.util.Experimental;
import java.util.function.Function;
import org.neo4j.driver.util.Immutable;

/**
 * A unit of data that adheres to the Neo4j type system.
 *
 * This interface describes a number of <code>isType</code> methods along with
 * <code>typeValue</code> methods. The first set of these correlate with types from
 * the Neo4j Type System and are used to determine which Neo4j type is represented.
 * The second set of methods perform coercions to Java types (wherever possible).
 * For example, a common String value should be tested for using <code>isString</code>
 * and extracted using <code>stringValue</code>.
 *
 * <h2>Navigating a tree structure</h2>
 *
 * Because Neo4j often handles dynamic structures, this interface is designed to help
 * you handle such structures in Java. Specifically, {@link Value} lets you navigate arbitrary tree
 * structures without having to resort to type casting.
 *
 * Given a tree structure like:
 *
 * <pre>
 * {@code
 * {
 *   users : [
 *     { name : "Anders" },
 *     { name : "John" }
 *   ]
 * }
 * }
 * </pre>
 *
 * You can retrieve the name of the second user, John, like so:
 *
 * <pre class="docTest:ValueDocIT#classDocTreeExample">
 * {@code
 * String username = value.get("users").get(1).get("name").asString();
 * }
 * </pre>
 *
 * You can also easily iterate over the users:
 *
 * <pre class="docTest:ValueDocIT#classDocIterationExample">
 * {@code
 * List<String> names = new LinkedList<>();
 * for(Value user : value.get("users").values() )
 * {
 *     names.add(user.get("name").asString());
 * }
 * }
 * </pre>
 * @since 1.0
 */
@Immutable
public interface Value extends MapAccessor, MapAccessorWithDefaultValue
{
    /**
     * If the underlying value is a collection type, return the number of values in the collection.
     * <p>
     * For {@link TypeSystem#LIST()}  list} values, this will return the size of the list.
     * <p>
     * For {@link TypeSystem#MAP() map} values, this will return the number of entries in the map.
     * <p>
     * For {@link TypeSystem#NODE() node} and {@link TypeSystem#RELATIONSHIP()}  relationship} values,
     * this will return the number of properties.
     * <p>
     * For {@link TypeSystem#PATH() path} values, this returns the length (number of relationships) in the path.
     *
     * @return the number of values in an underlying collection
     */
    int size();

    /**
     * If this value represents a list or map, test if the collection is empty.
     *
     * @return {@code true} if size() is 0, otherwise {@code false}
     */
    boolean isEmpty();

    /**
     * If the underlying value supports {@link #get(String) key-based indexing}, return an iterable of the keys in the
     * map, this applies to {@link TypeSystem#MAP() map}, {@link #asNode() node} and {@link
     * TypeSystem#RELATIONSHIP()}  relationship} values.
     *
     * @return the keys in the value
     */
    @Override
    Iterable<String> keys();

    /**
     * Retrieve the value at the given index
     *
     * @param index the index of the value
     * @return the value or a {@link org.neo4j.driver.internal.value.NullValue} if the index is out of bounds
     * @throws ClientException if record has not been initialized
     */
    Value get( int index );

    /** @return The type of this value as defined in the Neo4j type system */
    @Experimental
    Type type();

    /**
     * Test if this value is a value of the given type
     *
     * @param type the given type
     * @return type.isTypeOf( this )
     */
    @Experimental
    boolean hasType( Type type );

    /**
     * @return {@code true} if the value is a Boolean value and has the value True.
     */
    boolean isTrue();

    /**
     * @return {@code true} if the value is a Boolean value and has the value False.
     */
    boolean isFalse();

    /**
     * @return {@code true} if the value is a Null, otherwise {@code false}
     */
    boolean isNull();

    /**
     * This returns a java standard library representation of the underlying value,
     * using a java type that is "sensible" given the underlying type. The mapping
     * for common types is as follows:
     *
     * <ul>
     *     <li>{@link TypeSystem#NULL()} - {@code null}</li>
     *     <li>{@link TypeSystem#LIST()} - {@link List}</li>
     *     <li>{@link TypeSystem#MAP()} - {@link Map}</li>
     *     <li>{@link TypeSystem#BOOLEAN()} - {@link Boolean}</li>
     *     <li>{@link TypeSystem#INTEGER()} - {@link Long}</li>
     *     <li>{@link TypeSystem#FLOAT()} - {@link Double}</li>
     *     <li>{@link TypeSystem#STRING()} - {@link String}</li>
     *     <li>{@link TypeSystem#BYTES()} - {@literal byte[]}</li>
     *     <li>{@link TypeSystem#DATE()} - {@link LocalDate}</li>
     *     <li>{@link TypeSystem#TIME()} - {@link OffsetTime}</li>
     *     <li>{@link TypeSystem#LOCAL_TIME()} - {@link LocalTime}</li>
     *     <li>{@link TypeSystem#DATE_TIME()} - {@link ZonedDateTime}</li>
     *     <li>{@link TypeSystem#LOCAL_DATE_TIME()} - {@link LocalDateTime}</li>
     *     <li>{@link TypeSystem#DURATION()} - {@link IsoDuration}</li>
     *     <li>{@link TypeSystem#POINT()} - {@link Point}</li>
     *     <li>{@link TypeSystem#NODE()} - {@link Node}</li>
     *     <li>{@link TypeSystem#RELATIONSHIP()} - {@link Relationship}</li>
     *     <li>{@link TypeSystem#PATH()} - {@link Path}</li>
     * </ul>
     *
     * Note that the types in {@link TypeSystem} refers to the Neo4j type system
     * where {@link TypeSystem#INTEGER()} and {@link TypeSystem#FLOAT()} are both
     * 64-bit precision. This is why these types return java {@link Long} and
     * {@link Double}, respectively.
     *
     * @return the value as a Java Object
     */
    Object asObject();

    /**
     * Apply the mapping function on the value if the value is not a {@link NullValue}, or the default value if the value is a {@link NullValue}.
     * @param mapper The mapping function defines how to map a {@link Value} to T.
     * @param defaultValue the value to return if the value is a {@link NullValue}
     * @param <T> The return type
     * @return The value after applying the given mapping function or the default value if the value is {@link NullValue}.
     */
    <T>T computeOrDefault( Function<Value, T> mapper, T defaultValue );

    /**
     * @return the value as a Java boolean, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    boolean asBoolean();

    /**
     * @param defaultValue return this value if the value is a {@link NullValue}.
     * @return the value as a Java boolean, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    boolean asBoolean( boolean defaultValue );

    /**
     *  @return the value as a Java byte array, if possible.
     *  @throws Uncoercible if value types are incompatible.
     */
    byte[] asByteArray();

    /**
     *  @param defaultValue default to this value if the original value is a {@link NullValue}
     *  @return the value as a Java byte array, if possible.
     *  @throws Uncoercible if value types are incompatible.
     */
    byte[] asByteArray( byte[] defaultValue );

    /**
     *  @return the value as a Java String, if possible.
     *  @throws Uncoercible if value types are incompatible.
     */
    String asString();

    /**
     * @param defaultValue return this value if the value is null.
     * @return the value as a Java String, if possible
     * @throws Uncoercible if value types are incompatible.
     */
    String asString( String defaultValue );

    /**
     * @return the value as a Java Number, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    Number asNumber();

    /**
     * Returns a Java long if no precision is lost in the conversion.
     *
     * @return the value as a Java long.
     * @throws LossyCoercion if it is not possible to convert the value without loosing precision.
     * @throws Uncoercible if value types are incompatible.
     */
    long asLong();

    /**
     * Returns a Java long if no precision is lost in the conversion.
     * @param defaultValue return this default value if the value is a {@link NullValue}.
     * @return the value as a Java long.
     * @throws LossyCoercion if it is not possible to convert the value without loosing precision.
     * @throws Uncoercible if value types are incompatible.
     */
    long asLong( long defaultValue );

    /**
     * Returns a Java int if no precision is lost in the conversion.
     *
     * @return the value as a Java int.
     * @throws LossyCoercion if it is not possible to convert the value without loosing precision.
     * @throws Uncoercible if value types are incompatible.
     */
    int asInt();

    /**
     * Returns a Java int if no precision is lost in the conversion.
     * @param defaultValue return this default value if the value is a {@link NullValue}.
     * @return the value as a Java int.
     * @throws LossyCoercion if it is not possible to convert the value without loosing precision.
     * @throws Uncoercible if value types are incompatible.
     */
    int asInt( int defaultValue );

    /**
     * Returns a Java double if no precision is lost in the conversion.
     *
     * @return the value as a Java double.
     * @throws LossyCoercion if it is not possible to convert the value without loosing precision.
     * @throws Uncoercible if value types are incompatible.
     */
    double asDouble();

    /**
     * Returns a Java double if no precision is lost in the conversion.
     * @param defaultValue default to this value if the value is a {@link NullValue}.
     * @return the value as a Java double.
     * @throws LossyCoercion if it is not possible to convert the value without loosing precision.
     * @throws Uncoercible if value types are incompatible.
     */
    double asDouble( double defaultValue );

    /**
     * Returns a Java float if no precision is lost in the conversion.
     *
     * @return the value as a Java float.
     * @throws LossyCoercion if it is not possible to convert the value without loosing precision.
     * @throws Uncoercible if value types are incompatible.
     */
    float asFloat();

    /**
     * Returns a Java float if no precision is lost in the conversion.
     * @param defaultValue default to this value if the value is a {@link NullValue}
     * @return the value as a Java float.
     * @throws LossyCoercion if it is not possible to convert the value without loosing precision.
     * @throws Uncoercible if value types are incompatible.
     */
    float asFloat( float defaultValue );

    /**
     * If the underlying type can be viewed as a list, returns a java list of
     * values, where each value has been converted using {@link #asObject()}.
     *
     * @see #asObject()
     * @return the value as a Java list of values, if possible
     */
    List<Object> asList();


    /**
     * If the underlying type can be viewed as a list, returns a java list of
     * values, where each value has been converted using {@link #asObject()}.
     *
     * @see #asObject()
     * @param defaultValue default to this value if the value is a {@link NullValue}
     * @return the value as a Java list of values, if possible
     */
    List<Object> asList( List<Object> defaultValue );

    /**
     * @param mapFunction a function to map from Value to T. See {@link Values} for some predefined functions, such
     * as {@link Values#ofBoolean()}, {@link Values#ofList(Function)}.
     * @param <T> the type of target list elements
     * @see Values for a long list of built-in conversion functions
     * @return the value as a list of T obtained by mapping from the list elements, if possible
     */
    <T> List<T> asList( Function<Value,T> mapFunction );

    /**
     * @param mapFunction a function to map from Value to T. See {@link Values} for some predefined functions, such
     * as {@link Values#ofBoolean()}, {@link Values#ofList(Function)}.
     * @param <T> the type of target list elements
     * @param defaultValue default to this value if the value is a {@link NullValue}
     * @see Values for a long list of built-in conversion functions
     * @return the value as a list of T obtained by mapping from the list elements, if possible
     */
    <T> List<T> asList( Function<Value,T> mapFunction, List<T> defaultValue );

    /**
     * @return the value as a {@link Entity}, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    Entity asEntity();

    /**
     * @return the value as a {@link Node}, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    Node asNode();

    /**
     * @return the value as a {@link Relationship}, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    Relationship asRelationship();

    /**
     * @return the value as a {@link Path}, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    Path asPath();

    /**
     * @return the value as a {@link LocalDate}, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    LocalDate asLocalDate();

    /**
     * @return the value as a {@link OffsetTime}, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    OffsetTime asOffsetTime();

    /**
     * @return the value as a {@link LocalTime}, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    LocalTime asLocalTime();

    /**
     * @return the value as a {@link LocalDateTime}, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    LocalDateTime asLocalDateTime();

    /**
     * @return the value as a {@link java.time.OffsetDateTime}, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    OffsetDateTime asOffsetDateTime();

    /**
     * @return the value as a {@link ZonedDateTime}, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    ZonedDateTime asZonedDateTime();

    /**
     * @return the value as a {@link IsoDuration}, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    IsoDuration asIsoDuration();

    /**
     * @return the value as a {@link Point}, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    Point asPoint();

    /**
     * @param defaultValue default to this value if the value is a {@link NullValue}
     * @return the value as a {@link LocalDate}, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    LocalDate asLocalDate( LocalDate defaultValue );

    /**
     * @param defaultValue default to this value if the value is a {@link NullValue}
     * @return the value as a {@link OffsetTime}, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    OffsetTime asOffsetTime( OffsetTime defaultValue );

    /**
     * @param defaultValue default to this value if the value is a {@link NullValue}
     * @return the value as a {@link LocalTime}, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    LocalTime asLocalTime( LocalTime defaultValue );

    /**
     * @param defaultValue default to this value if the value is a {@link NullValue}
     * @return the value as a {@link LocalDateTime}, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    LocalDateTime asLocalDateTime( LocalDateTime defaultValue );

    /**
     * @param defaultValue default to this value if the value is a {@link NullValue}
     * @return the value as a {@link OffsetDateTime}, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    OffsetDateTime asOffsetDateTime( OffsetDateTime defaultValue );

    /**
     * @param defaultValue default to this value if the value is a {@link NullValue}
     * @return the value as a {@link ZonedDateTime}, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    ZonedDateTime asZonedDateTime( ZonedDateTime defaultValue );

    /**
     * @param defaultValue default to this value if the value is a {@link NullValue}
     * @return the value as a {@link IsoDuration}, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    IsoDuration asIsoDuration( IsoDuration defaultValue );

    /**
     * @param defaultValue default to this value if the value is a {@link NullValue}
     * @return the value as a {@link Point}, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    Point asPoint( Point defaultValue );

    /**
     * Return as a map of string keys and values converted using
     * {@link Value#asObject()}.
     *
     * This is equivalent to calling {@link #asMap(Function, Map)} with {@link Values#ofObject()}.
     *
     * @param defaultValue default to this value if the value is a {@link NullValue}
     * @return the value as a Java map
     */
    Map<String, Object> asMap( Map<String,Object> defaultValue );

    /**
     * @param mapFunction a function to map from Value to T. See {@link Values} for some predefined functions, such
     * as {@link Values#ofBoolean()}, {@link Values#ofList(Function)}.
     * @param <T> the type of map values
     * @param defaultValue default to this value if the value is a {@link NullValue}
     * @see Values for a long list of built-in conversion functions
     * @return the value as a map from string keys to values of type T obtained from mapping he original map values, if possible
     */
    <T> Map<String, T> asMap( Function<Value, T> mapFunction, Map<String, T> defaultValue );

    @Override
    boolean equals( Object other );

    @Override
    int hashCode();

    @Override
    String toString();
}
