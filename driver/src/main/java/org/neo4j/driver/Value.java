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

import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.value.LossyCoercion;
import org.neo4j.driver.exceptions.value.Uncoercible;
import org.neo4j.driver.internal.value.NullValue;
import org.neo4j.driver.mapping.Property;
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
import org.neo4j.driver.util.Immutable;
import org.neo4j.driver.util.Preview;

/**
 * A unit of data that adheres to the Neo4j type system.
 * <p>
 * This interface describes a number of <code>isType</code> methods along with
 * <code>typeValue</code> methods. The first set of these correlate with types from
 * the Neo4j Type System and are used to determine which Neo4j type is represented.
 * The second set of methods perform coercions to Java types (wherever possible).
 * For example, a common String value should be tested for using <code>isString</code>
 * and extracted using <code>stringValue</code>.
 * <h2>Navigating a tree structure</h2>
 * <p>
 * Because Neo4j often handles dynamic structures, this interface is designed to help
 * you handle such structures in Java. Specifically, {@link Value} lets you navigate arbitrary tree
 * structures without having to resort to type casting.
 * <p>
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
 * <p>
 * You can retrieve the name of the second user, John, like so:
 * <pre class="docTest:ValueDocIT#classDocTreeExample">
 * {@code
 * String username = value.get("users").get(1).get("name").asString();
 * }
 * </pre>
 * <p>
 * You can also easily iterate over the users:
 * <pre class="docTest:ValueDocIT#classDocIterationExample">
 * {@code
 * List<String> names = new LinkedList<>();
 * for(Value user : value.get("users").values() )
 * {
 *     names.add(user.get("name").asString());
 * }
 * }
 * </pre>
 *
 * @since 1.0
 */
@Immutable
public interface Value extends MapAccessor, MapAccessorWithDefaultValue {
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
    Value get(int index);

    /**
     * Returns the type of this value as defined in the Neo4j type system.
     *
     * @return the type of this value as defined in the Neo4j type system
     */
    Type type();

    /**
     * Test if this value is a value of the given type.
     *
     * @param type the given type
     * @return type.isTypeOf(this)
     */
    boolean hasType(Type type);

    /**
     * Returns {@code true} if the value is a Boolean value and has the value True.
     *
     * @return {@code true} if the value is a Boolean value and has the value True
     */
    boolean isTrue();

    /**
     * Returns {@code true} if the value is a Boolean value and has the value False.
     *
     * @return {@code true} if the value is a Boolean value and has the value False
     */
    boolean isFalse();

    /**
     * Returns {@code true} if the value is a Null, otherwise {@code false}.
     *
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
     * <p>
     * Note that the types in {@link TypeSystem} refers to the Neo4j type system
     * where {@link TypeSystem#INTEGER()} and {@link TypeSystem#FLOAT()} are both
     * 64-bit precision. This is why these types return java {@link Long} and
     * {@link Double}, respectively.
     *
     * @return the value as a Java Object.
     * @throws DateTimeException if zone information supplied by server is not supported by driver runtime. Applicable to datetime values only.
     */
    Object asObject();

    /**
     * Apply the mapping function on the value if the value is not a {@link NullValue}, or the default value if the value is a {@link NullValue}.
     *
     * @param mapper       The mapping function defines how to map a {@link Value} to T.
     * @param defaultValue the value to return if the value is a {@link NullValue}
     * @param <T>          The return type
     * @return The value after applying the given mapping function or the default value if the value is {@link NullValue}.
     */
    <T> T computeOrDefault(Function<Value, T> mapper, T defaultValue);

    /**
     * Returns the value as a Java boolean, if possible.
     *
     * @return the value as a Java boolean, if possible
     * @throws Uncoercible if value types are incompatible
     */
    boolean asBoolean();

    /**
     * Returns the value as a Java boolean, if possible.
     *
     * @param defaultValue return this value if the value is a {@link NullValue}
     * @return the value as a Java boolean, if possible
     * @throws Uncoercible if value types are incompatible
     */
    boolean asBoolean(boolean defaultValue);

    /**
     * Returns the value as a Java byte array, if possible.
     *
     * @return the value as a Java byte array, if possible
     * @throws Uncoercible if value types are incompatible
     */
    byte[] asByteArray();

    /**
     * Returns the value as a Java byte array, if possible.
     *
     * @param defaultValue default to this value if the original value is a {@link NullValue}
     * @return the value as a Java byte array, if possible
     * @throws Uncoercible if value types are incompatible
     */
    byte[] asByteArray(byte[] defaultValue);

    /**
     * Returns the value as a Java String, if possible.
     *
     * @return the value as a Java String, if possible
     * @throws Uncoercible if value types are incompatible
     */
    String asString();

    /**
     * Returns the value as a Java String, if possible.
     *
     * @param defaultValue return this value if the value is null
     * @return the value as a Java String, if possible
     * @throws Uncoercible if value types are incompatible
     */
    String asString(String defaultValue);

    /**
     * Returns the value as a Java Number, if possible.
     *
     * @return the value as a Java Number, if possible
     * @throws Uncoercible if value types are incompatible
     */
    Number asNumber();

    /**
     * Returns a Java long if no precision is lost in the conversion.
     *
     * @return the value as a Java long.
     * @throws LossyCoercion if it is not possible to convert the value without loosing precision.
     * @throws Uncoercible   if value types are incompatible.
     */
    long asLong();

    /**
     * Returns a Java long if no precision is lost in the conversion.
     *
     * @param defaultValue return this default value if the value is a {@link NullValue}.
     * @return the value as a Java long.
     * @throws LossyCoercion if it is not possible to convert the value without loosing precision.
     * @throws Uncoercible   if value types are incompatible.
     */
    long asLong(long defaultValue);

    /**
     * Returns a Java int if no precision is lost in the conversion.
     *
     * @return the value as a Java int.
     * @throws LossyCoercion if it is not possible to convert the value without loosing precision.
     * @throws Uncoercible   if value types are incompatible.
     */
    int asInt();

    /**
     * Returns a Java int if no precision is lost in the conversion.
     *
     * @param defaultValue return this default value if the value is a {@link NullValue}.
     * @return the value as a Java int.
     * @throws LossyCoercion if it is not possible to convert the value without loosing precision.
     * @throws Uncoercible   if value types are incompatible.
     */
    int asInt(int defaultValue);

    /**
     * Returns a Java double if no precision is lost in the conversion.
     *
     * @return the value as a Java double.
     * @throws LossyCoercion if it is not possible to convert the value without loosing precision.
     * @throws Uncoercible   if value types are incompatible.
     */
    double asDouble();

    /**
     * Returns a Java double if no precision is lost in the conversion.
     *
     * @param defaultValue default to this value if the value is a {@link NullValue}.
     * @return the value as a Java double.
     * @throws LossyCoercion if it is not possible to convert the value without loosing precision.
     * @throws Uncoercible   if value types are incompatible.
     */
    double asDouble(double defaultValue);

    /**
     * Returns a Java float if no precision is lost in the conversion.
     *
     * @return the value as a Java float.
     * @throws LossyCoercion if it is not possible to convert the value without loosing precision.
     * @throws Uncoercible   if value types are incompatible.
     */
    float asFloat();

    /**
     * Returns a Java float if no precision is lost in the conversion.
     *
     * @param defaultValue default to this value if the value is a {@link NullValue}
     * @return the value as a Java float.
     * @throws LossyCoercion if it is not possible to convert the value without loosing precision.
     * @throws Uncoercible   if value types are incompatible.
     */
    float asFloat(float defaultValue);

    /**
     * If the underlying type can be viewed as a list, returns a java list of
     * values, where each value has been converted using {@link #asObject()}.
     *
     * @return the value as a Java list of values, if possible
     * @see #asObject()
     */
    List<Object> asList();

    /**
     * If the underlying type can be viewed as a list, returns a java list of
     * values, where each value has been converted using {@link #asObject()}.
     *
     * @param defaultValue default to this value if the value is a {@link NullValue}
     * @return the value as a Java list of values, if possible
     * @see #asObject()
     */
    List<Object> asList(List<Object> defaultValue);

    /**
     * Returns the value as a list of T obtained by mapping from the list elements, if possible.
     *
     * @param mapFunction a function to map from Value to T. See {@link Values} for some predefined functions, such
     *                    as {@link Values#ofBoolean()}, {@link Values#ofList(Function)}.
     * @param <T>         the type of target list elements
     * @return the value as a list of T obtained by mapping from the list elements, if possible
     * @see Values for a long list of built-in conversion functions
     */
    <T> List<T> asList(Function<Value, T> mapFunction);

    /**
     * Returns the value as a list of T obtained by mapping from the list elements, if possible.
     *
     * @param mapFunction  a function to map from Value to T. See {@link Values} for some predefined functions, such
     *                     as {@link Values#ofBoolean()}, {@link Values#ofList(Function)}.
     * @param <T>          the type of target list elements
     * @param defaultValue default to this value if the value is a {@link NullValue}
     * @return the value as a list of T obtained by mapping from the list elements, if possible
     * @see Values for a long list of built-in conversion functions
     */
    <T> List<T> asList(Function<Value, T> mapFunction, List<T> defaultValue);

    /**
     * Returns the value as a {@link Entity}, if possible.
     *
     * @return the value as a {@link Entity}, if possible
     * @throws Uncoercible if value types are incompatible
     */
    Entity asEntity();

    /**
     * Returns the value as a {@link Node}, if possible.
     *
     * @return the value as a {@link Node}, if possible
     * @throws Uncoercible if value types are incompatible
     */
    Node asNode();

    /**
     * Returns the value as a {@link Relationship}, if possible.
     *
     * @return the value as a {@link Relationship}, if possible
     * @throws Uncoercible if value types are incompatible
     */
    Relationship asRelationship();

    /**
     * Returns the value as a {@link Path}, if possible.
     *
     * @return the value as a {@link Path}, if possible
     * @throws Uncoercible if value types are incompatible
     */
    Path asPath();

    /**
     * Returns the value as a {@link LocalDate}, if possible.
     *
     * @return the value as a {@link LocalDate}, if possible
     * @throws Uncoercible if value types are incompatible
     */
    LocalDate asLocalDate();

    /**
     * Returns the value as a {@link OffsetTime}, if possible.
     *
     * @return the value as a {@link OffsetTime}, if possible
     * @throws Uncoercible if value types are incompatible
     */
    OffsetTime asOffsetTime();

    /**
     * Returns the value as a {@link LocalTime}, if possible.
     *
     * @return the value as a {@link LocalTime}, if possible
     * @throws Uncoercible if value types are incompatible
     */
    LocalTime asLocalTime();

    /**
     * Returns the value as a {@link LocalDateTime}, if possible.
     *
     * @return the value as a {@link LocalDateTime}, if possible
     * @throws Uncoercible if value types are incompatible
     */
    LocalDateTime asLocalDateTime();

    /**
     * Returns the value as a {@link java.time.OffsetDateTime}, if possible.
     *
     * @return the value as a {@link java.time.OffsetDateTime}, if possible
     * @throws Uncoercible       if value types are incompatible
     * @throws DateTimeException if zone information supplied by server is not supported by driver runtime
     */
    OffsetDateTime asOffsetDateTime();

    /**
     * Returns the value as a {@link ZonedDateTime}, if possible.
     *
     * @return the value as a {@link ZonedDateTime}, if possible
     * @throws Uncoercible       if value types are incompatible
     * @throws DateTimeException if zone information supplied by server is not supported by driver runtime
     */
    ZonedDateTime asZonedDateTime();

    /**
     * Returns the value as a {@link IsoDuration}, if possible.
     *
     * @return the value as a {@link IsoDuration}, if possible
     * @throws Uncoercible if value types are incompatible
     */
    IsoDuration asIsoDuration();

    /**
     * Returns the value as a {@link Point}, if possible.
     *
     * @return the value as a {@link Point}, if possible
     * @throws Uncoercible if value types are incompatible
     */
    Point asPoint();

    /**
     * Returns the value as a {@link LocalDate}, if possible.
     *
     * @param defaultValue default to this value if the value is a {@link NullValue}
     * @return the value as a {@link LocalDate}, if possible
     * @throws Uncoercible if value types are incompatible
     */
    LocalDate asLocalDate(LocalDate defaultValue);

    /**
     * Returns the value as a {@link OffsetTime}, if possible.
     *
     * @param defaultValue default to this value if the value is a {@link NullValue}
     * @return the value as a {@link OffsetTime}, if possible
     * @throws Uncoercible if value types are incompatible
     */
    OffsetTime asOffsetTime(OffsetTime defaultValue);

    /**
     * Returns the value as a {@link LocalTime}, if possible.
     *
     * @param defaultValue default to this value if the value is a {@link NullValue}
     * @return the value as a {@link LocalTime}, if possible
     * @throws Uncoercible if value types are incompatible
     */
    LocalTime asLocalTime(LocalTime defaultValue);

    /**
     * Returns the value as a {@link LocalDateTime}, if possible.
     *
     * @param defaultValue default to this value if the value is a {@link NullValue}
     * @return the value as a {@link LocalDateTime}, if possible
     * @throws Uncoercible if value types are incompatible
     */
    LocalDateTime asLocalDateTime(LocalDateTime defaultValue);

    /**
     * Returns the value as a {@link OffsetDateTime}, if possible.
     *
     * @param defaultValue default to this value if the value is a {@link NullValue}
     * @return the value as a {@link OffsetDateTime}, if possible
     * @throws Uncoercible if value types are incompatible
     */
    OffsetDateTime asOffsetDateTime(OffsetDateTime defaultValue);

    /**
     * Returns the value as a {@link ZonedDateTime}, if possible.
     *
     * @param defaultValue default to this value if the value is a {@link NullValue}
     * @return the value as a {@link ZonedDateTime}, if possible
     * @throws Uncoercible if value types are incompatible
     */
    ZonedDateTime asZonedDateTime(ZonedDateTime defaultValue);

    /**
     * Returns the value as a {@link IsoDuration}, if possible.
     *
     * @param defaultValue default to this value if the value is a {@link NullValue}
     * @return the value as a {@link IsoDuration}, if possible
     * @throws Uncoercible if value types are incompatible
     */
    IsoDuration asIsoDuration(IsoDuration defaultValue);

    /**
     * Returns the value as a {@link Point}, if possible.
     *
     * @param defaultValue default to this value if the value is a {@link NullValue}
     * @return the value as a {@link Point}, if possible
     * @throws Uncoercible if value types are incompatible
     */
    Point asPoint(Point defaultValue);

    /**
     * Return as a map of string keys and values converted using
     * {@link Value#asObject()}.
     * <p>
     * This is equivalent to calling {@link #asMap(Function, Map)} with {@link Values#ofObject()}.
     *
     * @param defaultValue default to this value if the value is a {@link NullValue}
     * @return the value as a Java map
     */
    Map<String, Object> asMap(Map<String, Object> defaultValue);

    /**
     * Returns the value as a map from string keys to values of type T obtained from mapping he original map values, if possible.
     *
     * @param mapFunction  a function to map from Value to T. See {@link Values} for some predefined functions, such
     *                     as {@link Values#ofBoolean()}, {@link Values#ofList(Function)}.
     * @param <T>          the type of map values
     * @param defaultValue default to this value if the value is a {@link NullValue}
     * @return the value as a map from string keys to values of type T obtained from mapping he original map values, if possible
     * @see Values for a long list of built-in conversion functions
     */
    <T> Map<String, T> asMap(Function<Value, T> mapFunction, Map<String, T> defaultValue);

    /**
     * Maps this value to the given type providing that is it supported.
     * <p>
     * <b>Basic Mapping</b>
     * <p>
     * Supported destination types depend on the value {@link Type}, please see the table below for more details.
     * <table>
     *     <caption>Supported Mappings</caption>
     *     <thead>
     *      <tr>
     *          <td>Value Type</td>
     *          <td>Supported Target Types</td>
     *      </tr>
     *     </thead>
     *     <tbody>
     *         <tr>
     *             <td>{@link TypeSystem#BOOLEAN}</td>
     *             <td>{@code boolean}, {@link Boolean}</td>
     *         </tr>
     *         <tr>
     *             <td>{@link TypeSystem#BYTES}</td>
     *             <td>{@code byte[]}</td>
     *         </tr>
     *         <tr>
     *             <td>{@link TypeSystem#STRING}</td>
     *             <td>{@link String}, {@code char}, {@link Character}</td>
     *         </tr>
     *         <tr>
     *             <td>{@link TypeSystem#INTEGER}</td>
     *             <td>{@code long}, {@link Long}, {@code int}, {@link Integer}, {@code short}, {@link Short}, {@code double}, {@link Double}, {@code float}, {@link Float}</td>
     *         </tr>
     *         <tr>
     *             <td>{@link TypeSystem#FLOAT}</td>
     *             <td>{@code long}, {@link Long}, {@code int}, {@link Integer}, {@code double}, {@link Double}, {@code float}, {@link Float}</td>
     *         </tr>
     *         <tr>
     *             <td>{@link TypeSystem#PATH}</td>
     *             <td>{@link Path}</td>
     *         </tr>
     *         <tr>
     *             <td>{@link TypeSystem#POINT}</td>
     *             <td>{@link Point}</td>
     *         </tr>
     *         <tr>
     *             <td>{@link TypeSystem#DATE}</td>
     *             <td>{@link LocalDate}</td>
     *         </tr>
     *         <tr>
     *             <td>{@link TypeSystem#TIME}</td>
     *             <td>{@link OffsetTime}</td>
     *         </tr>
     *         <tr>
     *             <td>{@link TypeSystem#LOCAL_TIME}</td>
     *             <td>{@link LocalTime}</td>
     *         </tr>
     *         <tr>
     *             <td>{@link TypeSystem#LOCAL_DATE_TIME}</td>
     *             <td>{@link LocalDateTime}</td>
     *         </tr>
     *         <tr>
     *             <td>{@link TypeSystem#DATE_TIME}</td>
     *             <td>{@link ZonedDateTime}, {@link OffsetDateTime}</td>
     *         </tr>
     *         <tr>
     *             <td>{@link TypeSystem#DURATION}</td>
     *             <td>{@link IsoDuration}, {@link java.time.Period} (only when {@code seconds = 0} and {@code nanoseconds = 0} and no overflow happens),
     *             {@link java.time.Duration} (only when {@code months = 0} and {@code days = 0} and no overflow happens)</td>
     *         </tr>
     *         <tr>
     *             <td>{@link TypeSystem#NULL}</td>
     *             <td>{@code null}</td>
     *         </tr>
     *         <tr>
     *             <td>{@link TypeSystem#LIST}</td>
     *             <td>{@link List}, {@code T[]} as long as list elements may be mapped to the array component type
     *             (for example, {@code char[]}, {@code boolean[]}, {@code String[]}, {@code long[]}, {@code int[]},
     *             {@code short[]}, {@code double[]}, {@code float[]})</td>
     *         </tr>
     *         <tr>
     *             <td>{@link TypeSystem#MAP}</td>
     *             <td>{@link Map}</td>
     *         </tr>
     *         <tr>
     *             <td>{@link TypeSystem#NODE}</td>
     *             <td>{@link Node}</td>
     *         </tr>
     *         <tr>
     *             <td>{@link TypeSystem#RELATIONSHIP}</td>
     *             <td>{@link Relationship}</td>
     *         </tr>
     *     </tbody>
     * </table>
     *
     * <p>
     * <b>Object Mapping</b>
     * <p>
     * Mapping of user-defined properties to user-defined types is supported for the following value types:
     * <ul>
     *     <li>{@link TypeSystem#NODE}</li>
     *     <li>{@link TypeSystem#RELATIONSHIP}</li>
     *     <li>{@link TypeSystem#MAP}</li>
     * </ul>
     * <p>
     * Example (using the <a href=https://github.com/neo4j-graph-examples/movies>Neo4j Movies Database</a>):
     * <pre>
     * {@code
     * // assuming the following Java record
     * public record Movie(String title, String tagline, long released) {}
     * // the nodes may be mapped to Movie instances
     * var movies = driver.executableQuery("MATCH (movie:Movie) RETURN movie")
     *         .execute()
     *         .records()
     *         .stream()
     *         .map(record -> record.get("movie").as(Movie.class))
     *         .toList();
     * }
     * </pre>
     * <p>
     * Note that Object Mapping is an alternative to accessing the user-defined values in a {@link MapAccessor}.
     * If Object Graph Mapping (OGM) is needed, please use a higher level solution built on top of the driver, like
     * <a href="https://neo4j.com/docs/getting-started/languages-guides/java/spring-data-neo4j/">Spring Data Neo4j</a>.
     * <p>
     * The mapping is done by matching user-defined property names to target type constructor parameters. Therefore, the
     * constructor parameters must either have {@link Property} annotation or have a matching name that is available
     * at runtime (note that the constructor parameter names are typically changed by the compiler unless either the
     * compiler {@code -parameters} option is used or they belong to the cannonical constructor of
     * {@link java.lang.Record java.lang.Record}). The name matching is case-sensitive.
     * <p>
     * Additionally, the {@link Property} annotation may be used when mapping a property with a different name to
     * {@link java.lang.Record java.lang.Record} cannonical constructor parameter.
     * <p>
     * The constructor selection criteria is the following (top priority first):
     * <ol>
     *     <li>Maximum matching properties.</li>
     *     <li>Minimum mismatching properties.</li>
     * </ol>
     * The constructor search is done in the order defined by the {@link Class#getDeclaredConstructors} and is
     * finished either when a full match is found with no mismatches or once all constructors have been visited.
     * <p>
     * At least 1 property match must be present for mapping to work.
     * <p>
     * A {@code null} value is used for arguments that don't have a matching property. If the argument does not accept
     * {@code null} value (this includes primitive types), an alternative constructor that excludes it must be
     * available.
     * <p>
     * The mapping only works for types with directly accessible constructors, not interfaces or abstract types.
     * <p>
     * Example with optional property (using the <a href=https://github.com/neo4j-graph-examples/movies>Neo4j Movies Database</a>):
     * <pre>
     * {@code
     * // assuming the following Java record
     * public record Person(String name, long born) {
     *     // alternative constructor for values that don't have 'born' property available
     *     public Person(@Property("name") String name) {
     *         this(name, -1);
     *     }
     * }
     * // the nodes may be mapped to Person instances
     * var persons = driver.executableQuery("MATCH (person:Person) RETURN person")
     *         .execute()
     *         .records()
     *         .stream()
     *         .map(record -> record.get("person").as(Person.class))
     *         .toList();
     * }
     * </pre>
     * <p>
     * Types with generic parameters defined at the class level are not supported. However, constructor arguments with
     * specific types are permitted.
     * <p>
     * Example (using the <a href=https://github.com/neo4j-graph-examples/movies>Neo4j Movies Database</a>):
     * <pre>
     * {@code
     * // assuming the following Java record
     * public record Acted(List<String> roles) {}
     * // the relationships may be mapped to Acted instances
     * var actedList = driver.executableQuery("MATCH ()-[acted:ACTED_IN]-() RETURN acted")
     *         .execute()
     *         .records()
     *         .stream()
     *         .map(record -> record.get("acted").as(Acted.class))
     *         .toList();
     * }
     * </pre>
     * On the contrary, the following record would not be mapped because the type information is insufficient:
     * <pre>
     * {@code
     * public record Acted<T>(List<T> roles) {}
     * }
     * </pre>
     * Wildcard type value is not supported.
     *
     * @param targetClass the target class to map to
     * @param <T>         the target type to map to
     * @return the mapped value
     * @throws Uncoercible when mapping to the target type is not possible
     * @throws LossyCoercion when mapping cannot be achieved without losing precision
     * @see Property
     * @since 5.28.5
     */
    @Preview(name = "Object mapping")
    default <T> T as(Class<T> targetClass) {
        // for backwards compatibility only
        throw new UnsupportedOperationException("not supported");
    }

    @Override
    boolean equals(Object other);

    @Override
    int hashCode();

    @Override
    String toString();
}
