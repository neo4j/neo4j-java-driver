/**
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.driver.v1;

import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.value.LossyCoercion;
import org.neo4j.driver.v1.exceptions.value.Uncoercible;
import org.neo4j.driver.v1.types.Entity;
import org.neo4j.driver.v1.types.MapAccessor;
import org.neo4j.driver.v1.types.MapAccessorWithDefaultValue;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Path;
import org.neo4j.driver.v1.types.Relationship;
import org.neo4j.driver.v1.types.Type;
import org.neo4j.driver.v1.types.TypeSystem;
import org.neo4j.driver.v1.util.Experimental;
import org.neo4j.driver.v1.util.Function;
import org.neo4j.driver.v1.util.Immutable;

/**
 * Represents a value from Neo4j.
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
     * @return <tt>true</tt> if size() is 0, otherwise <tt>false</tt>
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
     * @return <tt>true</tt> if the value is a Boolean value and has the value True.
     */
    boolean isTrue();

    /**
     * @return <tt>true</tt> if the value is a Boolean value and has the value False.
     */
    boolean isFalse();

    /**
     * @return <tt>true</tt> if the value is a Null, otherwise <tt>false</tt>
     */
    boolean isNull();

    /**
     * This returns a java standard library representation of the underlying value,
     * using a java type that is "sensible" given the underlying type. The mapping
     * for common types is as follows:
     *
     * <ul>
     *     <li>{@link TypeSystem#INTEGER()} - {@link Long}</li>
     *     <li>{@link TypeSystem#FLOAT()} - {@link Double}</li>
     *     <li>{@link TypeSystem#NUMBER()} - {@link Number}</li>
     *     <li>{@link TypeSystem#STRING()} - {@link String}</li>
     *     <li>{@link TypeSystem#BOOLEAN()} - {@link Boolean}</li>
     *     <li>{@link TypeSystem#NULL()} - {@code null}</li>
     *     <li>{@link TypeSystem#NODE()} - {@link Node}</li>
     *     <li>{@link TypeSystem#RELATIONSHIP()} - {@link Relationship}</li>
     *     <li>{@link TypeSystem#PATH()} - {@link Path}</li>
     *     <li>{@link TypeSystem#MAP()} - {@link Map}</li>
     *     <li>{@link TypeSystem#LIST()} - {@link List}</li>
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
     * @return the value as a Java boolean, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    boolean asBoolean();

    /**
     *  @return the value as a Java String, if possible.
     *  @throws Uncoercible if value types are incompatible.
     */
    String asString();

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
     * Returns a Java int if no precision is lost in the conversion.
     *
     * @return the value as a Java int.
     * @throws LossyCoercion if it is not possible to convert the value without loosing precision.
     * @throws Uncoercible if value types are incompatible.
     */
    int asInt();

    /**
     * Returns a Java double if no precision is lost in the conversion.
     *
     * @return the value as a Java double.
     * @throws LossyCoercion if it is not possible to convert the value without loosing precision.
     * @throws Uncoercible if value types are incompatible.
     */
    double asDouble();

    /**
     * Returns a Java float if no precision is lost in the conversion.
     *
     * @return the value as a Java float.
     * @throws LossyCoercion if it is not possible to convert the value without loosing precision.
     * @throws Uncoercible if value types are incompatible.
     */
    float asFloat();

    /**
     * If the underlying type can be viewed as a list, returns a java list of
     * values, where each value has been converted using {@link #asObject()}.
     *
     * @see #asObject()
     * @return the value as a Java list of values, if possible
     */
    List<Object> asList();

    /**
     * @param mapFunction a function to map from Value to T. See {@link Values} for some predefined functions, such
     * as {@link Values#ofBoolean()}, {@link Values#ofList(Function)}.
     * @param <T> the type of target list elements
     * @see Values for a long list of built-in conversion functions
     * @return the value as a list of T obtained by mapping from the list elements, if possible
     */
    <T> List<T> asList( Function<Value, T> mapFunction );

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

    // Force implementation
    @Override
    boolean equals( Object other );

    // Force implementation
    @Override
    int hashCode();

    //Force implementation
    @Override
    String toString();
}
