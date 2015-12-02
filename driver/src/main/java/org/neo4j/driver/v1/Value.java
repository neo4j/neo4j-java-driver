/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
 * String username = value.value("users").value(1).value("name").asString();
 * }
 * </pre>
 *
 * You can also easily iterate over the users:
 *
 * <pre class="docTest:ValueDocIT#classDocIterationExample">
 * {@code
 * List<String> names = new LinkedList<>();
 * for(Value user : value.value("users").values() )
 * {
 *     names.add(user.value("name").asString());
 * }
 * }
 * </pre>
 */
public interface Value extends MapLike, ListLike
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
    @Override
    int elementCount();

    /**
     * If the underlying value supports {@link #value(String) key-based indexing}, return an iterable of the keys in the
     * map, this applies to {@link TypeSystem#MAP() map}, {@link #asNode() node} and {@link
     * TypeSystem#RELATIONSHIP()}  relationship} values.
     *
     * @return the keys in the value
     */
    @Override
    Iterable<String> keys();

    /** @return The type of this value as defined in the Cypher language */
    Type type();

    /**
     * Test if this value is a value of the given type
     *
     * @param type the given type
     * @return type.isTypeOf( this )
     */
    boolean hasType( Type type );

    boolean isNull();

    /** @return the value as a Java Object */
    Object asObject();

    /**
     * If the value represents a number, this method will return true if the number is not equals to 0.
     * If the value represents a collection, this method will return true if the collection is not empty.
     * If the value represents a string, this method will return true, if the string is not empty.
     * @return the value as a Java boolean, if possible.
     */
    boolean asBoolean();

    /** @return the value as a Java String, if possible. */
    String asString();

    /** @return the value as a Java char, if possible. */
    char asChar();

    /** @return the value as a Java Number, if possible. */
    Number asNumber();

    /** @return the value as a Java long, if possible. */
    long asLong();

    /** @return the value as a Java int, if possible. */
    int asInt();

    /** @return the value as a Java short, if possible. */
    short asShort();

    /** @return the value as a Java byte, if possible. */
    byte asByte();

    /** @return the value as a Java double, if possible. */
    double asDouble();

    /** @return the value as a Java float, if possible. */
    float asFloat();

    /** @return the value as an {@link Identity}, if possible. */
    Identity asIdentity();

    /**
     * @return the value as a list of values, if possible
     */
    List<Value> asList();

    /**
     * @param mapFunction a function to map from Value to T. See {@link Values} for some predefined functions, such
     * as {@link Values#valueAsBoolean()}, {@link Values#valueAsList(Function)}.
     * @param <T> the type of list elements
     * @return the value as a list of T, if possible
     */
    <T> List<T> asList( Function<Value, T> mapFunction );

    Value[] asArray();

    <T> T[] asArray( Class<T> clazz, Function<Value, T> mapFunction );

    char[] asCharArray();

    long[] asLongArray();

    int[] asIntArray();

    short[] asShortArray();

    byte[] asByteArray();

    double[] asDoubleArray();

    float[] asFloatArray();

    /**
     * @return the value as a value map, if possible
     */
    Map<String, Value> asMap();

    /**
     * @param mapFunction a function to map from Value to T. See {@link Values} for some predefined functions, such
     * as {@link Values#valueAsBoolean()}, {@link Values#valueAsList(Function)}.
     * @param <T> the type of map values
     * @return the value as a map from string keys to values of type T, if possible
     */
    <T> Map<String, T> asMap( Function<Value, T> mapFunction );

    /** @return the value as a {@link Node}, if possible. */
    Node asNode();

    /** @return the value as a {@link Relationship}, if possible. */
    Relationship asRelationship();

    /** @return the value as a {@link Path}, if possible. */
    Path asPath();
}
