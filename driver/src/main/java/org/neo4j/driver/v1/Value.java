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
 * String username = value.get("users").get(1).get("name").javaString();
 * }
 * </pre>
 *
 * You can also easily iterate over the users:
 *
 * <pre class="docTest:ValueDocIT#classDocIterationExample">
 * {@code
 * List<String> names = new LinkedList<>();
 * for(Value user : value.get("users") )
 * {
 *     names.add(user.get("name").javaString());
 * }
 * }
 * </pre>
 */
public interface Value extends Iterable<Value>
{
    /** @return the value as a Java String, if possible. */
    String javaString();

    /** @return the value as a Java int, if possible. */
    int javaInteger();

    /** @return the value as a Java long, if possible. */
    long javaLong();

    /** @return the value as a Java float, if possible. */
    float javaFloat();

    /** @return the value as a Java double, if possible. */
    double javaDouble();

    /**
     * If the value represents a number, this method will return true if the number is not equals to 0.
     * If the value represents a collection, this method will return true if the collection is not empty.
     * If the value represents a string, this method will return true, if the string is not empty.
     * @return the value as a Java boolean, if possible.
     */
    boolean javaBoolean();

    /**
     * @param mapFunction a function to map from Value to T. See {@link Values} for some predefined functions, such
     * as {@link Values#valueToBoolean()}, {@link Values#valueToList(Function)}.
     * @param <T> the type of list elements
     * @return the value as a list of T, if possible
     */
    <T> List<T> javaList( Function<Value, T> mapFunction );

    /**
     * @param mapFunction a function to map from Value to T. See {@link Values} for some predefined functions, such
     * as {@link Values#valueToBoolean()}, {@link Values#valueToList(Function)}.
     * @param <T> the type of map values
     * @return the value as a map from string keys to values of type T, if possible
     */
    <T> Map<String, T> javaMap( Function<Value, T> mapFunction );

    /** @return the value as an {@link Identity}, if possible. */
    Identity asIdentity();

    /** @return the value as a {@link Node}, if possible. */
    Node asNode();

    /** @return the value as a {@link Relationship}, if possible. */
    Relationship asRelationship();

    /** @return the value as a {@link Path}, if possible. */
    Path asPath();

    /**
     * Retrieve an inner value by index. This can be used for {@link #isList() lists}.
     *
     * @param index the index to look up a value by
     * @return the value at the specified index
     */
    Value get( long index );

    /**
     * Retrieve an inner value by key. This can be used for {@link #isMap() maps},
     * {@link #isNode() nodes} and {@link #isRelationship() relationships}. For nodes and relationships, this method
     * returns property values. If no value could be found with the specified key, then null will be returned.
     *
     * @param key the key to find a value by
     * @return the value with the specified key or null if no value could be found with the key
     */
    Value get( String key );

    /**
     * If the underlying value is a collection type, return the number of values in the collection.
     * <p>
     * For {@link #isList() list} values, this will return the size of the list.
     * <p>
     * For {@link #isMap() map} values, this will return the number of entries in the map.
     * <p>
     * For {@link #isNode() node} and {@link #isRelationship() relationship} values,
     * this will return the number of properties.
     * <p>
     * For {@link #isPath() path} values, this returns the length (number of relationships) in the path.
     *
     * @return the number of values in an underlying collection
     */
    long size();

    /**
     * If the underlying value supports {@link #get(String) key-based indexing}, return an iterable of the keys in the
     * map, this applies to {@link #isMap() map}, {@link #asNode() node} and {@link
     * #isRelationship() relationship} values.
     *
     * @return the keys in the value
     */
    Iterable<String> keys();

    /** @return true if the underlying value is a Neo4j string value */
    boolean isString();

    /** @return if the underlying value is a Neo4j 64-bit integer */
    boolean isInteger();

    /** @return if the underlying value is a Neo4j 64-bit float */
    boolean isFloat();

    /** @return if the underlying value is a Neo4j boolean */
    boolean isBoolean();

    /** @return if the underlying value is a Neo4j identity */
    boolean isIdentity();

    /** @return if the underlying value is a Neo4j node */
    boolean isNode();

    /** @return if the underlying value is a Neo4j path */
    boolean isPath();

    /** @return if the underlying value is a Neo4j relationship */
    boolean isRelationship();

    /**
     * Lists are an ordered collection of values. You can {@link #iterator() iterate} over a list as well as
     * access specific values {@link #get(long) by index}.
     * <p>
     * {@link #size()} will give you the number of entries in the list.
     *
     * @return if the underlying value is a Neo4j list
     */
    boolean isList();

    /**
     * Maps are key/value objects, similar to {@link java.util.Map java maps}. You can use {@link #get(String)} to
     * retrive values from the map, {@link #keys()} to list keys and {@link #iterator()} to iterate over the values.
     * <p>
     * {@link #size()} will give you the number of entries in the map.
     *
     * @return if the underlying value is a Neo4j map
     */
    boolean isMap();
}
