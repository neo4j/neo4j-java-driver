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

import org.neo4j.driver.v1.exceptions.value.LossyCoercion;
import org.neo4j.driver.v1.exceptions.value.Uncoercible;

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
public interface Value extends PropertyMapAccessor, ListAccessor
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
     * Test if the underlying collection is empty
     *
     * @return <tt>true</tt> if size() is 0, otherwise <tt>false</tt>
     */
    boolean isEmpty();

    /**
     * If the underlying value supports {@link #value(String) key-based indexing}, return an iterable of the keys in the
     * map, this applies to {@link TypeSystem#MAP() map}, {@link #asNode() node} and {@link
     * TypeSystem#RELATIONSHIP()}  relationship} values.
     *
     * @return the keys in the value
     */
    @Override
    Iterable<String> keys();

    /** @return The type of this value as defined in the Neo4j type system */
    Type type();

    /**
     * Test if this value is a value of the given type
     *
     * @param type the given type
     * @return type.isTypeOf( this )
     */
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

    /** @return the value as a Java Object */
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
     * @return the value as a Java char, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
    char asChar();

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
     * Returns a Java short if no precision is lost in the conversion.
     *
     * @return the value as a Java short.
     * @throws LossyCoercion if it is not possible to convert the value without loosing precision.
     * @throws Uncoercible if value types are incompatible.
     */
    short asShort();

    /**
     * Returns a Java byte if no precision is lost in the conversion.
     *
     * @return the value as a Java byte.
     * @throws LossyCoercion if it is not possible to convert the value without loosing precision.
     * @throws Uncoercible if value types are incompatible.
     */
    byte asByte();

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
     * @return the value as an {@link Identity}, if possible.
     * @throws Uncoercible if value types are incompatible.
     */
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

    /**
     * If the Value is for example a List, returns the value as an array of values instead.
     * @return an array of Values.
     * @throws Uncoercible if the Value cannot be turned into an array.
     */
    Value[] asArray();


    /**
     * Map the value with provided function. See {@link Values} for some predefined functions, such
     * as {@link Values#valueAsBoolean()}, {@link Values#valueAsList(Function)}.
     *
     * @param clazz the class of T
     * @param mapFunction a function mapping Values into T
     * @param <T> The type of the array
     * @return an array of T
     * @throws Uncoercible if the Value cannot be turned into an array.
     */
    <T> T[] asArray( Class<T> clazz, Function<Value, T> mapFunction );

    /**
     * @return the value as an array of chars.
     * @throws Uncoercible if the value cannot be coerced to a char array.
     */
    char[] asCharArray();

    /**
     * @return the value as an array of longs.
     * @throws Uncoercible if the value cannot be coerced to a long array.
     */
    long[] asLongArray();

    /**
     * @return the value as an array of ints.
     * @throws Uncoercible if the value cannot be coerced to a int array.
     */
    int[] asIntArray();

    /**
     * @return the value as an array of shorts.
     * @throws Uncoercible if the value cannot be coerced to a short array.
     */
    short[] asShortArray();

    /**
     * @return the value as an array of bytes.
     * @throws Uncoercible if the value cannot be coerced to a byte array.
     */
    byte[] asByteArray();

    /**
     * @return the value as an array of doubles.
     * @throws Uncoercible if the value cannot be coerced to a double array.
     */
    double[] asDoubleArray();

    /**
     * @return the value as an array of floats.
     * @throws Uncoercible if the value cannot be coerced to a float array.
     */
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
}
