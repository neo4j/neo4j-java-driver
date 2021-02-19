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

import java.util.List;
import java.util.Map;

import org.neo4j.driver.internal.value.NullValue;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.NoSuchRecordException;
import org.neo4j.driver.types.MapAccessorWithDefaultValue;
import java.util.function.Function;
import org.neo4j.driver.util.Immutable;
import org.neo4j.driver.util.Pair;

/**
 * Container for Cypher result values.
 * <p>
 * Streams of records are returned from Cypher query execution, contained
 * within a {@link Result}.
 * <p>
 * A record is a form of ordered map and, as such, contained values can be
 * accessed by either positional {@link #get(int) index} or textual
 * {@link #get(String) key}.
 *
 * @since 1.0
 */
@Immutable
public interface Record extends MapAccessorWithDefaultValue
{
    /**
     * Retrieve the keys of the underlying map
     *
     * @return all field keys in order
     */
    List<String> keys();

    /**
     * Retrieve the values of the underlying map
     *
     * @return all field keys in order
     */
    List<Value> values();

    /**
     * Check if the list of keys contains the given key
     *
     * @param key the key
     * @return {@code true} if this map keys contains the given key otherwise {@code false}
     */
    boolean containsKey( String key );

    /**
     * Retrieve the index of the field with the given key
     *
     * @throws java.util.NoSuchElementException if the given key is not from {@link #keys()}
     * @param key the give key
     * @return the index of the field as used by {@link #get(int)}
     */
    int index( String key );

    /**
     * Retrieve the value of the property with the given key
     *
     * @param key the key of the property
     * @return the property's value or a {@link NullValue} if no such key exists
     * @throws NoSuchRecordException if the associated underlying record is not available
     */
    Value get( String key );

    /**
     * Retrieve the value at the given field index
     *
     * @param index the index of the value
     * @return the value or a {@link org.neo4j.driver.internal.value.NullValue} if the index is out of bounds
     * @throws ClientException if record has not been initialized
     */
    Value get( int index );

    /**
     * Retrieve the number of fields in this record
     *
     * @return the number of fields in this record
     */
    int size();

    /**
     * Return this record as a map, where each value has been converted to a default
     * java object using {@link Value#asObject()}.
     *
     * This is equivalent to calling {@link #asMap(Function)} with {@link Values#ofObject()}.
     *
     * @return this record as a map
     */
    Map<String, Object> asMap();

    /**
     * Return this record as a map, where each value has been converted using the provided
     * mapping function. You can find a library of common mapping functions in {@link Values}.
     *
     * @see Values for a long list of built-in conversion functions
     * @param mapper the mapping function
     * @param <T> the type to convert to
     * @return this record as a map
     */
    <T> Map<String, T> asMap( Function<Value, T> mapper );

    /**
     * Retrieve all record fields
     *
     * @return all fields in key order
     * @throws NoSuchRecordException if the associated underlying record is not available
     */
    List<Pair<String, Value>> fields();

}
