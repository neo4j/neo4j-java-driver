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

import org.neo4j.driver.internal.value.NullValue;
import org.neo4j.driver.v1.exceptions.NoRecordException;

/**
 * Access an underlying record (which is an ordered map of fields)
 *
 * This provides only read methods. Subclasses may chose to provide additional methods
 * for changing the underlying record.
 *
 * @see Pair
 */
public interface RecordAccessor extends ListAccessor
{
    /**
     * Retrieve the keys of the underlying map
     *
     * @return all map keys in unspecified order
     */
    List<String> keys();

    /**
     * Check if the list of keys contains the given key
     *
     * @param key the key
     * @return <tt>true</tt> if this map keys contains the given key otherwise <tt>false</tt>
     */
    boolean containsKey( String key );

    /**
     * Retrieve the index of the field with the given key
     *
     * @throws java.util.NoSuchElementException if the given key is not from {@link #keys()}
     * @param key the give key
     * @return the index of the field as used by {@link #value(int)}
     */
    int index( String key );

    /**
     * Retrieve the value of the property with the given key
     *
     * @param key the key of the property
     * @return the property's value or a {@link NullValue} if no such key exists
     * @throws NoRecordException if the associated underlying record is not available
     */
    Value value( String key );

    /**
     * Retrieve the number of fields in this record
     *
     * @return the number of fields in this record
     */
    @Override
    int size();

    /**
     * Retrieve all record fields
     *
     * @return all fields in key order
     * @throws NoRecordException if the associated underlying record is not available
     */
    List<Pair<String, Value>> fields();

    /**
     * @return if this record accessor is currently associated with an underlying record
     */
    boolean hasRecord();

    /**
     * @throws NoRecordException if the associated underlying record is not available
     * @return an immutable copy of the currently associated underlying record
     */
    Record record();
}
