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

/**
 * Access to the interior elements of an underlying collection like
 * data structure
 *
 * Each collection element represents (includes or is) a {@link Value}
 */
public interface CollectionLike
{
    /**
     * Get the size of the underlying collection as a Java int
     *
     * @return the number of accessible elements of the underlying collection
     */
    int countElements();

    /**
     * Test if there is at least one element available
     *
     * @return <tt>true</tt> if elementCount() is larger than 0, otherwise <tt>false</tt>
     */
    boolean hasElements();

    /**
     * Retrieve all values of the underlying collection
     *
     * @return all values in unspecified order
     */
    Iterable<Value> values();

    /**
     * Map and retrieve all field values of the underlying collection
     *
     * @param mapFunction a function to map from Value to T. See {@link Values} for some predefined functions, such
     * as {@link Values#valueAsBoolean()}, {@link Values#valueAsList(Function)}.
     * @param <T> the type of collection elements
     * @return the result of mapping all field values in unspecified order
     */
    <T> Iterable<T> values( Function<Value, T> mapFunction );
}
