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
 * Access the properties of an underlying unordered map
 *
 * This provides only read methods. Subclasses may chose to provide additional methods
 * for changing the underlying map.
 */
public interface PropertyMapAccessor extends MapAccessor
{
    /**
     * Retrieve all values of the underlying collection
     *
     * @return all values in unspecified order
     */
    Iterable<Value> values();

    /**
     * Map and retrieve all values of the underlying collection
     *
     * @param mapFunction a function to map from Value to T. See {@link Values} for some predefined functions, such
     * as {@link Values#valueAsBoolean()}, {@link Values#valueAsList(Function)}.
     * @param <T> the target type of mapping
     * @return the result of mapping all values in unspecified order
     */
    <T> Iterable<T> values( Function<Value, T> mapFunction );

    /**
     * @return number of properties in this property map
     */
    int propertyCount();

    /**
     * Retrieve the property for the given key
     *
     * @param key the key of the property
     * @return the property for the given key or a property with a null value if the key is unknown
     */
    Property property( String key );

    /**
     * Retrieve all properties of the underlying map
     *
     * @see Property
     * @return all properties in unspecified order
     */
    Iterable<Property<Value>> properties();

    /**
     * Retrieve all properties of the underlying map
     *
     * @see Property
     * @param mapFunction a function to map from Value to T. See {@link Values} for some predefined functions, such
     * as {@link Values#valueAsBoolean()}, {@link Values#valueAsList(Function)}.
     * @param <V> the target type of mapping
     * @return all mapped properties in unspecified order
     */
    <V> Iterable<Property<V>> properties( Function<Value, V> mapFunction );
}
