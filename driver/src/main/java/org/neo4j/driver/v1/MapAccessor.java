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

import org.neo4j.driver.internal.value.NullValue;
import org.neo4j.driver.v1.exceptions.ClientException;

/**
 * Access the keys, properties and values of an underlying unordered map by key
 *
 * This provides only read methods. Subclasses may chose to provide additional methods
 * for changing the underlying map.
 */
public interface MapAccessor
{
    /**
     * Retrieve the keys of the underlying map
     *
     * @return all map keys in unspecified order
     */
    Iterable<String> keys();

    /**
     * Check if the list of keys contains the given key
     *
     * @param key the key
     * @return <tt>true</tt> if this map keys contains the given key otherwise <tt>false</tt>
     */
    boolean containsKey( String key );

    /**
     * Retrieve the value of the property with the given key
     *
     * @param key the key of the property
     * @return the property's value or a {@link NullValue} if no such key exists
     * @throws ClientException if record has not been initialized
     */
    Value value( String key );
}
