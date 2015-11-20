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
 * A uniquely identifiable property container that can form part of a Neo4j graph.
 */
public interface Entity
{
    /**
     * A unique {@link Identity identity} for this Entity. Identities are guaranteed
     * to remain stable for the duration of the session they were found in, but may be re-used for other
     * entities after that. As such, if you want a public identity to use for your entities, attaching
     * an explicit 'id' property or similar persistent and unique identifier is a better choice.
     *
     * @return an identity object
     */
    Identity identity();

    /**
     * Return all property keys.
     *
     * @return a property key Collection
     */
    Iterable<String> propertyKeys();

    /**
     * Number of properties in this entity.
     *
     * @return the number of properties this entity contains.
     */
    int propertyCount();

    /**
     * Return a specific property {@link Value}. If no value could be found with the specified key,
     * null will be returned.
     *
     * @param key a property key
     * @return the property value or null
     */
    Value property( String key );
}
