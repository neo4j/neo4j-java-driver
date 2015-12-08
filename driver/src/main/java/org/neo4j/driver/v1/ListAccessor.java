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

import org.neo4j.driver.v1.exceptions.ClientException;

/**
 * Access an underlying list of values by index
 *
 * This provides only read methods. Subclasses may chose to provide additional methods
 * for changing the underlying list.
 * *
 * @see Value
 */
public interface ListAccessor
{
    /**
     * Retrieve the value at the given index
     *
     * @param index the index of the value
     * @return the value or a {@link org.neo4j.driver.internal.value.NullValue} if the index is out of bounds
     * @throws ClientException if record has not been initialized
     */
    Value value( int index );
}


