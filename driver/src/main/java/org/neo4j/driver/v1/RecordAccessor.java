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

import org.neo4j.driver.v1.exceptions.ClientException;

/**
 * Access an underlying record (which is an ordered map of fields)
 *
 * This provides only read methods. Subclasses may chose to provide additional methods
 * for changing the underlying record.
 *
 * @see Field
 */
public interface RecordAccessor extends ListAccessor, MapAccessor
{
    @Override
    List<String> keys();

    /**
     * @return number of fields in this record
     */
    int fieldCount();

    /**
     * Retrieve all record fields
     *
     * @return all fields in key order
     * @throws ClientException if record has not been initialized.
     */
    List<Field<Value>> fields();

    /**
     * Map and retrieve all record fields
     *
     * @param <V> the target type of the map function
     * @param mapFunction a function to map from Value to T. See {@link Values} for some predefined functions, such
     * as {@link Values#valueAsBoolean()}, {@link Values#valueAsList(Function)}.
     * @return the result of mapping all record fields in key order
     * @throws ClientException if record has not been initialized.
     */
    <V> List<Field<V>> fields( Function<Value, V> mapFunction );

    /**
     * @return an immutable copy of the currently viewed record
     */
    Record record();
}
