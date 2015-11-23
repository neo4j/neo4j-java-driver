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
 * A record is a collection of named fields, and is what makes up the individual items in a {@link
 * Result}
 */
public interface Record
{
    /**
     * From the current record the result is pointing to, retrieve the value in the specified field.
     *
     * @param fieldIndex the field index into the current record
     * @return the value in the specified field
     */
    Value get( int fieldIndex );

    /**
     * From the current record the result is pointing to, retrieve the value in the specified field.
     * If no value could be found in the specified filed, null will be returned.
     *
     * @param fieldName the field to retrieve the value from
     * @return the value in the specified field or null if no value could be found in the field.
     */
    Value get( String fieldName );

    /**
     * Get an ordered sequence of the field names in this result.
     *
     * @return field names
     */
    Iterable<String> fieldNames();

}
