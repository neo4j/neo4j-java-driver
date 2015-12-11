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

import java.util.Map;

/**
 * A record is an immutable copy of an ordered map
 *
 * @see Result#record()
 * @see Result#retain()
 */
@Immutable
public interface Record extends RecordAccessor
{
    /**
     * @return the value as a value map
     */
    Map<String, Value> asMap();

    /**
     * @param mapFunction a function to map from Value to T. See {@link Values} for some predefined functions, such
     * as {@link Values#valueAsBoolean()}, {@link Values#valueAsList(Function)}.
     * @param <T> the type of map values
     * @return the value as a map from string keys to values of type T
     */
    <T> Map<String, T> asMap( Function<Value, T> mapFunction );

    // Force implementation
    @Override
    boolean equals( Object other );

    // Force implementation
    @Override
    int hashCode();
}
