/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.NoSuchRecordException;
import org.neo4j.driver.types.MapAccessorWithDefaultValue;
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
public interface Record extends MapAccessorWithDefaultValue {
    /**
     * Retrieve the keys of the underlying map
     *
     * @return all field keys in order
     */
    @Override
    List<String> keys();

    /**
     * Retrieve the values of the underlying map
     *
     * @return all field keys in order
     */
    @Override
    List<Value> values();

    /**
     * Retrieve the index of the field with the given key
     *
     * @throws java.util.NoSuchElementException if the given key is not from {@link #keys()}
     * @param key the give key
     * @return the index of the field as used by {@link #get(int)}
     */
    int index(String key);

    /**
     * Retrieve the value at the given field index
     *
     * @param index the index of the value
     * @return the value or a {@link org.neo4j.driver.internal.value.NullValue} if the index is out of bounds
     * @throws ClientException if record has not been initialized
     */
    Value get(int index);

    /**
     * Retrieve all record fields
     *
     * @return all fields in key order
     * @throws NoSuchRecordException if the associated underlying record is not available
     */
    List<Pair<String, Value>> fields();
}
