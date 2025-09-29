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
package org.neo4j.driver.types;

import org.neo4j.driver.Values;
import org.neo4j.driver.internal.InternalInt64Vector;

/**
 * Represents Neo4j Vector type that holds a sequence of {@code long} values.
 *
 * @since 6.0.0
 * @see Vector
 * @see Values
 */
public sealed interface Int64Vector extends Vector permits InternalInt64Vector {
    /**
     * Returns array with vector elements.
     *
     * @return the array with vector elements
     */
    long[] toArray();

    /**
     * Returns Neo4j Vector as a {@link String}.
     * <p>
     * For example: <pre>vector([0], 1, INTEGER NOT NULL)</pre>
     *
     * @return the string value
     */
    @Override
    String toString();
}
