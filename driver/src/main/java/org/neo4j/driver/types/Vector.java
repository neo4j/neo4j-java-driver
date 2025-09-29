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

/**
 * Represents Neo4j Vector type.
 * <p>
 * Values that represent vectors be created using the following methods:
 * <ul>
 *     <li>{@link Values#vector(byte[])} - returns {@link Int8Vector}</li>
 *     <li>{@link Values#vector(short[])} - returns {@link Int16Vector}</li>
 *     <li>{@link Values#vector(int[])} - returns {@link Int32Vector}</li>
 *     <li>{@link Values#vector(long[])} - returns {@link Int64Vector}</li>
 *     <li>{@link Values#vector(float[])} - returns {@link Float32Vector}</li>
 *     <li>{@link Values#vector(double[])} - returns {@link Float64Vector}</li>
 * </ul>
 *
 * @see Values
 * @since 6.0.0
 */
public sealed interface Vector permits Int8Vector, Int16Vector, Int32Vector, Int64Vector, Float32Vector, Float64Vector {
    /**
     * Returns the element type.
     *
     * @return the element type
     */
    Class<?> elementType();

    /**
     * Returns the length.
     *
     * @return the length
     */
    int length();
}
