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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.driver.Values;

class VectorTest {

    static Stream<Arguments> shouldReturnNeo4jVectorString() {

        var longDoubleArray = ThreadLocalRandom.current().doubles(4096).toArray();
        var doubles = Arrays.stream(longDoubleArray).mapToObj(Double::toString).collect(Collectors.joining(", "));

        return Stream.of(
                Arguments.of(
                        "vector([], 0, INTEGER8 NOT NULL)",
                        Values.vector(new byte[0]).as(Int8Vector.class)),
                Arguments.of(
                        "vector([0], 1, INTEGER8 NOT NULL)",
                        Values.vector(new byte[1]).as(Int8Vector.class)),
                Arguments.of(
                        "vector([0, 0], 2, INTEGER16 NOT NULL)",
                        Values.vector((new short[2])).as(Int16Vector.class)),
                Arguments.of(
                        "vector([0, 0, 0], 3, INTEGER32 NOT NULL)",
                        Values.vector((new int[3])).as(Int32Vector.class)),
                Arguments.of(
                        "vector([0, 0, 0, 0], 4, INTEGER NOT NULL)",
                        Values.vector((new long[4])).as(Int64Vector.class)),
                Arguments.of(
                        "vector([0.0, 0.0, 0.0, 0.0, 0.0], 5, FLOAT32 NOT NULL)",
                        Values.vector((new float[5])).as(Float32Vector.class)),
                Arguments.of(
                        "vector([0.0, 0.0, -Infinity, Infinity, NaN, NaN], 6, FLOAT NOT NULL)",
                        Values.vector((new double[] {
                                    0.0,
                                    0.0,
                                    Double.NEGATIVE_INFINITY,
                                    Double.POSITIVE_INFINITY,
                                    Double.longBitsToDouble(0xFFF8000000000000L),
                                    Double.NaN
                                }))
                                .as(Float64Vector.class)),
                Arguments.of(
                        "vector([%s], 4096, FLOAT NOT NULL)".formatted(doubles),
                        Values.vector((longDoubleArray)).as(Float64Vector.class)));
    }

    @ParameterizedTest
    @MethodSource
    void shouldReturnNeo4jVectorString(String expected, Vector vector) {
        assertEquals(expected, vector.toString());
    }
}
