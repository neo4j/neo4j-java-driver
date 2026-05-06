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
package org.neo4j.driver.internal;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

class InternalVectorTest {

    @ParameterizedTest
    @MethodSource("vectors")
    void shouldRespectEqualityAndHashCode(AbstractArrayVector<?> vector1, AbstractArrayVector<?> vector2) {
        assertEquals(vector1, vector2);
        assertEquals(vector1.hashCode(), vector2.hashCode());
    }

    private static Stream<Arguments> vectors() {
        return Stream.of(
                Arguments.of(new InternalInt8Vector(new byte[] {1, 2, 3}), new InternalInt8Vector(new byte[] {1, 2, 3})),
                Arguments.of(
                        new InternalInt16Vector(new short[] {1, 2, 3}),
                        new InternalInt16Vector(new short[] {1, 2, 3})),
                Arguments.of(new InternalInt32Vector(new int[] {1, 2, 3}), new InternalInt32Vector(new int[] {1, 2, 3})),
                Arguments.of(
                        new InternalInt64Vector(new long[] {1L, 2L, 3L}),
                        new InternalInt64Vector(new long[] {1L, 2L, 3L})),
                Arguments.of(
                        new InternalFloat32Vector(new float[] {1.1F, 2.2F, 3.3F}),
                        new InternalFloat32Vector(new float[] {1.1F, 2.2F, 3.3F})),
                Arguments.of(
                        new InternalFloat64Vector(new double[] {1.1D, 2.2D, 3.3D}),
                        new InternalFloat64Vector(new double[] {1.1D, 2.2D, 3.3D})));
    }
}