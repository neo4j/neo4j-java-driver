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
package org.neo4j.driver.internal.bolt.routedimpl.cluster.loadbalancing;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class RoundRobinArrayIndexTest {
    @Test
    void shouldHandleZeroLength() {
        var roundRobinIndex = new RoundRobinArrayIndex();

        var index = roundRobinIndex.next(0);

        assertEquals(-1, index);
    }

    @Test
    void shouldReturnIndexesInRoundRobinOrder() {
        var roundRobinIndex = new RoundRobinArrayIndex();

        for (var i = 0; i < 10; i++) {
            var index = roundRobinIndex.next(10);
            assertEquals(i, index);
        }

        for (var i = 0; i < 5; i++) {
            var index = roundRobinIndex.next(5);
            assertEquals(i, index);
        }
    }

    @Test
    void shouldHandleOverflow() {
        var arrayLength = 10;
        var roundRobinIndex = new RoundRobinArrayIndex(Integer.MAX_VALUE - 1);

        assertEquals((Integer.MAX_VALUE - 1) % arrayLength, roundRobinIndex.next(arrayLength));
        assertEquals(Integer.MAX_VALUE % arrayLength, roundRobinIndex.next(arrayLength));
        assertEquals(0, roundRobinIndex.next(arrayLength));
        assertEquals(1, roundRobinIndex.next(arrayLength));
        assertEquals(2, roundRobinIndex.next(arrayLength));
    }
}
