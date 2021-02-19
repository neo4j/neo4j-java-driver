/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.driver.internal.cluster.loadbalancing;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RoundRobinArrayIndexTest
{
    @Test
    void shouldHandleZeroLength()
    {
        RoundRobinArrayIndex roundRobinIndex = new RoundRobinArrayIndex();

        int index = roundRobinIndex.next( 0 );

        assertEquals( -1, index );
    }

    @Test
    void shouldReturnIndexesInRoundRobinOrder()
    {
        RoundRobinArrayIndex roundRobinIndex = new RoundRobinArrayIndex();

        for ( int i = 0; i < 10; i++ )
        {
            int index = roundRobinIndex.next( 10 );
            assertEquals( i, index );
        }

        for ( int i = 0; i < 5; i++ )
        {
            int index = roundRobinIndex.next( 5 );
            assertEquals( i, index );
        }
    }

    @Test
    void shouldHandleOverflow()
    {
        int arrayLength = 10;
        RoundRobinArrayIndex roundRobinIndex = new RoundRobinArrayIndex( Integer.MAX_VALUE - 1 );

        assertEquals( (Integer.MAX_VALUE - 1) % arrayLength, roundRobinIndex.next( arrayLength ) );
        assertEquals( Integer.MAX_VALUE % arrayLength, roundRobinIndex.next( arrayLength ) );
        assertEquals( 0, roundRobinIndex.next( arrayLength ) );
        assertEquals( 1, roundRobinIndex.next( arrayLength ) );
        assertEquals( 2, roundRobinIndex.next( arrayLength ) );
    }
}
