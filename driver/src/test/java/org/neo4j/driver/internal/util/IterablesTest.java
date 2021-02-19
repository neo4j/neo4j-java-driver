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
package org.neo4j.driver.internal.util;

import org.junit.jupiter.api.Test;

import java.util.Queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IterablesTest
{
    @Test
    void shouldCreateHashMapWithExpectedSize()
    {
        assertNotNull( Iterables.newHashMapWithSize( 42 ) );
    }

    @Test
    void shouldCreateLinkedHashMapWithExpectedSize()
    {
        assertNotNull( Iterables.newLinkedHashMapWithSize( 42 ) );
    }

    @Test
    void shouldThrowWhenNegativeHashMapSizeGiven()
    {
        assertThrows( IllegalArgumentException.class, () -> Iterables.newHashMapWithSize( -42 ) );
    }

    @Test
    void shouldThrowWhenNegativeLinkedHashMapSizeGiven()
    {
        assertThrows( IllegalArgumentException.class, () -> Iterables.newLinkedHashMapWithSize( -42 ) );
    }

    @Test
    void shouldReturnEmptyQueue()
    {
        Queue<Object> queue = Iterables.emptyQueue();
        assertEquals( 0, queue.size() );
        assertTrue( queue.isEmpty() );
        assertNull( queue.peek() );
        assertNull( queue.poll() );

        assertThrows( UnsupportedOperationException.class, () -> queue.add( "Hello" ) );
        assertThrows( UnsupportedOperationException.class, () -> queue.offer( "World" ) );
    }

    @Test
    void shouldReturnSameEmptyQueue()
    {
        assertSame( Iterables.emptyQueue(), Iterables.emptyQueue() );
    }
}
