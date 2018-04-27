/*
 * Copyright (c) 2002-2018 Neo4j Sweden AB [http://neo4j.com]
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

import org.junit.Test;

import java.util.Queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IterablesTest
{
    @Test
    public void shouldCreateHashMapWithExpectedSize()
    {
        assertNotNull( Iterables.newHashMapWithSize( 42 ) );
    }

    @Test
    public void shouldCreateLinkedHashMapWithExpectedSize()
    {
        assertNotNull( Iterables.newLinkedHashMapWithSize( 42 ) );
    }

    @Test
    public void shouldThrowWhenNegativeHashMapSizeGiven()
    {
        try
        {
            Iterables.newHashMapWithSize( -42 );
            fail( "Exception expected" );
        }
        catch ( IllegalArgumentException ignore )
        {
        }
    }

    @Test
    public void shouldThrowWhenNegativeLinkedHashMapSizeGiven()
    {
        try
        {
            Iterables.newLinkedHashMapWithSize( -42 );
            fail( "Exception expected" );
        }
        catch ( IllegalArgumentException ignore )
        {
        }
    }

    @Test
    public void shouldReturnEmptyQueue()
    {
        Queue<Object> queue = Iterables.emptyQueue();
        assertEquals( 0, queue.size() );
        assertTrue( queue.isEmpty() );
        assertNull( queue.peek() );
        assertNull( queue.poll() );

        try
        {
            queue.add( "Hello" );
            fail( "Exception expected" );
        }
        catch ( UnsupportedOperationException ignore )
        {
        }

        try
        {
            queue.offer( "World" );
            fail( "Exception expected" );
        }
        catch ( UnsupportedOperationException ignore )
        {
        }
    }

    @Test
    public void shouldReturnSameEmptyQueue()
    {
        assertSame( Iterables.emptyQueue(), Iterables.emptyQueue() );
    }
}
