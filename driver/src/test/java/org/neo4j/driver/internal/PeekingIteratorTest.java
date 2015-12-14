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
package org.neo4j.driver.internal;

import java.util.Collections;
import java.util.NoSuchElementException;

import org.junit.Test;

import static java.util.Arrays.asList;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PeekingIteratorTest
{
    @Test
    public void shouldSupportDiscard()
    {
        // GIVEN
        PeekingIterator<Integer> iter = new PeekingIterator<>( asList( 1, 2, 3 ).iterator() );
        iter.next();

        // WHEN
        iter.discard();

        // THEN
        assertFalse( iter.hasNext() );
        assertNull( iter.peek() );
        assertNoNext( iter );
    }

    @Test
    public void shouldNotPeekEmptyIterator()
    {
        // GIVEN
        PeekingIterator<Integer> iter = new PeekingIterator<>( Collections.<Integer>emptyIterator() );

        // THEN
        assertFalse( iter.hasNext() );
        assertNull( iter.peek() );
        assertNoNext( iter );
    }

    @Test
    public void shouldPeekNonEmptyIterator()
    {
        // GIVEN
        PeekingIterator<Integer> iter = new PeekingIterator<>( asList( 1, 2, 3 ).iterator() );

        // THEN
        assertTrue( iter.hasNext() );
        assertThat( iter.peek(), equalTo( (Object) 1 ) );

        // WHEN
        iter.next();

        // THEN
        assertTrue( iter.hasNext() );
        assertThat( iter.peek(), equalTo( (Object) 2 ) );

        // WHEN
        iter.next();

        // THEN
        assertTrue( iter.hasNext() );
        assertThat( iter.peek(), equalTo( (Object) 3 ) );

        // WHEN
        iter.next();
        assertFalse( iter.hasNext() );
        assertNull( iter.peek() );
        assertNoNext( iter );
    }

    private void assertNoNext( PeekingIterator iter )
    {
        try
        {
            iter.next();
            fail( "Expected NoSuchElementException" );
        }
        catch ( NoSuchElementException e )
        {
            // yay
        }
    }
}
