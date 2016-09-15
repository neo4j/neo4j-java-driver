/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.driver.internal.util;


import org.junit.Test;

import java.util.Comparator;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class ConcurrentRingSetTest
{

    @Test
    public void shouldBeAbleToIterateIndefinitely()
    {
        // Given
        ConcurrentRingSet<Integer> integers = new ConcurrentRingSet<>();

        // When
        integers.addAll( asList( 0, 1, 2, 3, 4 ) );

        // Then
        for ( int i = 0; i < 100; i++ )
        {
            assertThat(integers.next(), equalTo( i % 5));
        }
    }

    @Test
    public void shouldBeAbleToUseCustomComparator()
    {
        // Given
        ConcurrentRingSet<Integer> integers = new ConcurrentRingSet<>( new Comparator<Integer>()
        {
            @Override
            public int compare( Integer o1, Integer o2 )
            {
                return Integer.compare( o2, o1 );
            }
        } );

        // When
        integers.addAll( asList( 0, 1, 2, 3, 4 ) );

        // Then
        assertThat(integers.next(), equalTo( 4));
        assertThat(integers.next(), equalTo( 3));
        assertThat(integers.next(), equalTo( 2));
        assertThat(integers.next(), equalTo( 1));
        assertThat(integers.next(), equalTo( 0));
        assertThat(integers.next(), equalTo( 4));
        assertThat(integers.next(), equalTo( 3));
        //....
    }
}