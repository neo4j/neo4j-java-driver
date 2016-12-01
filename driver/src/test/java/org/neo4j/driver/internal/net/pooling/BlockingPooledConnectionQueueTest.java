/*
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
package org.neo4j.driver.internal.net.pooling;


import org.junit.Test;

import org.neo4j.driver.internal.util.Supplier;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BlockingPooledConnectionQueueTest
{
    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldCreateNewConnectionWhenEmpty()
    {
        // Given
        PooledConnection connection = mock( PooledConnection.class );
        Supplier<PooledConnection> supplier = mock( Supplier.class );
        when( supplier.get() ).thenReturn( connection );
        BlockingPooledConnectionQueue queue = new BlockingPooledConnectionQueue( 10 );

        // When
        queue.acquire( supplier );

        // Then
        verify( supplier ).get();
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldNotCreateNewConnectionWhenNotEmpty()
    {
        // Given
        PooledConnection connection = mock( PooledConnection.class );
        Supplier<PooledConnection> supplier = mock( Supplier.class );
        when( supplier.get() ).thenReturn( connection );
        BlockingPooledConnectionQueue queue = new BlockingPooledConnectionQueue( 1 );
        queue.offer( connection );

        // When
        queue.acquire( supplier );

        // Then
        verify( supplier, never() ).get();
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldTerminateAllSeenConnections()
    {
        // Given
        PooledConnection connection1 = mock( PooledConnection.class );
        PooledConnection connection2 = mock( PooledConnection.class );
        Supplier<PooledConnection> supplier = mock( Supplier.class );
        when( supplier.get() ).thenReturn( connection1 );
        BlockingPooledConnectionQueue queue = new BlockingPooledConnectionQueue( 2 );
        queue.offer( connection1 );
        queue.offer( connection2 );
        assertThat( queue.size(), equalTo( 2 ) );

        // When
        queue.acquire( supplier );
        assertThat( queue.size(), equalTo( 1 ) );
        queue.terminate();

        // Then
        verify( connection1 ).dispose();
        verify( connection2 ).dispose();
    }

    @Test
    public void shouldNotAcceptWhenFull()
    {
        // Given
        PooledConnection connection1 = mock( PooledConnection.class );
        PooledConnection connection2 = mock( PooledConnection.class );
        BlockingPooledConnectionQueue queue = new BlockingPooledConnectionQueue( 1 );

        // Then
        assertTrue(queue.offer( connection1 ));
        assertFalse(queue.offer( connection2 ));
    }
}