/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.PooledConnection;
import org.neo4j.driver.internal.util.Consumer;
import org.neo4j.driver.internal.util.Supplier;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Logging;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.net.BoltServerAddress.LOCAL_DEFAULT;
import static org.neo4j.driver.internal.util.Clock.SYSTEM;

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
        BlockingPooledConnectionQueue queue = newConnectionQueue( 10 );

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
        BlockingPooledConnectionQueue queue = newConnectionQueue( 1 );
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
        BlockingPooledConnectionQueue queue = newConnectionQueue( 2 );
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
        BlockingPooledConnectionQueue queue = newConnectionQueue( 1 );

        // Then
        assertTrue( queue.offer( connection1 ) );
        assertFalse( queue.offer( connection2 ) );
    }

    @Test
    public void shouldDisposeAllConnectionsWhenOneOfThemFailsToDispose()
    {
        BlockingPooledConnectionQueue queue = newConnectionQueue( 5 );

        PooledConnection connection1 = mock( PooledConnection.class );
        PooledConnection connection2 = mock( PooledConnection.class );
        PooledConnection connection3 = mock( PooledConnection.class );

        RuntimeException disposeError = new RuntimeException( "Failed to stop socket" );
        doThrow( disposeError ).when( connection2 ).dispose();

        queue.offer( connection1 );
        queue.offer( connection2 );
        queue.offer( connection3 );

        queue.terminate();

        verify( connection1 ).dispose();
        verify( connection2 ).dispose();
        verify( connection3 ).dispose();
    }

    @Test
    @SuppressWarnings( "unchecked" )
    public void shouldTryToCloseAllUnderlyingConnections()
    {
        BlockingPooledConnectionQueue queue = newConnectionQueue( 5 );

        Connection connection1 = mock( Connection.class );
        Connection connection2 = mock( Connection.class );
        Connection connection3 = mock( Connection.class );

        RuntimeException closeError1 = new RuntimeException( "Failed to close 1" );
        RuntimeException closeError2 = new RuntimeException( "Failed to close 2" );
        RuntimeException closeError3 = new RuntimeException( "Failed to close 3" );

        doThrow( closeError1 ).when( connection1 ).close();
        doThrow( closeError2 ).when( connection2 ).close();
        doThrow( closeError3 ).when( connection3 ).close();

        PooledConnection pooledConnection1 = new PooledSocketConnection( connection1, mock( Consumer.class ), SYSTEM );
        PooledConnection pooledConnection2 = new PooledSocketConnection( connection2, mock( Consumer.class ), SYSTEM );
        PooledConnection pooledConnection3 = new PooledSocketConnection( connection3, mock( Consumer.class ), SYSTEM );

        queue.offer( pooledConnection1 );
        queue.offer( pooledConnection2 );
        queue.offer( pooledConnection3 );

        queue.terminate();

        verify( connection1 ).close();
        verify( connection2 ).close();
        verify( connection3 ).close();
    }

    @Test
    @SuppressWarnings( "unchecked" )
    public void shouldLogWhenConnectionDisposeFails()
    {
        Logging logging = mock( Logging.class );
        Logger logger = mock( Logger.class );
        when( logging.getLog( anyString() ) ).thenReturn( logger );

        BlockingPooledConnectionQueue queue = newConnectionQueue( 5, logging );

        Connection connection = mock( Connection.class );
        RuntimeException closeError = new RuntimeException( "Fail" );
        doThrow( closeError ).when( connection ).close();
        PooledConnection pooledConnection = new PooledSocketConnection( connection, mock( Consumer.class ), SYSTEM );
        queue.offer( pooledConnection );

        queue.terminate();

        verify( logger ).warn( anyString(), eq( closeError ) );
    }

    @Test
    public void shouldHaveZeroSizeAfterTermination()
    {
        BlockingPooledConnectionQueue queue = newConnectionQueue( 5 );

        queue.offer( mock( PooledConnection.class ) );
        queue.offer( mock( PooledConnection.class ) );
        queue.offer( mock( PooledConnection.class ) );

        queue.terminate();

        assertEquals( 0, queue.size() );
    }

    @Test
    @SuppressWarnings( "unchecked" )
    public void shouldTerminateBothAcquiredAndIdleConnections()
    {
        BlockingPooledConnectionQueue queue = newConnectionQueue( 5 );

        PooledConnection connection1 = mock( PooledConnection.class );
        PooledConnection connection2 = mock( PooledConnection.class );
        PooledConnection connection3 = mock( PooledConnection.class );
        PooledConnection connection4 = mock( PooledConnection.class );

        queue.offer( connection1 );
        queue.offer( connection2 );
        queue.offer( connection3 );
        queue.offer( connection4 );

        PooledConnection acquiredConnection1 = queue.acquire( mock( Supplier.class ) );
        PooledConnection acquiredConnection2 = queue.acquire( mock( Supplier.class ) );
        assertSame( connection1, acquiredConnection1 );
        assertSame( connection2, acquiredConnection2 );

        queue.terminate();

        verify( connection1 ).dispose();
        verify( connection2 ).dispose();
        verify( connection3 ).dispose();
        verify( connection4 ).dispose();
    }

    @Test
    public void shouldReportZeroActiveConnectionsWhenEmpty()
    {
        BlockingPooledConnectionQueue queue = newConnectionQueue( 5 );

        assertEquals( 0, queue.activeConnections() );
    }

    @Test
    public void shouldReportZeroActiveConnectionsWhenHasOnlyIdleConnections()
    {
        BlockingPooledConnectionQueue queue = newConnectionQueue( 5 );

        queue.offer( mock( PooledConnection.class ) );
        queue.offer( mock( PooledConnection.class ) );

        assertEquals( 0, queue.activeConnections() );
    }

    @Test
    @SuppressWarnings( "unchecked" )
    public void shouldReportActiveConnections()
    {
        BlockingPooledConnectionQueue queue = newConnectionQueue( 5 );

        PooledConnection connection1 = mock( PooledConnection.class );
        PooledConnection connection2 = mock( PooledConnection.class );
        PooledConnection connection3 = mock( PooledConnection.class );

        queue.offer( connection1 );
        queue.offer( connection2 );
        queue.offer( connection3 );

        queue.acquire( mock( Supplier.class ) );
        queue.acquire( mock( Supplier.class ) );
        queue.acquire( mock( Supplier.class ) );

        assertEquals( 3, queue.activeConnections() );

        queue.offer( connection1 );
        queue.offer( connection2 );
        queue.offer( connection3 );

        assertEquals( 0, queue.activeConnections() );
    }

    private static BlockingPooledConnectionQueue newConnectionQueue( int capacity )
    {
        return newConnectionQueue( capacity, mock( Logging.class, RETURNS_MOCKS ) );
    }

    private static BlockingPooledConnectionQueue newConnectionQueue( int capacity, Logging logging )
    {
        return new BlockingPooledConnectionQueue( LOCAL_DEFAULT, capacity, logging );
    }
}
