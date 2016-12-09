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

import org.neo4j.driver.internal.exceptions.InvalidOperationException;
import org.neo4j.driver.internal.spi.PooledConnection;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Logging;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.net.BoltServerAddress.LOCAL_DEFAULT;

public class BlockingPooledConnectionQueueTest
{
    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldCreateNewConnectionWhenEmpty() throws Throwable
    {
        // Given
        PooledConnection connection = mock( PooledSocketConnection.class );
        PooledConnectionFactory supplier = mock( PooledConnectionFactory.class );
        when( supplier.newInstance() ).thenReturn( connection );
        BlockingPooledConnectionQueue queue = newConnectionQueue( 10 );

        // When
        queue.acquire( supplier );

        // Then
        verify( supplier ).newInstance();
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldNotCreateNewConnectionWhenNotEmpty() throws Throwable
    {
        // Given
        PooledConnection connection = mock( PooledSocketConnection.class );
        PooledConnectionFactory supplier = mock( PooledConnectionFactory.class );
        when( supplier.newInstance() ).thenReturn( connection );
        BlockingPooledConnectionQueue queue = newConnectionQueue( 1 );

        queue.offer( connection );

        // When
        queue.acquire( supplier );

        // Then
        verify( supplier, never() ).newInstance();
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldTerminateAllSeenConnections() throws Throwable
    {
        // Given

        PooledSocketConnection connection1 = mock( PooledSocketConnection.class );
        PooledSocketConnection connection2 = mock( PooledSocketConnection.class );
        PooledConnectionFactory supplier = mock( PooledConnectionFactory.class );
        when( supplier.newInstance() ).thenReturn( connection1 );
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
    public void shouldNotAcceptWhenFull() throws Throwable
    {
        // Given

        PooledSocketConnection connection1 = mock( PooledSocketConnection.class );
        PooledSocketConnection connection2 = mock( PooledSocketConnection.class );
        BlockingPooledConnectionQueue queue = newConnectionQueue( 1 );

        // Then
        assertTrue( queue.offer( connection1 ) );
        assertFalse( queue.offer( connection2 ) );
    }

    @Test
    public void shouldDisposeAllConnectionsWhenOneOfThemFailsToDispose() throws Throwable
    {
        BlockingPooledConnectionQueue queue = newConnectionQueue( 5 );

        PooledConnection connection1 = mock( PooledConnection.class );
        PooledConnection connection2 = mock( PooledConnection.class );
        PooledConnection connection3 = mock( PooledConnection.class );

        InvalidOperationException disposeError = new InvalidOperationException( "Failed to stop socket" );
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
    public void shouldTryToCloseAllUnderlyingConnections() throws Throwable
    {
        BlockingPooledConnectionQueue queue = newConnectionQueue( 5 );

        PooledConnection connection1 = mock( PooledConnection.class );
        PooledConnection connection2 = mock( PooledConnection.class );
        PooledConnection connection3 = mock( PooledConnection.class );

        InvalidOperationException closeError1 = new InvalidOperationException( "Failed to close 1" );
        InvalidOperationException closeError2 = new InvalidOperationException( "Failed to close 2" );
        InvalidOperationException closeError3 = new InvalidOperationException( "Failed to close 3" );

        doThrow( closeError1 ).when( connection1 ).close();
        doThrow( closeError2 ).when( connection2 ).close();
        doThrow( closeError3 ).when( connection3 ).close();

        PooledConnection pooledConnection1 = new PooledSocketConnection( connection1, mock( PooledConnectionReleaseManager.class ) );
        PooledConnection pooledConnection2 = new PooledSocketConnection( connection2, mock( PooledConnectionReleaseManager.class ) );
        PooledConnection pooledConnection3 = new PooledSocketConnection( connection3, mock( PooledConnectionReleaseManager.class ) );

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
    public void shouldLogWhenConnectionDisposeFails() throws Throwable
    {
        Logging logging = mock( Logging.class );
        Logger logger = mock( Logger.class );
        when( logging.getLog( anyString() ) ).thenReturn( logger );

        BlockingPooledConnectionQueue queue = newConnectionQueue( 5, logging );

        PooledConnection connection = mock( PooledConnection.class );
        InvalidOperationException closeError = new InvalidOperationException( "Fail" );
        doThrow( closeError ).when( connection ).close();
        PooledConnection pooledConnection = new PooledSocketConnection( connection, mock(
                PooledConnectionReleaseManager.class ) );
        queue.offer( pooledConnection );

        queue.terminate();

        verify( logger ).error( anyString(), eq( closeError ) );
    }

    @Test
    public void shouldHaveZeroSizeAfterTermination() throws Throwable
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
    public void shouldTerminateBothAcquiredAndIdleConnections() throws Throwable
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

        PooledConnection acquiredConnection1 = queue.acquire( mock( PooledConnectionFactory.class ) );
        PooledConnection acquiredConnection2 = queue.acquire( mock( PooledConnectionFactory.class ) );
        assertSame( connection1, acquiredConnection1 );
        assertSame( connection2, acquiredConnection2 );

        queue.terminate();

        verify( connection1 ).dispose();
        verify( connection2 ).dispose();
        verify( connection3 ).dispose();
        verify( connection4 ).dispose();
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
