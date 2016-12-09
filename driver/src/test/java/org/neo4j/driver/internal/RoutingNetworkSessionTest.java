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
package org.neo4j.driver.internal;

import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import org.neo4j.driver.internal.exceptions.BoltProtocolException;
import org.neo4j.driver.internal.exceptions.InvalidOperationException;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;

public class RoutingNetworkSessionTest
{
    private RoutingErrorHandler mockedOnError;
    private NetworkSession mockedSession;
    private static final BoltServerAddress LOCALHOST = new BoltServerAddress( "localhost", 7687 );

    @Before
    public void setUp()
    {
        mockedSession = mock( NetworkSession.class );
        mockedOnError = mock( RoutingErrorHandler.class );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldHandleConnectionFailures()
    {
        // Given
        doThrow( new ServiceUnavailableException( "oh no" ) ).
                when( mockedSession ).run( anyString(), any( Map.class ) );

        RoutingNetworkSession result = new RoutingNetworkSession( mockedSession, AccessMode.WRITE, LOCALHOST,
                mockedOnError );

        // When
        try
        {
            result.run( "CREATE ()" );
            fail();
        }
        catch ( SessionExpiredException e )
        {
            //ignore
        }

        // Then
        verify( mockedOnError ).onConnectionFailure( LOCALHOST );
        verifyNoMoreInteractions( mockedOnError );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldHandleWriteFailuresInWriteAccessMode() throws InvalidOperationException, BoltProtocolException
    {
        // Given
        doThrow( new ClientException( "Neo.ClientError.Cluster.NotALeader", "oh no!" ) ).
                when( mockedSession ).run( anyString(), any( Map.class ) );
        RoutingNetworkSession session = new RoutingNetworkSession( mockedSession, AccessMode.WRITE, LOCALHOST,
                mockedOnError );

        // When
        try
        {
            session.run( "CREATE ()" );
            fail();
        }
        catch ( SessionExpiredException e )
        {
            //ignore
        }

        // Then
        verify( mockedOnError ).onWriteFailure( LOCALHOST );
        verifyNoMoreInteractions( mockedOnError );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldHandleWriteFailuresInReadAccessMode()
    {
        // Given
        doThrow( new ClientException( "Neo.ClientError.Cluster.NotALeader", "oh no!" ) ).
                when( mockedSession ).run( anyString(), any( Map.class ) );
        RoutingNetworkSession session = new RoutingNetworkSession( mockedSession, AccessMode.READ, LOCALHOST,
                mockedOnError );

        // When
        try
        {
            session.run( "CREATE ()" );
            fail();
        }
        catch ( ClientException e )
        {
            //ignore
        }
        verifyNoMoreInteractions( mockedOnError );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldRethrowNonWriteFailures()
    {
        // Given
        ClientException toBeThrown = new ClientException( "code", "oh no!" );
        doThrow( toBeThrown ).
                when( mockedSession ).run( anyString(), any( Map.class ) );
        RoutingNetworkSession session = new RoutingNetworkSession( mockedSession, AccessMode.WRITE, LOCALHOST,
                mockedOnError );

        // When
        try
        {
            session.run( "CREATE ()" );
            fail();
        }
        catch ( ClientException e )
        {
            assertThat( e, is( toBeThrown ) );
        }

        // Then
        verifyZeroInteractions( mockedOnError );
    }

    @Test
    public void shouldHandleConnectionFailuresOnClose()
    {
        // Given
        doThrow( new ServiceUnavailableException( "oh no" ) ).
                when( mockedSession ).close();

        RoutingNetworkSession session = new RoutingNetworkSession( mockedSession,  AccessMode.WRITE, LOCALHOST,
                mockedOnError );

        // When
        try
        {
            session.close();
            fail();
        }
        catch ( SessionExpiredException e )
        {
            //ignore
        }

        // Then
        verify( mockedOnError ).onConnectionFailure( LOCALHOST );
        verifyNoMoreInteractions( mockedOnError );
    }

    @Test
    public void shouldHandleWriteFailuresOnClose()
    {
        // Given
        doThrow( new ClientException( "Neo.ClientError.Cluster.NotALeader", "oh no!" ) ).when( mockedSession ).close();
        RoutingNetworkSession session = new RoutingNetworkSession( mockedSession, AccessMode.WRITE, LOCALHOST,
                mockedOnError );

        // When
        try
        {
            session.close();
            fail();
        }
        catch ( SessionExpiredException e )
        {
            //ignore
        }

        // Then
        verify( mockedOnError ).onWriteFailure( LOCALHOST );
        verifyNoMoreInteractions( mockedOnError );
    }

    @Test
    public void shouldDelegateLastBookmark()
    {
        // Given
        RoutingNetworkSession session = new RoutingNetworkSession( mockedSession, AccessMode.WRITE, LOCALHOST,
                mockedOnError );

        // When
        session.lastBookmark();

        // Then
        verify( mockedSession ).lastBookmark();
    }

    @Test
    public void shouldDelegateReset()
    {
        // Given
        RoutingNetworkSession session = new RoutingNetworkSession( mockedSession, AccessMode.WRITE, LOCALHOST,
                mockedOnError );

        // When
        session.reset();

        // Then
        verify( mockedSession ).reset();
    }

    @Test
    public void shouldDelegateIsOpen()
    {
        // Given
        RoutingNetworkSession session = new RoutingNetworkSession( mockedSession, AccessMode.WRITE, LOCALHOST,
                mockedOnError );

        // When
        session.isOpen();

        // Then
        verify( mockedSession ).isOpen();
    }
}
