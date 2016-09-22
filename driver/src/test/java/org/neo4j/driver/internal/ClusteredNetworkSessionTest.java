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
package org.neo4j.driver.internal;

import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.spi.Collector;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.ConnectionFailureException;
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
import static org.mockito.Mockito.when;

public class ClusteredNetworkSessionTest
{
    private Connection connection;
    private ClusteredErrorHandler onError;
    private static final BoltServerAddress LOCALHOST = new BoltServerAddress( "localhost", 7687 );

    @Before
    public void setUp()
    {
        connection = mock( Connection.class );
        when( connection.address() ).thenReturn( LOCALHOST );
        when( connection.isOpen() ).thenReturn( true );
        onError = mock( ClusteredErrorHandler.class );
    }

    @Test
    public void shouldHandleConnectionFailures()
    {
        // Given
        doThrow( new ConnectionFailureException( "oh no" ) ).
                when( connection ).run( anyString(), any( Map.class ), any( Collector.class ) );

        ClusteredNetworkSession result =
                new ClusteredNetworkSession( connection, onError, mock( Logger.class ) );

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
        verify( onError ).onConnectionFailure( LOCALHOST );
        verifyNoMoreInteractions( onError );
    }

    @Test
    public void shouldHandleWriteFailures()
    {
        // Given
        doThrow( new ClientException( "Neo.ClientError.Cluster.NotALeader", "oh no!" ) ).
                when( connection ).run( anyString(), any( Map.class ), any( Collector.class ) );
        ClusteredNetworkSession session =
                new ClusteredNetworkSession( connection, onError, mock( Logger.class ) );

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
        verify( onError ).onWriteFailure( LOCALHOST );
        verifyNoMoreInteractions( onError );
    }

    @Test
    public void shouldRethrowNonWriteFailures()
    {
        // Given
        ClientException toBeThrown = new ClientException( "code", "oh no!" );
        doThrow( toBeThrown ).
                when( connection ).run( anyString(), any( Map.class ), any( Collector.class ) );
        ClusteredNetworkSession session =
                new ClusteredNetworkSession( connection, onError, mock( Logger.class ) );

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
        verifyZeroInteractions(  onError );
    }

    @Test
    public void shouldHandleConnectionFailuresOnClose()
    {
        // Given
        doThrow( new ConnectionFailureException( "oh no" ) ).
                when( connection ).sync();

        ClusteredNetworkSession session =
                new ClusteredNetworkSession( connection, onError, mock( Logger.class ) );

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
        verify( onError ).onConnectionFailure( LOCALHOST );
        verifyNoMoreInteractions( onError );
    }
}