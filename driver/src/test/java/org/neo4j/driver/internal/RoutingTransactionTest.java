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
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Map;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.spi.Collector;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class RoutingTransactionTest
{
    private static final BoltServerAddress LOCALHOST = new BoltServerAddress( "localhost", 7687 );
    private Connection connection;
    private RoutingErrorHandler onError;
    private Runnable cleanup;

    private Answer<Void> throwingAnswer( final Throwable throwable )
    {
        return new Answer<Void>()
        {
            @Override
            public Void answer( InvocationOnMock invocationOnMock ) throws Throwable
            {
                String statement = (String) invocationOnMock.getArguments()[0];
                if ( statement.equals( "BEGIN" ) )
                {
                    return null;
                }
                else
                {
                    throw throwable;
                }
            }
        };
    }

    @Before
    public void setUp()
    {
        connection = mock( Connection.class );
        when( connection.boltServerAddress() ).thenReturn( LOCALHOST );
        when( connection.isOpen() ).thenReturn( true );
        onError = mock( RoutingErrorHandler.class );
        cleanup = mock( Runnable.class );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldHandleConnectionFailures()
    {
        // Given

        doAnswer( throwingAnswer( new ServiceUnavailableException( "oh no" ) ) )
                .when( connection ).run( anyString(), any( Map.class ), any( Collector.class ) );

        RoutingTransaction tx =
                new RoutingTransaction( new ExplicitTransaction( connection, cleanup ), AccessMode.READ, LOCALHOST,
                        onError );

        // When
        try
        {
            tx.run( "CREATE ()" );
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

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldHandleWriteFailuresInWriteAccessMode()
    {
        // Given
        doAnswer( throwingAnswer( new ClientException( "Neo.ClientError.Cluster.NotALeader", "oh no!" ) ) )
                .when( connection ).run( anyString(), any( Map.class ), any( Collector.class ) );

        RoutingTransaction tx =
                new RoutingTransaction( new ExplicitTransaction( connection, cleanup ), AccessMode.WRITE,
                        connection.boltServerAddress(), onError );

        // When
        try
        {
            tx.run( "CREATE ()" );
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

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldHandleWriteFailuresInReadAccessMode()
    {
        // Given
        doAnswer( throwingAnswer( new ClientException( "Neo.ClientError.Cluster.NotALeader", "oh no!" ) ) )
                .when( connection ).run( anyString(), any( Map.class ), any( Collector.class ) );
        RoutingTransaction tx =
                new RoutingTransaction( new ExplicitTransaction( connection, cleanup ), AccessMode.READ,
                        connection.boltServerAddress(), onError );

        // When
        try
        {
            tx.run( "CREATE ()" );
            fail();
        }
        catch ( ClientException e )
        {
            //ignore
        }
        verifyNoMoreInteractions( onError );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldRethrowNonWriteFailures()
    {
        // Given
        ClientException toBeThrown = new ClientException( "code", "oh no!" );
        doAnswer( throwingAnswer( toBeThrown ) )
                .when( connection ).run( anyString(), any( Map.class ), any( Collector.class ) );
        RoutingTransaction tx =
                new RoutingTransaction( new ExplicitTransaction( connection, cleanup ), AccessMode.WRITE,
                        connection.boltServerAddress(), onError );

        // When
        try
        {
            tx.run( "CREATE ()" );
            fail();
        }
        catch ( ClientException e )
        {
            assertThat( e, is( toBeThrown ) );
        }

        // Then
        verifyZeroInteractions( onError );
    }

    @Test
    public void shouldHandleConnectionFailuresOnClose()
    {
        // Given
        doThrow( new ServiceUnavailableException( "oh no" ) ).
                when( connection ).sync();

        RoutingTransaction tx =
                new RoutingTransaction( new ExplicitTransaction( connection, cleanup ), AccessMode.WRITE,
                        connection.boltServerAddress(), onError );

        // When
        try
        {
            tx.close();
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
    public void shouldHandleWriteFailuresOnClose()
    {
        // Given
        doThrow( new ClientException( "Neo.ClientError.Cluster.NotALeader", "oh no!" ) ).when( connection ).sync();

        RoutingTransaction tx =
                new RoutingTransaction( new ExplicitTransaction( connection, cleanup ), AccessMode.WRITE,
                        connection.boltServerAddress(), onError );

        // When
        try
        {
            tx.close();
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
    public void shouldDelegateSuccess()
    {
        // Given
        Transaction inner = mock( Transaction.class );
        RoutingTransaction tx =
                new RoutingTransaction(inner, AccessMode.WRITE,
                        connection.boltServerAddress(), onError );

        // When
        tx.success();

        // Then
        verify( inner ).success();
    }

    @Test
    public void shouldDelegateFailure()
    {
        // Given
        Transaction inner = mock( Transaction.class );
        RoutingTransaction tx =
                new RoutingTransaction(inner, AccessMode.WRITE,
                        connection.boltServerAddress(), onError );

        // When
        tx.failure();

        // Then
        verify( inner ).failure();
    }

    @Test
    public void shouldDelegateIsOpen()
    {
        // Given
        Transaction inner = mock( Transaction.class );
        RoutingTransaction tx =
                new RoutingTransaction(inner, AccessMode.WRITE,
                        connection.boltServerAddress(), onError );

        // When
        tx.isOpen();

        // Then
        verify( inner ).isOpen();
    }

    @Test
    public void shouldDelegateTypesystem()
    {
        // Given
        Transaction inner = mock( Transaction.class );
        RoutingTransaction tx =
                new RoutingTransaction(inner, AccessMode.WRITE,
                        connection.boltServerAddress(), onError );

        // When
        tx.typeSystem();

        // Then
        verify( inner ).typeSystem();
    }
}
