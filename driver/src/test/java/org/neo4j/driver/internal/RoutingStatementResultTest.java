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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;


@RunWith(Parameterized.class)
public class RoutingStatementResultTest
{
    private static final BoltServerAddress LOCALHOST = new BoltServerAddress( "localhost", 7687 );
    private StatementResult delegate = mock( StatementResult.class );
    private RoutingErrorHandler onError = mock( RoutingErrorHandler.class );
    private final AccessMode accessMode;

    @Parameterized.Parameters(name = "accessMode-{0}")
    public static Collection<AccessMode> data()
    {
        return Arrays.asList( AccessMode.values() );
    }

    public RoutingStatementResultTest( AccessMode accessMode )
    {
        this.accessMode = accessMode;
    }

    @Test
    public void shouldHandleConnectionFailureOnConsume()
    {
        // Given
        when( delegate.consume() ).thenThrow( new ServiceUnavailableException( "oh no" ) );
        RoutingStatementResult result =
                new RoutingStatementResult( delegate, accessMode, LOCALHOST, onError );

        // When
        try
        {
            result.consume();
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
    public void shouldHandleConnectionFailureOnHasNext()
    {
        // Given
        when( delegate.hasNext() ).thenThrow( new ServiceUnavailableException( "oh no" ) );
        RoutingStatementResult result =
                new RoutingStatementResult( delegate, accessMode, LOCALHOST, onError );

        // When
        try
        {
            result.hasNext();
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
    public void shouldHandleConnectionFailureOnKeys()
    {
        // Given
        when( delegate.keys() ).thenThrow( new ServiceUnavailableException( "oh no" ) );
        RoutingStatementResult result =
                new RoutingStatementResult( delegate, accessMode, LOCALHOST, onError );

        // When
        try
        {
            result.keys();
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
    public void shouldHandleConnectionFailureOnList()
    {
        // Given
        when( delegate.list() ).thenThrow( new ServiceUnavailableException( "oh no" ) );
        RoutingStatementResult result =
                new RoutingStatementResult( delegate, accessMode, LOCALHOST, onError );

        // When
        try
        {
            result.list();
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
    public void shouldHandleConnectionFailureOnNext()
    {
        // Given
        when( delegate.next() ).thenThrow( new ServiceUnavailableException( "oh no" ) );
        RoutingStatementResult result =
                new RoutingStatementResult( delegate, accessMode, LOCALHOST, onError );

        // When
        try
        {
            result.next();
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
    public void shouldHandleConnectionFailureOnPeek()
    {
        // Given
        when( delegate.peek() ).thenThrow( new ServiceUnavailableException( "oh no" ) );
        RoutingStatementResult result =
                new RoutingStatementResult( delegate, accessMode, LOCALHOST, onError );

        // When
        try
        {
            result.peek();
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
    public void shouldHandleConnectionFailureOnSingle()
    {
        // Given
        when( delegate.single() ).thenThrow( new ServiceUnavailableException( "oh no" ) );
        RoutingStatementResult result =
                new RoutingStatementResult( delegate, accessMode, LOCALHOST, onError );

        // When
        try
        {
            result.single();
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
