/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.driver.internal.handlers;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.CompletionException;

import org.neo4j.driver.internal.RoutingErrorHandler;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.driver.exceptions.TransientException;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.neo4j.driver.internal.BoltServerAddress.LOCAL_DEFAULT;

class RoutingResponseHandlerTest
{
    @Test
    void shouldUnwrapCompletionException()
    {
        RuntimeException error = new RuntimeException( "Hi" );
        RoutingErrorHandler errorHandler = mock( RoutingErrorHandler.class );

        Throwable handledError = handle( new CompletionException( error ), errorHandler );

        assertEquals( error, handledError );
        verifyZeroInteractions( errorHandler );
    }

    @Test
    void shouldHandleServiceUnavailableException()
    {
        ServiceUnavailableException error = new ServiceUnavailableException( "Hi" );
        RoutingErrorHandler errorHandler = mock( RoutingErrorHandler.class );

        Throwable handledError = handle( error, errorHandler );

        assertThat( handledError, instanceOf( SessionExpiredException.class ) );
        verify( errorHandler ).onConnectionFailure( LOCAL_DEFAULT );
    }

    @Test
    void shouldHandleDatabaseUnavailableError()
    {
        TransientException error = new TransientException( "Neo.TransientError.General.DatabaseUnavailable", "Hi" );
        RoutingErrorHandler errorHandler = mock( RoutingErrorHandler.class );

        Throwable handledError = handle( error, errorHandler );

        assertEquals( error, handledError );
        verify( errorHandler ).onConnectionFailure( LOCAL_DEFAULT );
    }

    @Test
    void shouldHandleTransientException()
    {
        TransientException error = new TransientException( "Neo.TransientError.Transaction.DeadlockDetected", "Hi" );
        RoutingErrorHandler errorHandler = mock( RoutingErrorHandler.class );

        Throwable handledError = handle( error, errorHandler );

        assertEquals( error, handledError );
        verifyZeroInteractions( errorHandler );
    }

    @Test
    void shouldHandleNotALeaderErrorWithReadAccessMode()
    {
        testWriteFailureWithReadAccessMode( "Neo.ClientError.Cluster.NotALeader" );
    }

    @Test
    void shouldHandleNotALeaderErrorWithWriteAccessMode()
    {
        testWriteFailureWithWriteAccessMode( "Neo.ClientError.Cluster.NotALeader" );
    }

    @Test
    void shouldHandleForbiddenOnReadOnlyDatabaseErrorWithReadAccessMode()
    {
        testWriteFailureWithReadAccessMode( "Neo.ClientError.General.ForbiddenOnReadOnlyDatabase" );
    }

    @Test
    void shouldHandleForbiddenOnReadOnlyDatabaseErrorWithWriteAccessMode()
    {
        testWriteFailureWithWriteAccessMode( "Neo.ClientError.General.ForbiddenOnReadOnlyDatabase" );
    }

    @Test
    void shouldHandleClientException()
    {
        ClientException error = new ClientException( "Neo.ClientError.Request.Invalid", "Hi" );
        RoutingErrorHandler errorHandler = mock( RoutingErrorHandler.class );

        Throwable handledError = handle( error, errorHandler, AccessMode.READ );

        assertEquals( error, handledError );
        verifyZeroInteractions( errorHandler );
    }

    @Test
    public void shouldDelegateCanManageAutoRead()
    {
        ResponseHandler responseHandler = mock( ResponseHandler.class );
        RoutingResponseHandler routingResponseHandler =
                new RoutingResponseHandler( responseHandler, LOCAL_DEFAULT, AccessMode.READ, null );

        routingResponseHandler.canManageAutoRead();

        verify( responseHandler ).canManageAutoRead();
    }

    @Test
    public void shouldDelegateDisableAutoReadManagement()
    {
        ResponseHandler responseHandler = mock( ResponseHandler.class );
        RoutingResponseHandler routingResponseHandler =
                new RoutingResponseHandler( responseHandler, LOCAL_DEFAULT, AccessMode.READ, null );

        routingResponseHandler.disableAutoReadManagement();

        verify( responseHandler ).disableAutoReadManagement();
    }

    private void testWriteFailureWithReadAccessMode( String code )
    {
        ClientException error = new ClientException( code, "Hi" );
        RoutingErrorHandler errorHandler = mock( RoutingErrorHandler.class );

        Throwable handledError = handle( error, errorHandler, AccessMode.READ );

        assertThat( handledError, instanceOf( ClientException.class ) );
        assertEquals( "Write queries cannot be performed in READ access mode.", handledError.getMessage() );
        verifyZeroInteractions( errorHandler );
    }

    private void testWriteFailureWithWriteAccessMode( String code )
    {
        ClientException error = new ClientException( code, "Hi" );
        RoutingErrorHandler errorHandler = mock( RoutingErrorHandler.class );

        Throwable handledError = handle( error, errorHandler, AccessMode.WRITE );

        assertThat( handledError, instanceOf( SessionExpiredException.class ) );
        assertEquals( "Server at " + LOCAL_DEFAULT + " no longer accepts writes", handledError.getMessage() );
        verify( errorHandler ).onWriteFailure( LOCAL_DEFAULT );
    }

    private static Throwable handle( Throwable error, RoutingErrorHandler errorHandler )
    {
        return handle( error, errorHandler, AccessMode.READ );
    }

    private static Throwable handle( Throwable error, RoutingErrorHandler errorHandler, AccessMode accessMode )
    {
        ResponseHandler responseHandler = mock( ResponseHandler.class );
        RoutingResponseHandler routingResponseHandler =
                new RoutingResponseHandler( responseHandler, LOCAL_DEFAULT, accessMode, errorHandler );

        routingResponseHandler.onFailure( error );

        ArgumentCaptor<Throwable> handledErrorCaptor = ArgumentCaptor.forClass( Throwable.class );
        verify( responseHandler ).onFailure( handledErrorCaptor.capture() );
        return handledErrorCaptor.getValue();
    }
}
