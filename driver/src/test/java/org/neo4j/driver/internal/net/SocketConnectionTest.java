/*
 * Copyright (c) 2002-2018 Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.driver.internal.net;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Queue;

import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.SuccessMessage;
import org.neo4j.driver.internal.summary.InternalServerInfo;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.summary.ServerInfo;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.logging.DevNullLogger.DEV_NULL_LOGGER;
import static org.neo4j.driver.internal.net.BoltServerAddress.LOCAL_DEFAULT;
import static org.neo4j.driver.v1.Values.parameters;

public class SocketConnectionTest
{
    private static final InternalServerInfo SERVER_INFO = new InternalServerInfo( LOCAL_DEFAULT, "test" );

    @Test
    public void shouldReceiveServerInfoAfterInit() throws Throwable
    {
        // Given
        SocketClient socket = mock( SocketClient.class );
        SocketConnection conn = new SocketConnection( socket, SERVER_INFO, DEV_NULL_LOGGER );

        when( socket.address() ).thenReturn( new BoltServerAddress( "neo4j.com:9000" ) );

        // set up response messages
        ArrayList<Message> serverResponses = new ArrayList<>();
        serverResponses.add(
                new SuccessMessage( Values.parameters( "server", "super-awesome" ).asMap( Values.ofValue() )
        ) );
        final Iterator<Message> iterator = serverResponses.iterator();
        doAnswer( new Answer<Object>()
        {
            @Override
            public Object answer( InvocationOnMock invocation ) throws Throwable
            {
                Object[] arguments = invocation.getArguments();
                SocketResponseHandler responseHandler = ( SocketResponseHandler ) arguments[0];
                iterator.next().dispatch( responseHandler );
                return null; // does not matter what to return
            }
        } ).when( socket ).receiveOne( any( SocketResponseHandler.class ) );
        doCallRealMethod().when( socket ).receiveAll( any(SocketResponseHandler.class) );

        // When
        conn.init( "java-driver-1.1", parameters( "scheme", "none" ).asMap( Values.ofValue() ) );

        // Then
        ServerInfo server = conn.server();
        assertThat( server.address(), equalTo( "neo4j.com:9000" ) );
        assertThat( server.version(), equalTo( "super-awesome" ) );
    }

    @Test
    public void shouldCloseConnectionIfFailedToCreate() throws Throwable
    {
        // Given
        SocketClient socket = mock( SocketClient.class );

        // When
        doThrow( new RuntimeException( "failed to start socket client" ) ).when( socket ).start();

        // Then
        try
        {
            new SocketConnection( socket, SERVER_INFO, DEV_NULL_LOGGER );
            fail( "should have failed with the provided exception" );
        }
        catch( Throwable e )
        {
            assertThat( e, instanceOf( Exception.class ) );
            assertThat( e.getMessage(), equalTo( "failed to start socket client" ) );
        }
        verify( socket, times( 1 ) ).stop();
    }

    @Test
    @SuppressWarnings( "unchecked" )
    public void flushThrowsWhenSocketIsBroken() throws Exception
    {
        SocketClient socket = mock( SocketClient.class );
        IOException sendError = new IOException( "Unable to send" );
        doThrow( sendError ).when( socket ).send( any( Queue.class ) );

        SocketConnection connection = new SocketConnection( socket, SERVER_INFO, DEV_NULL_LOGGER );

        try
        {
            connection.flush();
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ServiceUnavailableException.class ) );
            assertSame( sendError, e.getCause() );
        }
    }
}
