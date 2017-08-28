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
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import java.io.IOException;
import java.util.Map;
import java.util.Queue;

import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.net.SocketClient;
import org.neo4j.driver.internal.net.SocketConnection;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.spi.PooledConnection;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.internal.summary.InternalServerInfo;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.Consumers;
import org.neo4j.driver.v1.exceptions.DatabaseException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.logging.DevNullLogger.DEV_NULL_LOGGER;
import static org.neo4j.driver.internal.messaging.ResetMessage.RESET;
import static org.neo4j.driver.internal.net.BoltServerAddress.LOCAL_DEFAULT;

public class PooledConnectionValidatorTest
{
    @Test
    public void isNotReusableWhenPoolHasNoAddress()
    {
        Connection connection = mock( Connection.class );
        PooledConnection pooledConnection = newPooledConnection( connection );

        PooledConnectionValidator validator = new PooledConnectionValidator( connectionPoolMock( false ) );

        assertFalse( validator.isReusable( pooledConnection ) );
        verify( connection, never() ).reset();
        verify( connection, never() ).sync();
    }

    @Test
    @SuppressWarnings( "unchecked" )
    public void isNotReusableWhenHasUnrecoverableErrors()
    {
        Connection connection = mock( Connection.class );
        DatabaseException runError = new DatabaseException( "", "" );
        doThrow( runError ).when( connection ).run( anyString(), any( Map.class ), any( ResponseHandler.class ) );

        PooledConnection pooledConnection = newPooledConnection( connection );

        try
        {
            pooledConnection.run( "BEGIN", null, null );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertSame( runError, e );
        }
        assertTrue( pooledConnection.hasUnrecoverableErrors() );

        PooledConnectionValidator validator = new PooledConnectionValidator( connectionPoolMock( true ) );

        assertFalse( validator.isReusable( pooledConnection ) );
        verify( connection, never() ).reset();
        verify( connection, never() ).sync();
    }

    @Test
    public void resetAndSyncValidConnectionWhenCheckingIfReusable()
    {
        Connection connection = mock( Connection.class );
        PooledConnection pooledConnection = newPooledConnection( connection );

        PooledConnectionValidator validator = new PooledConnectionValidator( connectionPoolMock( true ) );
        boolean connectionIsValid = validator.isReusable( pooledConnection );

        assertTrue( connectionIsValid );

        InOrder inOrder = inOrder( connection );
        inOrder.verify( connection ).reset();
        inOrder.verify( connection ).sync();
    }

    @Test
    public void sendsSingleResetMessageForValidConnectionWhenCheckingIfReusable() throws IOException
    {
        SocketClient socket = mock( SocketClient.class );
        InternalServerInfo serverInfo = new InternalServerInfo( LOCAL_DEFAULT, "v1" );
        Connection connection = new SocketConnection( socket, serverInfo, DEV_NULL_LOGGER );
        PooledConnection pooledConnection = newPooledConnection( connection );

        PooledConnectionValidator validator = new PooledConnectionValidator( connectionPoolMock( true ) );
        boolean connectionIsValid = validator.isReusable( pooledConnection );

        assertTrue( connectionIsValid );

        ArgumentCaptor<Queue<Message>> captor = messagesCaptor();
        verify( socket ).send( captor.capture() );
        assertEquals( 1, captor.getAllValues().size() );
        Queue<Message> messages = captor.getValue();
        assertEquals( 1, messages.size() );
        assertEquals( RESET, messages.peek() );
    }

    @Test
    public void isConnectedReturnsFalseWhenResetFails()
    {
        Connection connection = mock( Connection.class );
        doThrow( new RuntimeException() ).when( connection ).reset();
        PooledConnection pooledConnection = newPooledConnection( connection );

        PooledConnectionValidator validator = new PooledConnectionValidator( connectionPoolMock( true ) );

        assertFalse( validator.isConnected( pooledConnection ) );
        verify( connection ).reset();
        verify( connection, never() ).sync();
    }

    @Test
    public void isConnectedReturnsFalseWhenSyncFails()
    {
        Connection connection = mock( Connection.class );
        doThrow( new RuntimeException() ).when( connection ).sync();
        PooledConnection pooledConnection = newPooledConnection( connection );

        PooledConnectionValidator validator = new PooledConnectionValidator( connectionPoolMock( true ) );

        assertFalse( validator.isConnected( pooledConnection ) );
        verify( connection ).reset();
        verify( connection ).sync();
    }

    @Test
    public void isConnectedReturnsTrueWhenUnderlyingConnectionWorks()
    {
        Connection connection = mock( Connection.class );
        PooledConnection pooledConnection = newPooledConnection( connection );

        PooledConnectionValidator validator = new PooledConnectionValidator( connectionPoolMock( true ) );

        assertTrue( validator.isConnected( pooledConnection ) );
        verify( connection ).reset();
        verify( connection ).sync();
    }

    private static PooledConnection newPooledConnection( Connection connection )
    {
        return new PooledSocketConnection( connection, Consumers.<PooledConnection>noOp(), Clock.SYSTEM );
    }

    private static ConnectionPool connectionPoolMock( boolean knowsAddressed )
    {
        ConnectionPool pool = mock( ConnectionPool.class );
        when( pool.hasAddress( any( BoltServerAddress.class ) ) ).thenReturn( knowsAddressed );
        return pool;
    }

    @SuppressWarnings( "unchecked" )
    private static ArgumentCaptor<Queue<Message>> messagesCaptor()
    {
        return (ArgumentCaptor) ArgumentCaptor.forClass( Queue.class );
    }
}
