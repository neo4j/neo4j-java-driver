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
package org.neo4j.driver.internal.net.pooling;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import java.io.IOException;
import java.util.Queue;

import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.net.SocketClient;
import org.neo4j.driver.internal.net.SocketConnection;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.summary.InternalServerInfo;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.Consumers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.logging.DevNullLogger.DEV_NULL_LOGGER;
import static org.neo4j.driver.internal.messaging.ResetMessage.RESET;
import static org.neo4j.driver.internal.net.BoltServerAddress.LOCAL_DEFAULT;

public class PooledConnectionValidatorTest
{
    @Test
    public void resetAndSyncValidConnection()
    {
        Connection connection = mock( Connection.class );
        PooledConnection pooledConnection = newPooledConnection( connection );

        PooledConnectionValidator validator = newValidatorWithMockedPool();
        boolean connectionIsValid = validator.apply( pooledConnection );

        assertTrue( connectionIsValid );

        InOrder inOrder = inOrder( connection );
        inOrder.verify( connection ).reset();
        inOrder.verify( connection ).sync();
    }

    @Test
    public void sendsSingleResetMessageForValidConnection() throws IOException
    {
        SocketClient socket = mock( SocketClient.class );
        InternalServerInfo serverInfo = new InternalServerInfo( LOCAL_DEFAULT, "v1" );
        Connection connection = new SocketConnection( socket, serverInfo, DEV_NULL_LOGGER );
        PooledConnection pooledConnection = newPooledConnection( connection );

        PooledConnectionValidator validator = newValidatorWithMockedPool();
        boolean connectionIsValid = validator.apply( pooledConnection );

        assertTrue( connectionIsValid );

        ArgumentCaptor<Queue<Message>> captor = messagesCaptor();
        verify( socket ).send( captor.capture() );
        assertEquals( 1, captor.getAllValues().size() );
        Queue<Message> messages = captor.getValue();
        assertEquals( 1, messages.size() );
        assertEquals( RESET, messages.peek() );
    }

    private static PooledConnection newPooledConnection( Connection connection )
    {
        return new PooledConnection( connection, Consumers.<PooledConnection>noOp(), Clock.SYSTEM );
    }

    private static PooledConnectionValidator newValidatorWithMockedPool()
    {
        return new PooledConnectionValidator( connectionPoolMock() );
    }

    private static ConnectionPool connectionPoolMock()
    {
        ConnectionPool pool = mock( ConnectionPool.class );
        when( pool.hasAddress( any( BoltServerAddress.class ) ) ).thenReturn( true );
        return pool;
    }

    @SuppressWarnings( "unchecked" )
    private static ArgumentCaptor<Queue<Message>> messagesCaptor()
    {
        return (ArgumentCaptor) ArgumentCaptor.forClass( Queue.class );
    }
}
