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
package org.neo4j.driver.internal.async.connection;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.internal.util.ServerVersion;
import org.neo4j.driver.net.ServerAddress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.AccessMode.READ;
import static org.neo4j.driver.internal.DatabaseNameUtil.defaultDatabase;

class DecoratedConnectionTest
{
    @ParameterizedTest
    @ValueSource( strings = {"true", "false"} )
    void shouldDelegateIsOpen( String open )
    {
        Connection mockConnection = mock( Connection.class );
        when( mockConnection.isOpen() ).thenReturn( Boolean.valueOf( open ) );

        DirectConnection connection = newConnection( mockConnection );

        assertEquals( Boolean.valueOf( open ).booleanValue(), connection.isOpen() );
        verify( mockConnection ).isOpen();
    }

    @Test
    void shouldDelegateEnableAutoRead()
    {
        Connection mockConnection = mock( Connection.class );
        DirectConnection connection = newConnection( mockConnection );

        connection.enableAutoRead();

        verify( mockConnection ).enableAutoRead();
    }

    @Test
    void shouldDelegateDisableAutoRead()
    {
        Connection mockConnection = mock( Connection.class );
        DirectConnection connection = newConnection( mockConnection );

        connection.disableAutoRead();

        verify( mockConnection ).disableAutoRead();
    }

    @Test
    void shouldDelegateWrite()
    {
        Connection mockConnection = mock( Connection.class );
        DirectConnection connection = newConnection( mockConnection );

        Message message = mock( Message.class );
        ResponseHandler handler = mock( ResponseHandler.class );

        connection.write( message, handler );

        verify( mockConnection ).write( message, handler );
    }

    @Test
    void shouldDelegateWriteTwoMessages()
    {
        Connection mockConnection = mock( Connection.class );
        DirectConnection connection = newConnection( mockConnection );

        Message message1 = mock( Message.class );
        ResponseHandler handler1 = mock( ResponseHandler.class );
        Message message2 = mock( Message.class );
        ResponseHandler handler2 = mock( ResponseHandler.class );

        connection.write( message1, handler1, message2, handler2 );

        verify( mockConnection ).write( message1, handler1, message2, handler2 );
    }

    @Test
    void shouldDelegateWriteAndFlush()
    {
        Connection mockConnection = mock( Connection.class );
        DirectConnection connection = newConnection( mockConnection );

        Message message = mock( Message.class );
        ResponseHandler handler = mock( ResponseHandler.class );

        connection.writeAndFlush( message, handler );

        verify( mockConnection ).writeAndFlush( message, handler );
    }

    @Test
    void shouldDelegateWriteAndFlush1()
    {
        Connection mockConnection = mock( Connection.class );
        DirectConnection connection = newConnection( mockConnection );

        Message message1 = mock( Message.class );
        ResponseHandler handler1 = mock( ResponseHandler.class );
        Message message2 = mock( Message.class );
        ResponseHandler handler2 = mock( ResponseHandler.class );

        connection.writeAndFlush( message1, handler1, message2, handler2 );

        verify( mockConnection ).writeAndFlush( message1, handler1, message2, handler2 );
    }

    @Test
    void shouldDelegateReset()
    {
        Connection mockConnection = mock( Connection.class );
        DirectConnection connection = newConnection( mockConnection );

        connection.reset();

        verify( mockConnection ).reset();
    }

    @Test
    void shouldDelegateRelease()
    {
        Connection mockConnection = mock( Connection.class );
        DirectConnection connection = newConnection( mockConnection );

        connection.release();

        verify( mockConnection ).release();
    }

    @Test
    void shouldDelegateTerminateAndRelease()
    {
        Connection mockConnection = mock( Connection.class );
        DirectConnection connection = newConnection( mockConnection );

        connection.terminateAndRelease( "a reason" );

        verify( mockConnection ).terminateAndRelease( "a reason" );
    }

    @Test
    void shouldDelegateServerAddress()
    {
        BoltServerAddress address = BoltServerAddress.from( ServerAddress.of( "localhost", 9999 ) );
        Connection mockConnection = mock( Connection.class );
        when( mockConnection.serverAddress() ).thenReturn( address );
        DirectConnection connection = newConnection( mockConnection );

        assertSame( address, connection.serverAddress() );
        verify( mockConnection ).serverAddress();
    }

    @Test
    void shouldDelegateServerVersion()
    {
        ServerVersion version = ServerVersion.version( "Neo4j/3.5.3" );
        Connection mockConnection = mock( Connection.class );
        when( mockConnection.serverVersion() ).thenReturn( version );
        DirectConnection connection = newConnection( mockConnection );

        assertSame( version, connection.serverVersion() );
        verify( mockConnection ).serverVersion();
    }

    @Test
    void shouldDelegateProtocol()
    {
        BoltProtocol protocol = mock( BoltProtocol.class );
        Connection mockConnection = mock( Connection.class );
        when( mockConnection.protocol() ).thenReturn( protocol );
        DirectConnection connection = newConnection( mockConnection );

        assertSame( protocol, connection.protocol() );
        verify( mockConnection ).protocol();
    }

    @ParameterizedTest
    @EnumSource( AccessMode.class )
    void shouldReturnModeFromConstructor( AccessMode mode )
    {
        DirectConnection connection = new DirectConnection( mock( Connection.class ), defaultDatabase(), mode );

        assertEquals( mode, connection.mode() );
    }

    @Test
    void shouldReturnConnection()
    {
        Connection mockConnection = mock( Connection.class );
        DirectConnection connection = newConnection( mockConnection );

        assertSame( mockConnection, connection.connection() );
    }
    
    private static DirectConnection newConnection( Connection connection )
    {
        return new DirectConnection( connection, defaultDatabase(), READ );
    }
}
