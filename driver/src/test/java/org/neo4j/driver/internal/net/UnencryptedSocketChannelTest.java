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
package org.neo4j.driver.internal.net;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class UnencryptedSocketChannelTest
{
    private static final byte[] EMPTY_INPUT = new byte[0];

    @Test
    public void shouldReadFromSocketInputStream() throws IOException
    {
        Socket socket = socketMock( new byte[]{1, 2, 3, 4, 5, 42} );
        UnencryptedSocketChannel channel = newChannel( socket );

        ByteBuffer buffer = ByteBuffer.allocate( 100 );
        channel.read( buffer );
        buffer.flip();

        assertEquals( 1, buffer.get() );
        assertEquals( 2, buffer.get() );
        assertEquals( 3, buffer.get() );
        assertEquals( 4, buffer.get() );
        assertEquals( 5, buffer.get() );
        assertEquals( 42, buffer.get() );
        assertEquals( 0, buffer.remaining() );
    }

    @Test
    public void shouldWriteToSocketOutputStream() throws IOException
    {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        Socket socket = socketMock( output );
        UnencryptedSocketChannel channel = newChannel( socket );

        byte[] array = {42, 34, 9, 99, 42, 1, 2};
        ByteBuffer buffer = ByteBuffer.wrap( array );

        channel.write( buffer );

        assertArrayEquals( array, output.toByteArray() );
    }

    @Test
    public void shouldKnowWhenOpen() throws IOException
    {
        Socket openSocket = socketMock( true );
        Socket closedSocket = socketMock( false );

        UnencryptedSocketChannel openChannel = newChannel( openSocket );
        UnencryptedSocketChannel closedChannel = newChannel( closedSocket );

        assertTrue( openChannel.isOpen() );
        assertFalse( closedChannel.isOpen() );
    }

    @Test
    public void shouldCloseSocket() throws IOException
    {
        Socket socket = socketMock( true );
        UnencryptedSocketChannel channel = newChannel( socket );

        channel.close();

        verify( socket ).close();
    }

    private static Socket socketMock( boolean open ) throws IOException
    {
        return socketMock( open, EMPTY_INPUT, new ByteArrayOutputStream() );
    }

    private static Socket socketMock( byte[] input ) throws IOException
    {
        return socketMock( true, input, new ByteArrayOutputStream() );
    }

    private static Socket socketMock( OutputStream output ) throws IOException
    {
        return socketMock( true, EMPTY_INPUT, output );
    }

    private static Socket socketMock( boolean open, byte[] input, OutputStream output ) throws IOException
    {
        Socket socket = mock( Socket.class );
        when( socket.getInputStream() ).thenReturn( new ByteArrayInputStream( input ) );
        when( socket.getOutputStream() ).thenReturn( output );
        when( socket.isClosed() ).thenReturn( !open );
        return socket;
    }

    private static UnencryptedSocketChannel newChannel( Socket socket ) throws IOException
    {
        return new UnencryptedSocketChannel( socket );
    }
}
