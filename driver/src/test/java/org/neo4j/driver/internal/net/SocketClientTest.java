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
package org.neo4j.driver.internal.net;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.util.ArrayList;
import java.util.List;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.driver.internal.logging.DevNullLogger;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.v1.exceptions.ClientException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SocketClientTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    // TODO: This is not possible with blocking NIO channels, unless we use inputStreams, but then we can't use
    // off-heap buffers. We need to swap to use selectors, which would allow us to time out.
    @Test
    @Ignore
    public void testNetworkTimeout() throws Throwable
    {
        // Given a server that will never reply
        ServerSocket server = new ServerSocket( 0 );
        BoltServerAddress address = new BoltServerAddress( "localhost", server.getLocalPort() );

        SecurityPlan securityPlan = SecurityPlan.insecure();
        SocketClient client = new SocketClient( address, securityPlan, new DevNullLogger() );

        // Expect
        exception.expect( ClientException.class );
        exception.expectMessage( "database took longer than network timeout (100ms) to reply." );

        // When
        client.start();
    }

    private SocketClient dummyClient()
    {
        return new SocketClient( BoltServerAddress.LOCAL_DEFAULT, SecurityPlan.insecure(), new DevNullLogger() );
    }

    @Test
    public void shouldReadAllBytes() throws IOException
    {
        // Given
        ByteBuffer buffer = ByteBuffer.allocate( 4 );
        ByteAtATimeChannel channel = new ByteAtATimeChannel( new byte[]{0, 1, 2, 3} );
        SocketClient client = dummyClient();

        // When
        client.setChannel( channel );
        client.blockingRead( buffer );
        buffer.flip();

        // Then
        assertThat(buffer.get(), equalTo((byte) 0));
        assertThat(buffer.get(), equalTo((byte) 1));
        assertThat(buffer.get(), equalTo((byte) 2));
        assertThat(buffer.get(), equalTo((byte) 3));
    }

    @Test
    public void shouldFailIfConnectionFailsWhileReading() throws IOException
    {
        // Given
        ByteBuffer buffer = ByteBuffer.allocate( 4 );
        ByteChannel channel = mock( ByteChannel.class );
        when(channel.read( buffer )).thenReturn( -1 );
        SocketClient client = dummyClient();

        //Expect
        exception.expect( ClientException.class );
        exception.expectMessage( "Expected 4 bytes, received none" );

        // When
        client.setChannel( channel );
        client.blockingRead( buffer );
    }

    @Test
    public void shouldWriteAllBytes() throws IOException
    {
        // Given
        ByteBuffer buffer = ByteBuffer.wrap(  new byte[]{0, 1, 2, 3});
        ByteAtATimeChannel channel = new ByteAtATimeChannel( new byte[0] );
        SocketClient client = dummyClient();

        // When
        client.setChannel( channel );
        client.blockingWrite( buffer );

        // Then
        assertThat(channel.writtenBytes.get(0), equalTo((byte) 0));
        assertThat(channel.writtenBytes.get(1), equalTo((byte) 1));
        assertThat(channel.writtenBytes.get(2), equalTo((byte) 2));
        assertThat(channel.writtenBytes.get(3), equalTo((byte) 3));
    }

    @Test
    public void shouldFailIfConnectionFailsWhileWriting() throws IOException
    {
        // Given
        ByteBuffer buffer = ByteBuffer.allocate( 4 );
        buffer.position( 1 );
        ByteChannel channel = mock( ByteChannel.class );
        when(channel.write( buffer )).thenReturn( -1 );
        SocketClient client = dummyClient();

        //Expect
        exception.expect( ClientException.class );
        exception.expectMessage( "Expected 4 bytes, wrote 00" );

        // When
        client.setChannel( channel );
        client.blockingWrite( buffer );
    }

    private static class ByteAtATimeChannel implements ByteChannel
    {

        private final byte[] bytes;
        private int index = 0;
        private List<Byte> writtenBytes = new ArrayList<>(  );

        private ByteAtATimeChannel( byte[] bytes )
        {
            this.bytes = bytes;
        }

        @Override
        public int read( ByteBuffer dst ) throws IOException
        {
            if (index >= bytes.length)
            {
                return -1;
            }

            dst.put( bytes[index++]);
            return 1;
        }

        @Override
        public int write( ByteBuffer src ) throws IOException
        {
            writtenBytes.add( src.get() );
            return 1;
        }

        @Override
        public boolean isOpen()
        {
            return true;
        }

        @Override
        public void close() throws IOException
        {

        }
    }

}
