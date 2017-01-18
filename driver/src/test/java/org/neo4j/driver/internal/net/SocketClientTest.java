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

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.neo4j.driver.internal.logging.DevNullLogger;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;

import static junit.framework.TestCase.fail;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.runners.Parameterized.Parameters;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.net.BoltServerAddress.LOCAL_DEFAULT;

@RunWith(Parameterized.class)
public class SocketClientTest
{
    private static final int CONNECTION_TIMEOUT = 42;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Parameters(name = "{0} connections")
    public static Collection<Object[]> data()
    {
        return Arrays.asList(new Object[][] {
                { "insecure", SecurityPlan.insecure() },
                { "encrypted", createEncryptedSecurityPlan() }
        });
    }
    private static SecurityPlan createEncryptedSecurityPlan()
    {
        try
        {
            return SecurityPlan.forAllCertificates();
        }
        catch ( GeneralSecurityException | IOException e )
        {
            fail("Ensuring the creation of certs is not part of the focus of the test, while if failed to create a " +
                 "cert, then this need to be fixed before running more tests on the top of this.");
        }
        return null;
    }

    private SecurityPlan plan;
    public SocketClientTest( String testName, SecurityPlan plan )
    {
        this.plan = plan;
    }

    // TODO: This is not possible with blocking NIO channels, unless we use inputStreams, but then we can't use
    // off-heap buffers. We need to swap to use selectors, which would allow us to time out.
    @Test
    @Ignore
    public void testNetworkTimeout() throws Throwable
    {
        // Given a server that will never reply
        ServerSocket server = new ServerSocket( 0 );
        BoltServerAddress address = new BoltServerAddress( "localhost", server.getLocalPort() );

        SocketClient client = dummyClient( address );

        // Expect
        exception.expect( ClientException.class );
        exception.expectMessage( "database took longer than network timeout (100ms) to reply." );

        // When
        client.start();
    }

    @Test
    public void testConnectionTimeout() throws Throwable
    {
        BoltServerAddress address = new BoltServerAddress( "localhost", 1234 );

        SocketClient client = dummyClient( address );

        // Expect
        exception.expect( ServiceUnavailableException.class );
        exception.expectMessage( "Unable to connect to localhost:1234, " +
                                 "ensure the database is running and that there is a working network connection to it." );

        // When
        client.start();
    }

    @Test
    public void testIOExceptionWhenFailedToEstablishConnection() throws Throwable
    {
        BoltServerAddress address = new BoltServerAddress( "localhost", 1234 ); // an random address

        SecurityPlan securityPlan = SecurityPlan.insecure();
        SocketClient client = new SocketClient( address, securityPlan, CONNECTION_TIMEOUT, new DevNullLogger() );

        ByteChannel mockedChannel = mock( ByteChannel.class );
        when( mockedChannel.write( any( ByteBuffer.class ) ) )
                .thenThrow( new IOException( "Failed to connect to server due to IOException"
        ) );
        client.setChannel( mockedChannel );

        // Expect
        exception.expect( ServiceUnavailableException.class );
        exception.expectMessage( "Unable to process request: Failed to connect to server due to IOException" );

        // When
        client.start();
    }

    private SocketClient dummyClient( BoltServerAddress address )
    {
        return new SocketClient( address, plan, CONNECTION_TIMEOUT, new DevNullLogger() );
    }

    private SocketClient dummyClient()
    {
        return dummyClient( LOCAL_DEFAULT );
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
        exception.expect( ServiceUnavailableException.class );
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
        exception.expect( ServiceUnavailableException.class );
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
