/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.driver.integration;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import javax.net.ssl.SSLHandshakeException;

import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.internal.connector.socket.SSLSocketChannel;
import org.neo4j.driver.internal.connector.socket.SSLTestSocketChannel;
import org.neo4j.driver.internal.logging.DevNullLogger;
import org.neo4j.driver.internal.spi.Logger;
import org.neo4j.driver.internal.util.CertificateTool;
import org.neo4j.driver.util.Neo4jRunner;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static junit.framework.TestCase.assertEquals;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class SSLSocketChannelIT
{
    private static TLSServer server;
    private static File knownCert;

    @BeforeClass
    public static void setup() throws IOException, InterruptedException
    {
        /* uncomment for JSSE debugging info */
        // System.setProperty( "javax.net.debug", "all" );
        // delete any certificate file that the client already know
        server = new TLSServer();
        knownCert = File.createTempFile( "neo4j_known_certs", ".tmp" );
    }

    @AfterClass
    public static void tearDown()
    {
        if ( server != null )
        {
            server.close();
        }
        knownCert.delete();
    }

    @Test
    public void shouldPerformTLSHandshake() throws Throwable
    {
        // Given
        Logger logger = mock( Logger.class );
        SocketChannel channel = SocketChannel.open();
        channel.connect( new InetSocketAddress( "localhost", 7687 ) );

        // When
        SSLSocketChannel sslChannel =
                new SSLSocketChannel( "localhost", 7687, channel, logger, knownCert, null );
        sslChannel.close();

        // Then
        verify( logger, atLeastOnce() ).debug( "TLS connection enabled" );
        verify( logger, atLeastOnce() ).debug( "TLS connection established" );
        verify( logger, atLeastOnce() ).debug( "TLS connection closed" );
    }

    @Test
    public void shouldEnlargeBuffers() throws Throwable
    {
        // Given
        final ArrayList<String> logs = new ArrayList<>();
        Logger logger = new DevNullLogger()
        {
            @Override
            public void debug( String format, Object... params )
            {
                logs.add( String.format( format, params ) );
            }
        };
        SocketChannel channel = SocketChannel.open();
        channel.connect( new InetSocketAddress( "localhost", 7687 ) );

        // When & Then
        SSLTestSocketChannel sslChannel =
                new SSLTestSocketChannel( "localhost", 7687, channel, logger, knownCert, null, 128, 128 );

        assertEquals( 5, logs.size() );
        assertTrue( logs.get( 0 ).equals( "TLS connection enabled" ) );
        assertTrue( logs.get( 1 ).startsWith( "Enlarged network output buffer from 128 to" ) );
        assertTrue( logs.get( 2 ).startsWith( "Enlarged application input buffer from 128 to" ) );
        assertTrue( logs.get( 3 ).startsWith( "Enlarged network input buffer from 128 to" ) );
        assertTrue( logs.get( 4 ).equals( "TLS connection established" ) );


        // Then we will do the protocol handshake
        // Reset the buffer to be a extreme small size
        sslChannel.setBufferSize( 32, 32 );
        ByteBuffer buf = ByteBuffer.wrap( new byte[]{
                0, 0, 0, 1,
                0, 0, 0, 0,
                0, 0, 0, 0,
                0, 0, 0, 0} );
        // Write the bytes to server
        sslChannel.write( buf );

        // Read the 0 0 0 1 from server in two read methods
        buf.clear();
        buf.limit( 2 );
        sslChannel.read( buf );
        buf.flip();
        short proposal = buf.getShort();
        assertEquals( 0, proposal );

        buf.clear();
        buf.limit( 2 );
        sslChannel.read( buf );
        buf.flip();
        proposal = buf.getShort();
        assertEquals( 1, proposal );

        assertEquals( 7, logs.size() );
        assertTrue( logs.get( 5 ).startsWith( "Enlarged network output buffer from 32 to" ) );
        assertTrue( logs.get( 6 ).startsWith( "Enlarged network input buffer from 32 to" ) );

        sslChannel.close();
    }

    @Test
    public void shouldRejectServerConnectionDueToWrongCert() throws Throwable
    {
        // Given
        Logger logger = mock( Logger.class );
        SocketChannel channel = SocketChannel.open();
        channel.connect( new InetSocketAddress( "localhost", 7687 ) );
        File trustedCert = File.createTempFile( "neo4j_trusted_cert", ".tmp" );
        trustedCert.deleteOnExit();
        CertificateTool.genX509Cert( trustedCert );

        // When & Then
        SSLSocketChannel sslChannel = null;
        try
        {
            sslChannel = new SSLSocketChannel( "localhost", 7687, channel, logger, knownCert, trustedCert );
            sslChannel.close();
        }
        catch ( SSLHandshakeException e )
        {
            assertEquals( "General SSLEngine problem", e.getMessage() );
            assertEquals( "General SSLEngine problem", e.getCause().getMessage() );
            assertEquals( "No trusted certificate found", e.getCause().getCause().getMessage() );
        }
        finally
        {
            if ( sslChannel != null )
            {
                sslChannel.close();
            }
        }
    }

    @Test
    public void shouldEstablishTLSConnection() throws Throwable
    {
        Driver driver = GraphDatabase.driver(
                URI.create( Neo4jRunner.DEFAULT_URL ),
                Config.build().withTLSEnabled( true ).toConfig() );

        Result result = driver.session().run( "RETURN 1" );
        assertTrue( result.next() );
        assertEquals( 1, result.get( 0 ).javaInteger() );
        assertFalse( result.next() );

        driver.close();
    }

    private static class TLSServer
    {
        private Neo4jRunner server;

        public TLSServer() throws IOException, InterruptedException
        {
            server = Neo4jRunner.getOrCreateGlobalServer();
            server.enableTLS( true );
        }

        public void close()
        {
            server.enableTLS( false );
        }

    }

}
