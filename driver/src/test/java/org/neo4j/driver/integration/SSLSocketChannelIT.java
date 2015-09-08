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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.channels.SocketChannel;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLHandshakeException;
import javax.xml.bind.DatatypeConverter;

import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.internal.connector.socket.SSLSocketChannel;
import org.neo4j.driver.internal.spi.Logger;
import org.neo4j.driver.util.Neo4jRunner;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;
import static junit.framework.TestCase.assertEquals;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.driver.Config.TLSAuthenticationConfig.usingKnownCerts;
import static org.neo4j.driver.Config.TLSAuthenticationConfig.usingTrustedCert;
import static org.neo4j.driver.internal.ConfigTest.deleteDefaultKnownCertFileIfExists;
import static org.neo4j.driver.internal.util.CertificateTool.saveX509Cert;
import static org.neo4j.driver.util.CertificateToolTest.generateSelfSignedCertificate;

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
    public void shouldPerformTLSHandshakeWithEmptyKnownCertsFile() throws Throwable
    {
        File knownCerts = File.createTempFile( "neo4j_known_certs", ".tmp" );
        knownCerts.deleteOnExit();

        performTLSHandshakeUsingKnownCerts( knownCerts );
    }

    private void performTLSHandshakeUsingKnownCerts( File knownCerts ) throws Throwable
    {
        // Given
        Logger logger = mock( Logger.class );
        SocketChannel channel = SocketChannel.open();
        channel.connect( new InetSocketAddress( "localhost", 7687 ) );

        // When
        SSLSocketChannel sslChannel =
                new SSLSocketChannel( "localhost", 7687, channel, logger, usingKnownCerts( knownCerts ) );
        sslChannel.close();

        // Then
        verify( logger, atLeastOnce() ).debug( "TLS connection enabled" );
        verify( logger, atLeastOnce() ).debug( "TLS connection established" );
        verify( logger, atLeastOnce() ).debug( "TLS connection closed" );
    }

    @Test
    public void shouldFailTLSHandshakeDueToWrongCertInKnownCertsFile() throws Throwable
    {
        // Given
        SocketChannel channel = SocketChannel.open();
        channel.connect( new InetSocketAddress( "localhost", 7687 ) );
        File knownCerts = File.createTempFile( "neo4j_known_certs", ".tmp" );
        knownCerts.deleteOnExit();

        //create a Fake Cert for the server in knownCert
        createFakeServerCertPairInKnownCerts( "localhost", 7687, knownCerts );

        // When & Then
        SSLSocketChannel sslChannel = null;
        try
        {
            sslChannel = new SSLSocketChannel( "localhost", 7687, channel, mock( Logger.class ),
                    usingKnownCerts( knownCerts ) );
            sslChannel.close();
        }
        catch ( SSLHandshakeException e )
        {
            assertEquals( "General SSLEngine problem", e.getMessage() );
            assertEquals( "General SSLEngine problem", e.getCause().getMessage() );
            assertTrue( e.getCause().getCause().getMessage().contains(
                    "If you trust the certificate the server uses now, simply remove the line that starts with" ) );
        }
        finally
        {
            if ( sslChannel != null )
            {
                sslChannel.close();
            }
        }
    }

    private void createFakeServerCertPairInKnownCerts( String host, int port, File knownCerts )
            throws Throwable
    {
        String ip = InetAddress.getByName( host ).getHostAddress(); // localhost -> 127.0.0.1
        String serverId = ip + ":" + port;

        X509Certificate cert = generateSelfSignedCertificate();
        String certStr = DatatypeConverter.printBase64Binary( cert.getEncoded() );

        BufferedWriter writer = new BufferedWriter( new FileWriter( knownCerts, true ) );
        writer.write( serverId + "," + certStr );
        writer.newLine();
        writer.close();
    }

    @Test
    public void shouldFailTLSHandshakeDueToServerCertNotSignedByKnownCA() throws Throwable
    {
        // Given
        SocketChannel channel = SocketChannel.open();
        channel.connect( new InetSocketAddress( "localhost", 7687 ) );
        File trustedCertFile = File.createTempFile( "neo4j_trusted_cert", ".tmp" );
        trustedCertFile.deleteOnExit();
        X509Certificate aRandomCert = generateSelfSignedCertificate();
        saveX509Cert( aRandomCert, trustedCertFile );

        // When & Then
        SSLSocketChannel sslChannel = null;
        try
        {
            sslChannel = new SSLSocketChannel( "localhost", 7687, channel, mock( Logger.class ),
                    usingTrustedCert( trustedCertFile ) );
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
    public void shouldPerformTLSHandshakeWithTrustedServerCert() throws Throwable
    {
        // Given
        File knownCerts = File.createTempFile( "neo4j_known_certs", ".tmp" );
        knownCerts.deleteOnExit();
        performTLSHandshakeUsingKnownCerts( knownCerts );

        String certStr = getServerCert( knownCerts );

        File trustedCert = File.createTempFile( "neo4j_trusted_cert", ".tmp" );
        trustedCert.deleteOnExit();
        saveX509Cert( certStr, trustedCert );

        Logger logger = mock( Logger.class );
        SocketChannel channel = SocketChannel.open();
        channel.connect( new InetSocketAddress( "localhost", 7687 ) );

        // When
        SSLSocketChannel sslChannel = new SSLSocketChannel( "localhost", 7687, channel, logger,
                usingTrustedCert( trustedCert ) );
        sslChannel.close();

        // Then
        verify( logger, atLeastOnce() ).debug( "TLS connection enabled" );
        verify( logger, atLeastOnce() ).debug( "TLS connection established" );
        verify( logger, atLeastOnce() ).debug( "TLS connection closed" );
    }

    private String getServerCert( File knownCerts ) throws Throwable
    {
        BufferedReader reader = new BufferedReader( new FileReader( knownCerts ) );

        String line = reader.readLine();
        assertNotNull( line );
        String[] strings = line.split( "," );
        assertEquals( 2, strings.length );
        String certStr = strings[1].trim();

        assertNull( reader.readLine() );
        reader.close();

        return certStr;
    }

    @Test
    public void shouldEstablishTLSConnection() throws Throwable
    {
        deleteDefaultKnownCertFileIfExists();
        Config config = Config.build().withTLSEnabled( true ).toConfig();

        Driver driver = GraphDatabase.driver(
                URI.create( Neo4jRunner.DEFAULT_URL ),
                config );

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
