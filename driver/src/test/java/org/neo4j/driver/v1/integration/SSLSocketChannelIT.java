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
package org.neo4j.driver.v1.integration;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.channels.SocketChannel;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLHandshakeException;
import javax.xml.bind.DatatypeConverter;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.driver.internal.ConfigTest;
import org.neo4j.driver.internal.connector.socket.SSLSocketChannel;
import org.neo4j.driver.internal.spi.Logger;
import org.neo4j.driver.internal.util.CertificateTool;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.ResultCursor;
import org.neo4j.driver.v1.util.CertificateToolTest;
import org.neo4j.driver.v1.util.Neo4jRunner;
import org.neo4j.driver.v1.util.Neo4jSettings;
import org.neo4j.driver.v1.util.TestNeo4j;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class SSLSocketChannelIT
{
    @Rule
    public TestNeo4j neo4j = new TestNeo4j( Neo4jSettings.DEFAULT.usingTLS( true ) );

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
                new SSLSocketChannel( "localhost", 7687, channel, logger, Config.TlsAuthenticationConfig.usingKnownCerts( knownCerts ) );
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
                    Config.TlsAuthenticationConfig.usingKnownCerts( knownCerts ) );
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

        X509Certificate cert = CertificateToolTest.generateSelfSignedCertificate();
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
        X509Certificate aRandomCert = CertificateToolTest.generateSelfSignedCertificate();
        CertificateTool.saveX509Cert( aRandomCert, trustedCertFile );

        // When & Then
        SSLSocketChannel sslChannel = null;
        try
        {
            sslChannel = new SSLSocketChannel( "localhost", 7687, channel, mock( Logger.class ),
                    Config.TlsAuthenticationConfig.usingTrustedCert( trustedCertFile ) );
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
        CertificateTool.saveX509Cert( certStr, trustedCert );

        Logger logger = mock( Logger.class );
        SocketChannel channel = SocketChannel.open();
        channel.connect( new InetSocketAddress( "localhost", 7687 ) );

        // When
        SSLSocketChannel sslChannel = new SSLSocketChannel( "localhost", 7687, channel, logger,
                Config.TlsAuthenticationConfig.usingTrustedCert( trustedCert ) );
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
        ConfigTest.deleteDefaultKnownCertFileIfExists();
        Config config = Config.build().withTlsEnabled( true ).toConfig();

        Driver driver = GraphDatabase.driver(
                URI.create( Neo4jRunner.DEFAULT_URL ),
                config );

        ResultCursor result = driver.session().run( "RETURN 1" );
        assertTrue( result.next() );
        assertEquals( 1, result.value( 0 ).asInt() );
        assertFalse( result.next() );

        driver.close();
    }
}
