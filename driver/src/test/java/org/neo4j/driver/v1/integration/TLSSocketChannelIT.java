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

import org.junit.Rule;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.channels.SocketChannel;
import java.security.cert.X509Certificate;
import java.util.Scanner;
import javax.net.ssl.SSLHandshakeException;
import javax.xml.bind.DatatypeConverter;

import org.neo4j.driver.internal.ConfigTest;
import org.neo4j.driver.internal.connector.socket.TLSSocketChannel;
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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TLSSocketChannelIT
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
        TLSSocketChannel sslChannel =
                new TLSSocketChannel( "localhost", 7687, channel, logger, Config.TrustStrategy.trustOnFirstUse( knownCerts ) );
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
        TLSSocketChannel sslChannel = null;
        try
        {
            sslChannel = new TLSSocketChannel( "localhost", 7687, channel, mock( Logger.class ),
                    Config.TrustStrategy.trustOnFirstUse( knownCerts ) );
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
        TLSSocketChannel sslChannel = null;
        try
        {
            sslChannel = new TLSSocketChannel( "localhost", 7687, channel, mock( Logger.class ),
                    Config.TrustStrategy.trustSignedBy( trustedCertFile ) );
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
        TestKeys keys = testKeys();
        neo4j.restartServerOnEmptyDatabase( Neo4jSettings.DEFAULT.usingEncryptionKeyAndCert( keys.serverKey, keys.serverCert ) );

        Logger logger = mock( Logger.class );
        SocketChannel channel = SocketChannel.open();
        channel.connect( new InetSocketAddress( "localhost", 7687 ) );

        // When
        TLSSocketChannel sslChannel = new TLSSocketChannel( "localhost", 7687, channel, logger,
                Config.TrustStrategy.trustSignedBy( keys.signingCert ) );
        sslChannel.close();

        // Then
        verify( logger, atLeastOnce() ).debug( "TLS connection enabled" );
        verify( logger, atLeastOnce() ).debug( "TLS connection established" );
        verify( logger, atLeastOnce() ).debug( "TLS connection closed" );
    }

    @Test
    public void shouldEstablishTLSConnection() throws Throwable
    {
        ConfigTest.deleteDefaultKnownCertFileIfExists();
        Config config = Config.build().withEncryptionLevel( Config.EncryptionLevel.REQUIRED ).toConfig();

        Driver driver = GraphDatabase.driver(
                URI.create( Neo4jRunner.DEFAULT_URL ),
                config );

        ResultCursor result = driver.session().run( "RETURN 1" );
        assertTrue( result.next() );
        assertEquals( 1, result.get( 0 ).asInt() );
        assertFalse( result.next() );

        driver.close();
    }

    class TestKeys
    {
        final File serverKey;
        final File serverCert;
        final File signingCert;

        TestKeys( File serverKey, File serverCert, File signingCert )
        {
            this.serverKey = serverKey;
            this.serverCert = serverCert;
            this.signingCert = signingCert;
        }
    }

    TestKeys testKeys() throws IOException
    {
        return new TestKeys( fileFromCertResource( "server.key" ), fileFromCertResource( "server.crt" ), fileFromCertResource( "ca.crt" ) );
    }

    private File fileFromCertResource( String fileName ) throws IOException
    {
        InputStream resourceAsStream = getClass().getClassLoader().getResourceAsStream( "certificates/" + fileName );
        try( Scanner scanner = new Scanner( resourceAsStream ).useDelimiter( "\\A" ) )
        {
            String contents = scanner.next();
            return new File( neo4j.putTmpFile( fileName, "", contents ).getFile() );
        }
    }
}
