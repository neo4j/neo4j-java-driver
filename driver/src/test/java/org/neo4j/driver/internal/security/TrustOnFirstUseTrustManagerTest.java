/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.driver.internal.security;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Scanner;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.v1.Logger;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.security.TrustOnFirstUseTrustManager.fingerprint;

class TrustOnFirstUseTrustManagerTest
{
    private File knownCertsFile;

    private String knownServerIp;
    private int knownServerPort;
    private String knownServer;

    private X509Certificate knownCertificate;

    @BeforeEach
    void setUp() throws Throwable
    {
        // create the cert file with one ip:port and some random "cert" in it
        knownCertsFile = Files.createTempFile( Paths.get( "target" ), "known-certs", "" ).toFile();
        knownServerIp = "1.2.3.4";
        knownServerPort = 100;
        knownServer = knownServerIp + ":" + knownServerPort;

        knownCertificate = mock( X509Certificate.class );
        when( knownCertificate.getEncoded() ).thenReturn( "certificate".getBytes( UTF_8 ) );

        PrintWriter writer = new PrintWriter( knownCertsFile );
        writer.println( " # I am a comment." );
        writer.println( knownServer + " " + fingerprint( knownCertificate ) );
        writer.close();
    }

    @AfterEach
    void tearDown() throws IOException
    {
        Files.deleteIfExists( knownCertsFile.toPath() );
    }

    @Test
    void shouldLoadExistingCert() throws Throwable
    {
        // Given
        BoltServerAddress knownServerAddress = new BoltServerAddress( knownServerIp, knownServerPort );
        Logger logger = mock(Logger.class);
        TrustOnFirstUseTrustManager manager =
                new TrustOnFirstUseTrustManager( knownServerAddress, knownCertsFile, logger );

        X509Certificate wrongCertificate = mock( X509Certificate.class );
        when( wrongCertificate.getEncoded() ).thenReturn( "fake certificate".getBytes() );

        // When & Then
        CertificateException e = assertThrows( CertificateException.class, () -> manager.checkServerTrusted( new X509Certificate[]{wrongCertificate}, null ) );
        assertTrue( e.getMessage().contains( "If you trust the certificate the server uses now, simply remove the line that starts with" ) );
        verifyNoMoreInteractions( logger );
    }

    @Test
    void shouldSaveNewCert() throws Throwable
    {
        // Given
        int newPort = 200;
        BoltServerAddress address = new BoltServerAddress( knownServerIp, newPort );
        Logger logger = mock(Logger.class);
        TrustOnFirstUseTrustManager manager = new TrustOnFirstUseTrustManager( address, knownCertsFile, logger );

        String fingerprint = fingerprint( knownCertificate );

        // When
        manager.checkServerTrusted( new X509Certificate[]{knownCertificate}, null );

        // Then no exception should've been thrown, and we should've logged that we now trust this certificate
        verify( logger ).info( "Adding %s as known and trusted certificate for %s.", fingerprint, "1.2.3.4:200" );

        // And the file should contain the right info
        Scanner reader = new Scanner( knownCertsFile );

        String line;
        line = nextLine( reader );
        assertEquals( knownServer + " " + fingerprint, line );
        assertTrue( reader.hasNextLine() );
        line = nextLine( reader );
        assertEquals( knownServerIp + ":" + newPort + " " + fingerprint, line );
    }

    private String nextLine( Scanner reader )
    {
        String line;
        do
        {
            assertTrue( reader.hasNext() );
            line = reader.nextLine();
        }
        while ( line.trim().startsWith( "#" ) );
        return line;
    }

    @Test
    void shouldThrowMeaningfulExceptionIfHasNoReadPermissionToKnownHostFile()
    {
        // Given
        File knownHostFile = mock( File.class );
        when( knownHostFile.canRead() ).thenReturn( false );
        when( knownHostFile.exists() ).thenReturn( true );

        // When & Then
        BoltServerAddress address = new BoltServerAddress( knownServerIp, knownServerPort );
        IOException e = assertThrows( IOException.class, () -> new TrustOnFirstUseTrustManager( address, knownHostFile, null ) );
        assertThat( e.getMessage(), containsString( "you have no read permissions to it" ) );
    }

    @Test
    void shouldThrowMeaningfulExceptionIfHasNoWritePermissionToKnownHostFile() throws Throwable
    {
        // Given
        File knownHostFile = mock( File.class );
        when( knownHostFile.exists() ).thenReturn( false /*skip reading*/, true );
        when( knownHostFile.canWrite() ).thenReturn( false );

        // When & Then
        BoltServerAddress address = new BoltServerAddress( knownServerIp, knownServerPort );
        TrustOnFirstUseTrustManager manager = new TrustOnFirstUseTrustManager( address, knownHostFile, mock( Logger.class ) );

        CertificateException e = assertThrows( CertificateException.class, () -> manager.checkServerTrusted( new X509Certificate[]{knownCertificate}, null ) );
        assertThat( e.getCause().getMessage(), containsString( "you have no write permissions to it" ) );
    }
}
