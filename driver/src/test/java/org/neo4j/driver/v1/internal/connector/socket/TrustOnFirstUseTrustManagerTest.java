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
package org.neo4j.driver.v1.internal.connector.socket;

import java.io.File;
import java.io.PrintWriter;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Scanner;
import javax.xml.bind.DatatypeConverter;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TrustOnFirstUseTrustManagerTest
{
    private static File knownCertsFile;

    private static String knownServerIp;
    private static int knownServerPort;
    private static String knownServer;

    private static String knownCert;

    @BeforeClass
    public static void setup() throws Throwable
    {
        // create the cert file with one ip:port and some random "cert" in it
        knownCertsFile = File.createTempFile( "neo4j_known_certs", ".tmp" );
        knownServerIp = "1.2.3.4";
        knownServerPort = 100;
        knownServer = knownServerIp + ":" + knownServerPort;
        knownCert = DatatypeConverter.printBase64Binary( "certificate".getBytes() );

        PrintWriter writer = new PrintWriter( knownCertsFile );
        writer.println( " # I am a comment." );
        writer.println( knownServer + "," + knownCert );
        writer.close();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @AfterClass
    public static void teardown()
    {
        knownCertsFile.delete();
    }

    @Test
    public void shouldLoadExistingCert() throws Throwable
    {
        // Given
        TrustOnFirstUseTrustManager manager =
                new TrustOnFirstUseTrustManager( knownServerIp, knownServerPort, knownCertsFile );

        X509Certificate fakeCert = mock( X509Certificate.class );
        when( fakeCert.getEncoded() ).thenReturn( "fake certificate".getBytes() );

        // When & Then
        try
        {
            manager.checkServerTrusted( new X509Certificate[]{fakeCert}, null );
            fail( "Should not trust the fake certificate" );
        }
        catch ( CertificateException e )
        {
            assertTrue( e.getMessage().contains(
                    "If you trust the certificate the server uses now, simply remove the line that starts with" ) );
        }
    }

    @Test
    public void shouldSaveNewCert() throws Throwable
    {
        // Given
        int newPort = 200;
        TrustOnFirstUseTrustManager manager = new TrustOnFirstUseTrustManager( knownServerIp, newPort, knownCertsFile );

        byte[] encoded = "certificate".getBytes();
        String cert = DatatypeConverter.printBase64Binary( encoded );

        X509Certificate newCert = mock( X509Certificate.class );
        when( newCert.getEncoded() ).thenReturn( encoded );

        // When && Then
        try
        {
            manager.checkServerTrusted( new X509Certificate[]{newCert}, null );
        }
        catch ( CertificateException e )
        {
            fail( "Should trust the certificate the first time it is seen" );
            e.printStackTrace();
        }

        Scanner reader = new Scanner( knownCertsFile );

        String line;
        line = nextLine( reader );
        assertEquals( knownServer + "," + cert, line );
        assertTrue( reader.hasNextLine() );
        line = nextLine( reader );
        assertEquals( knownServerIp + ":" + newPort + "," + cert, line );
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
}
