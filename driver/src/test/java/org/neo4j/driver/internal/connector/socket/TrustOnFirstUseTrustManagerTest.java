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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.driver.internal.connector.socket;

import junit.framework.TestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.PrintWriter;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Scanner;
import javax.xml.bind.DatatypeConverter;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static junit.framework.TestCase.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TrustOnFirstUseTrustManagerTest
{
    @BeforeClass
    public static void setup() throws Throwable
    {
        // create the cert file with one ip and some random cert in it
        File certFile = new File( TrustOnFirstUseTrustManager.KNOWN_CERTS_FILE_PATH );
        if ( certFile.exists() )
        {
            certFile.delete();
        }
        PrintWriter writer = new PrintWriter( certFile );
        String knownIP = "1.2.3.4";
        String knownCert = DatatypeConverter.printBase64Binary( "certificate".getBytes() );
        writer.println( knownIP + "," + knownCert );
        writer.close();
    }

    @AfterClass
    public static void teardown()
    {
        new File( TrustOnFirstUseTrustManager.KNOWN_CERTS_FILE_PATH ).delete();
    }

    @Test
    public void shouldLoadExistingCert() throws Throwable
    {
        TrustOnFirstUseTrustManager manager = new TrustOnFirstUseTrustManager( "1.2.3.4" );

        X509Certificate x509Certificate = mock( X509Certificate.class );
        when( x509Certificate.getEncoded() ).thenReturn( "fake certificate".getBytes() );
        try
        {
            manager.checkServerTrusted( new X509Certificate[]{x509Certificate}, "authType" );
            fail( "Should not trust the fake certificate" );
        }
        catch ( CertificateException e )
        {
            assertTrue( e.getMessage().startsWith(
                    "The certificate received from the server is different from the one we've known in file" ) );
        }
    }


    @Test
    public void shouldSaveNewCert() throws Throwable
    {
        TrustOnFirstUseTrustManager manager = new TrustOnFirstUseTrustManager( "4.3.2.1" );

        byte[] encoded = "certificate".getBytes();
        String cert = DatatypeConverter.printBase64Binary( encoded );

        X509Certificate x509Certificate = mock( X509Certificate.class );
        when( x509Certificate.getEncoded() ).thenReturn( encoded );
        try
        {
            manager.checkServerTrusted( new X509Certificate[]{x509Certificate}, "authType" );
        }
        catch ( CertificateException e )
        {
            fail( "Should trust the certificate the first time it is seen" );
            e.printStackTrace();
        }

        File certFile = new File( TrustOnFirstUseTrustManager.KNOWN_CERTS_FILE_PATH );
        Scanner reader = new Scanner( certFile );

        String line;
        assertTrue( reader.hasNextLine() );
        line = reader.nextLine();
        assertEquals( "1.2.3.4," + cert, line );
        assertTrue( reader.hasNextLine() );
        line = reader.nextLine();
        TestCase.assertEquals( "4.3.2.1," + cert, line );
    }
}
