/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.driver.internal.util;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Base64;

/**
 * A tool used to save, load certs, etc.
 */
public final class CertificateTool
{
    private static final String BEGIN_CERT = "-----BEGIN CERTIFICATE-----";
    private static final String END_CERT = "-----END CERTIFICATE-----";

    /**
     * Save a certificate to a file in base 64 binary format with BEGIN and END strings
     * @param certStr
     * @param certFile
     * @throws IOException
     */
    public static void saveX509Cert( String certStr, File certFile ) throws IOException
    {
        try ( BufferedWriter writer = new BufferedWriter( new FileWriter( certFile ) ) )
        {
            writer.write( BEGIN_CERT );
            writer.newLine();

            writer.write( certStr );
            writer.newLine();

            writer.write( END_CERT );
            writer.newLine();
        }
    }

    /**
     * Save a certificate to a file. Remove all the content in the file if there is any before.
     *
     * @param cert
     * @param certFile
     * @throws GeneralSecurityException
     * @throws IOException
     */
    public static void saveX509Cert( Certificate cert, File certFile ) throws GeneralSecurityException, IOException
    {
        saveX509Cert( new Certificate[]{cert}, certFile );
    }

    /**
     * Save a list of certificates into a file
     *
     * @param certs
     * @param certFile
     * @throws GeneralSecurityException
     * @throws IOException
     */
    public static void saveX509Cert( Certificate[] certs, File certFile ) throws GeneralSecurityException, IOException
    {
        try ( BufferedWriter writer = new BufferedWriter( new FileWriter( certFile ) ) )
        {
            for ( Certificate cert : certs )
            {
                String certStr = Base64.getEncoder().encodeToString( cert.getEncoded() ).replaceAll( "(.{64})", "$1\n" );

                writer.write( BEGIN_CERT );
                writer.newLine();

                writer.write( certStr );
                writer.newLine();

                writer.write( END_CERT );
                writer.newLine();
            }
        }
    }

    /**
     * Load the certificates written in X.509 format in a file to a key store.
     *
     * @param certFile
     * @param keyStore
     * @throws GeneralSecurityException
     * @throws IOException
     */
    public static void loadX509Cert( File certFile, KeyStore keyStore ) throws GeneralSecurityException, IOException
    {
        try ( BufferedInputStream inputStream = new BufferedInputStream( new FileInputStream( certFile ) ) )
        {
            CertificateFactory certFactory = CertificateFactory.getInstance( "X.509" );

            int certCount = 0; // The file might contain multiple certs
            while ( inputStream.available() > 0 )
            {
                try
                {
                    Certificate cert = certFactory.generateCertificate( inputStream );
                    certCount++;
                    loadX509Cert( cert, "neo4j.javadriver.trustedcert." + certCount, keyStore );
                }
                catch ( CertificateException e )
                {
                    if ( e.getCause() != null && e.getCause().getMessage().equals( "Empty input" ) )
                    {
                        // This happens if there is whitespace at the end of the certificate - we load one cert, and then try and load a
                        // second cert, at which point we fail
                        return;
                    }
                    throw new IOException( "Failed to load certificate from `" + certFile.getAbsolutePath() + "`: " + certCount + " : " + e.getMessage(), e );
                }
            }
        }
    }

    public static void loadX509Cert( X509Certificate[] certificates, KeyStore keyStore ) throws GeneralSecurityException, IOException
    {
        for ( int i = 0; i < certificates.length; i++ )
        {
            loadX509Cert( certificates[i], "neo4j.javadriver.trustedcert." + i, keyStore );
        }
    }

    /**
     * Load a certificate to a key store with a name
     *
     * @param certAlias a name to identify different certificates
     * @param cert
     * @param keyStore
     */
    public static void loadX509Cert( Certificate cert, String certAlias, KeyStore keyStore ) throws KeyStoreException
    {
        keyStore.setCertificateEntry( certAlias, cert );
    }

    /**
     * Convert a certificate in base 64 binary format with BEGIN and END strings
     * @param cert encoded cert string
     * @return
     */
    public static String X509CertToString( String cert )
    {
        String cert64CharPerLine = cert.replaceAll( "(.{64})", "$1\n" );
        return BEGIN_CERT + "\n" + cert64CharPerLine + "\n"+ END_CERT + "\n";
    }

    private CertificateTool()
    {
    }
}



