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
package org.neo4j.driver.internal.util;

import sun.security.provider.X509Factory;
import sun.security.x509.AlgorithmId;
import sun.security.x509.CertificateAlgorithmId;
import sun.security.x509.CertificateSerialNumber;
import sun.security.x509.CertificateValidity;
import sun.security.x509.CertificateVersion;
import sun.security.x509.CertificateX509Key;
import sun.security.x509.X500Name;
import sun.security.x509.X509CertImpl;
import sun.security.x509.X509CertInfo;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.SignatureException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.Random;
import javax.xml.bind.DatatypeConverter;

import org.neo4j.driver.exceptions.ClientException;

/**
 * A tool related to create, save, load certs, etc.
 */
public class CertificateTool
{
    /**
     * Create a random certificate
     *
     * @return
     * @throws GeneralSecurityException
     * @throws IOException
     */
    public static X509Certificate genX509Cert() throws GeneralSecurityException, IOException
    {
        long validSecs = (long) 365 * 24 * 60 * 60; // valid for one year
        return generateSelfSignedCertificate( new X500Name( "CN=NEO4J_JAVA_DRIVER" ), 1024, validSecs );
    }

    /**
     * Create a random certificate and save it into a file in X.509 format
     *
     * @param saveTo
     * @throws GeneralSecurityException
     * @throws IOException
     */
    public static void genX509Cert( File saveTo ) throws GeneralSecurityException, IOException
    {
        X509Certificate cert = genX509Cert();
        saveX509Cert( cert, saveTo );
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
        BufferedWriter writer = new BufferedWriter( new FileWriter( certFile ) );

        for ( Certificate cert : certs )
        {
            String certStr = DatatypeConverter.printBase64Binary( cert.getEncoded() ).replaceAll( "(.{64})", "$1\n" );

            writer.write( X509Factory.BEGIN_CERT );
            writer.newLine();

            writer.write( certStr );
            writer.newLine();

            writer.write( X509Factory.END_CERT );
            writer.newLine();
        }

        writer.close();
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
        BufferedInputStream inputStream = new BufferedInputStream( new FileInputStream( certFile ) );
        CertificateFactory certFactory = CertificateFactory.getInstance( "X.509" );

        int certCount = 0; // The file might contain multiple certs
        while ( inputStream.available() > 0 )
        {
            Certificate cert = certFactory.generateCertificate( inputStream );
            certCount++;
            loadX509Cert( cert, "neo4j.javadriver.trustedcert." + certCount, keyStore );
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

    private static X509Certificate generateSelfSignedCertificate(X500Name name, int bits, long validSecs)
    {
        try {
            String sigAlg = "SHA1WithRSA";

            KeyPairGenerator keyGen = KeyPairGenerator.getInstance( "RSA" );
            SecureRandom rand = new SecureRandom();
            Date validAfter = new Date();
            Date validBefore = new Date(validAfter.getTime() + validSecs * 1000L);

            keyGen.initialize(bits, rand);
            KeyPair keyPair = keyGen.generateKeyPair();

            PublicKey publicKey = keyPair.getPublic();
            PrivateKey privateKey = keyPair.getPrivate();
            if(!"X.509".equalsIgnoreCase(publicKey.getFormat())) {
                throw new IllegalArgumentException("publicKey\'s is not X.509, but " + publicKey.getFormat());
            }

            X509CertInfo certInfo = new X509CertInfo();
            certInfo.set( "version", new CertificateVersion( 2 ) );
            certInfo.set( "serialNumber", new CertificateSerialNumber( (new Random()).nextInt() & 2147483647 ) );
            certInfo.set( "algorithmID", new CertificateAlgorithmId( AlgorithmId.get(sigAlg) ) );
            certInfo.set( "subject", name );
            certInfo.set( "key", new CertificateX509Key( publicKey ) );
            certInfo.set( "validity", new CertificateValidity(validAfter, validBefore) );
            certInfo.set( "issuer", name );

            X509CertImpl cert = new X509CertImpl(certInfo);
            cert.sign( privateKey, sigAlg );
            return cert;
        } catch (IOException | CertificateException | NoSuchAlgorithmException | SignatureException | NoSuchProviderException | InvalidKeyException e )
        {
            throw new ClientException( "Failed to generate self-signed certificate: " + e.getMessage(), e );
        }
    }
}



