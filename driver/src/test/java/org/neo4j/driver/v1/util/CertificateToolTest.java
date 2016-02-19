/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.driver.v1.util;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.bouncycastle.pkcs.PKCS10CertificationRequestBuilder;
import org.bouncycastle.pkcs.jcajce.JcaPKCS10CertificationRequestBuilder;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemWriter;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.Enumeration;
import javax.security.auth.x500.X500Principal;

import org.neo4j.driver.internal.util.CertificateTool;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.driver.internal.util.CertificateTool.saveX509Cert;

public class CertificateToolTest
{
    static
    {
        // adds the Bouncy castle provider to java security
        Security.addProvider( new BouncyCastleProvider() );
    }

    public static KeyPair generateKeyPair() throws NoSuchProviderException, NoSuchAlgorithmException
    {
        KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance( "RSA", "BC" );
        keyPairGenerator.initialize( 2048, new SecureRandom() );
        KeyPair keyPair = keyPairGenerator.generateKeyPair();
        return keyPair;
    }

    public static X509Certificate generateCert( X500Name issuer, X500Name subject, KeyPair issuerKeys, PublicKey
            publicKey )
            throws GeneralSecurityException, IOException, OperatorCreationException
    {
        // Create x509 certificate
        Date startDate = new Date( System.currentTimeMillis() );
        Date endDate = new Date( System.currentTimeMillis() + 365L * 24L * 60L * 60L * 1000L );
        BigInteger serialNum = BigInteger.valueOf( System.currentTimeMillis() );
        X509v3CertificateBuilder certBuilder = new JcaX509v3CertificateBuilder(
                issuer,
                serialNum,
                startDate, endDate,
                subject,
                publicKey );

        // Get the certificate back
        ContentSigner signer =
                new JcaContentSignerBuilder( "SHA512WithRSAEncryption" ).build( issuerKeys.getPrivate() );
        X509CertificateHolder certHolder = certBuilder.build( signer );
        X509Certificate certificate = new JcaX509CertificateConverter().setProvider( "BC" )
                .getCertificate( certHolder );

        certificate.verify( issuerKeys.getPublic() );
        return certificate;
    }

    public static class SelfSignedCertificateGenerator
    {
        private final KeyPair keyPair;
        private final X509Certificate certificate;

        public SelfSignedCertificateGenerator()
                throws GeneralSecurityException, IOException, OperatorCreationException
        {
            // Create the public/private rsa key pair
            keyPair = generateKeyPair();

            // Create x509 certificate
            certificate = generateCert(
                    new X500Name( "CN=NEO4J_JAVA_DRIVER_TEST_ROOT" ),
                    new X500Name( "CN=NEO4J_JAVA_DRIVER_TEST_ROOT" ),
                    keyPair,
                    keyPair.getPublic() );
        }

        public void savePrivateKey( File saveTo ) throws IOException
        {
            writePem( "PRIVATE KEY", keyPair.getPrivate().getEncoded(), saveTo );
        }

        public void saveSelfSignedCertificate( File saveTo ) throws CertificateEncodingException, IOException
        {
            writePem( "CERTIFICATE", certificate.getEncoded(), saveTo );
        }

        public X509Certificate sign(PKCS10CertificationRequest csr, PublicKey csrPublicKey )
                throws GeneralSecurityException, IOException, OperatorCreationException
        {
            X509Certificate certificate = generateCert(
                    X500Name.getInstance( this.certificate.getSubjectX500Principal().getEncoded() ),
                    csr.getSubject(), keyPair,
                    csrPublicKey );
            return certificate;

        }
    }

    public static class CertificateSigningRequestGenerator
    {
        // ref: http://senthadev.com/generating-csr-using-java-and-bouncycastle-api.html
        private final KeyPair keyPair;
        private final PKCS10CertificationRequest csr;

        public CertificateSigningRequestGenerator() throws NoSuchAlgorithmException, OperatorCreationException
        {
            KeyPairGenerator gen = KeyPairGenerator.getInstance( "RSA" );
            gen.initialize( 2048, new SecureRandom() );
            keyPair = gen.generateKeyPair();

            X500Principal subject = new X500Principal( "CN=NEO4j_JAVA_DRIVER_TEST_SERVER" );
            ContentSigner signGen =
                    new JcaContentSignerBuilder( "SHA512WithRSAEncryption" ).build( keyPair.getPrivate() );

            PKCS10CertificationRequestBuilder builder =
                    new JcaPKCS10CertificationRequestBuilder( subject, keyPair.getPublic() );
            csr = builder.build( signGen );
        }

        public PrivateKey privateKey()
        {
            return keyPair.getPrivate();
        }

        public PublicKey publicKey()
        {
            return keyPair.getPublic();
        }

        public PKCS10CertificationRequest certificateSigningRequest()
        {
            return csr;
        }

        public void savePrivateKey( File saveTo ) throws IOException
        {
            writePem( "PRIVATE KEY", keyPair.getPrivate().getEncoded(), saveTo );
        }
    }

    /**
     * Create a random certificate
     *
     * @return a random certificate
     * @throws GeneralSecurityException, IOException, OperatorCreationException
     */
    public static X509Certificate generateSelfSignedCertificate()
            throws GeneralSecurityException, IOException, OperatorCreationException
    {

        return new SelfSignedCertificateGenerator().certificate;
    }

    private static void writePem( String type, byte[] encodedContent, File path ) throws IOException
    {
        if( path.getParentFile() != null && path.getParentFile().exists() )
        path.getParentFile().mkdirs();
        try ( PemWriter writer = new PemWriter( new FileWriter( path ) ) )
        {
            writer.writeObject( new PemObject( type, encodedContent ) );
            writer.flush();
        }
    }

    @Test
    public void shouldLoadMultipleCertsIntoKeyStore() throws Throwable
    {
        // Given
        File certFile = File.createTempFile( "3random", ".cer" );
        certFile.deleteOnExit();

        X509Certificate cert1 = generateSelfSignedCertificate();
        X509Certificate cert2 = generateSelfSignedCertificate();
        X509Certificate cert3 = generateSelfSignedCertificate();

        saveX509Cert( new Certificate[] {cert1, cert2, cert3}, certFile );

        KeyStore keyStore = KeyStore.getInstance( "JKS" );
        keyStore.load( null, null );

        // When
        CertificateTool.loadX509Cert( certFile, keyStore );

        // Then
        Enumeration<String> aliases = keyStore.aliases();
        assertTrue( aliases.hasMoreElements() );
        assertTrue( aliases.nextElement().startsWith( "neo4j.javadriver.trustedcert" ) );
        assertTrue( aliases.hasMoreElements() );
        assertTrue( aliases.nextElement().startsWith( "neo4j.javadriver.trustedcert" ) );
        assertTrue( aliases.hasMoreElements() );
        assertTrue( aliases.nextElement().startsWith( "neo4j.javadriver.trustedcert" ) );
        assertFalse( aliases.hasMoreElements() );
    }

}
