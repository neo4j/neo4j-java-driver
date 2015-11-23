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
package org.neo4j.driver.v1.util;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.AlgorithmIdentifier;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v1CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.crypto.params.AsymmetricKeyParameter;
import org.bouncycastle.crypto.util.PrivateKeyFactory;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.DefaultDigestAlgorithmIdentifierFinder;
import org.bouncycastle.operator.DefaultSignatureAlgorithmIdentifierFinder;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.bc.BcRSAContentSignerBuilder;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.Enumeration;

import org.neo4j.driver.v1.internal.util.CertificateTool;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.driver.v1.internal.util.CertificateTool.saveX509Cert;

public class CertificateToolTest
{
    static
    {
        // adds the Bouncy castle provider to java security
        Security.addProvider( new BouncyCastleProvider() );
    }

    /**
     * Create a random certificate
     *
     * @return
     * @throws GeneralSecurityException, IOException, OperatorCreationException
     */
    public static X509Certificate generateSelfSignedCertificate()
            throws GeneralSecurityException, IOException, OperatorCreationException
    {
        // Create the public/private rsa key pair
        KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance( "RSA", "BC" );
        keyPairGenerator.initialize( 1024, new SecureRandom() );
        KeyPair keyPair = keyPairGenerator.generateKeyPair();

        // Create x509 certificate
        Date startDate = new Date( System.currentTimeMillis() );
        Date endDate = new Date( System.currentTimeMillis() + 365L * 24L * 60L * 60L * 1000L );
        BigInteger serialNum = BigInteger.valueOf( System.currentTimeMillis() );
        SubjectPublicKeyInfo publicKeyInfo = SubjectPublicKeyInfo.getInstance( keyPair.getPublic().getEncoded() );
        X509v1CertificateBuilder certBuilder = new X509v1CertificateBuilder(
                new X500Name( "CN=NEO4J_JAVA_DRIVER" ),
                serialNum,
                startDate, endDate,
                new X500Name( "CN=Test" ),
                publicKeyInfo );

        // Get the certificate back
        AlgorithmIdentifier sigAlgId = new DefaultSignatureAlgorithmIdentifierFinder().find( "SHA1withRSA" );
        AlgorithmIdentifier digAlgId = new DefaultDigestAlgorithmIdentifierFinder().find( sigAlgId );

        AsymmetricKeyParameter privateKey = PrivateKeyFactory.createKey( keyPair.getPrivate().getEncoded() );
        ContentSigner signer = new BcRSAContentSignerBuilder( sigAlgId, digAlgId ).build( privateKey );
        X509CertificateHolder certHolder = certBuilder.build( signer );

        return new JcaX509CertificateConverter().setProvider( "BC" ).getCertificate( certHolder );
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
