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
package org.neo4j.driver.util;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Enumeration;

import org.neo4j.driver.internal.util.CertificateTool;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.internal.util.CertificateTool.saveX509Cert;
import static org.neo4j.driver.util.CertificateUtil.generateSelfSignedCertificate;

public class CertificateUtilTest
{
    @Test
    void shouldLoadMultipleCertsIntoKeyStore() throws Throwable
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
