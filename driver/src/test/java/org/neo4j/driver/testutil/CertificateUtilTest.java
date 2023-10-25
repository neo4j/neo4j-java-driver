/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.driver.testutil;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.internal.util.CertificateTool.saveX509Cert;

import java.io.File;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.internal.util.CertificateTool;

public class CertificateUtilTest {
    @Test
    void shouldLoadMultipleCertsIntoKeyStore() throws Throwable {
        // Given
        var certFile = File.createTempFile("3random", ".cer");
        certFile.deleteOnExit();

        var cert1 = CertificateUtil.generateSelfSignedCertificate();
        var cert2 = CertificateUtil.generateSelfSignedCertificate();
        var cert3 = CertificateUtil.generateSelfSignedCertificate();

        saveX509Cert(new Certificate[] {cert1, cert2, cert3}, certFile);

        var keyStore = KeyStore.getInstance("JKS");
        keyStore.load(null, null);

        // When
        CertificateTool.loadX509Cert(Collections.singletonList(certFile), keyStore);

        // Then
        var aliases = keyStore.aliases();
        assertTrue(aliases.hasMoreElements());
        assertTrue(aliases.nextElement().startsWith("neo4j.javadriver.trustedcert"));
        assertTrue(aliases.hasMoreElements());
        assertTrue(aliases.nextElement().startsWith("neo4j.javadriver.trustedcert"));
        assertTrue(aliases.hasMoreElements());
        assertTrue(aliases.nextElement().startsWith("neo4j.javadriver.trustedcert"));
        assertFalse(aliases.hasMoreElements());
    }
}
