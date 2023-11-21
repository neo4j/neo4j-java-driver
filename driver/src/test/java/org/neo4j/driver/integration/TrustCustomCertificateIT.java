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
package org.neo4j.driver.integration;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.driver.Config.TrustStrategy.trustCustomCertificateSignedBy;
import static org.neo4j.driver.testutil.CertificateUtil.createNewCertificateAndKey;
import static org.neo4j.driver.testutil.CertificateUtil.createNewCertificateAndKeySignedBy;
import static org.neo4j.driver.testutil.DatabaseExtension.getDockerHostGeneralName;

import java.io.File;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.exceptions.SecurityException;
import org.neo4j.driver.testutil.DatabaseExtension;
import org.neo4j.driver.testutil.ParallelizableIT;

@ParallelizableIT
class TrustCustomCertificateIT {
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    @Test
    void shouldAcceptServerWithCertificateSignedByDriverCertificate() throws Throwable {
        // Given root certificate
        var root = createNewCertificateAndKey();

        // When
        var server = createNewCertificateAndKeySignedBy(root, getDockerHostGeneralName());
        neo4j.updateEncryptionKeyAndCert(server.key(), server.cert());

        // Then
        shouldBeAbleToRunCypher(() -> createDriverWithCustomCertificate(root.cert()));
    }

    @Test
    void shouldAcceptServerWithSameCertificate() {
        shouldBeAbleToRunCypher(() -> createDriverWithCustomCertificate(neo4j.tlsCertFile()));
    }

    @Test
    @SuppressWarnings("resource")
    void shouldRejectServerWithUntrustedCertificate() throws Throwable {
        // Given a driver with a (random) cert
        var certificateAndKey = createNewCertificateAndKey();

        // When & Then
        final var driver = createDriverWithCustomCertificate(certificateAndKey.cert());
        assertThrows(SecurityException.class, driver::verifyConnectivity);
    }

    private void shouldBeAbleToRunCypher(Supplier<Driver> driverSupplier) {
        try (var driver = driverSupplier.get();
                var session = driver.session()) {
            var result = session.run("RETURN 1 as n");
            assertThat(result.single().get("n").asInt(), equalTo(1));
        }
    }

    private Driver createDriverWithCustomCertificate(File cert) {
        return GraphDatabase.driver(
                neo4j.uri(),
                neo4j.authTokenManager(),
                Config.builder()
                        .withEncryption()
                        .withTrustStrategy(trustCustomCertificateSignedBy(cert))
                        .build());
    }
}
