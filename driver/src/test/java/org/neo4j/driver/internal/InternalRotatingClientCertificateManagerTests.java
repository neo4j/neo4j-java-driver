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
package org.neo4j.driver.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.io.File;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.ClientCertificates;

class InternalRotatingClientCertificateManagerTests {
    InternalRotatingClientCertificateManager manager;

    @Test
    void shouldThrowOnNullInitialCertificate() {
        assertThrows(NullPointerException.class, () -> new InternalRotatingClientCertificateManager(null));
    }

    @Test
    void shouldThrowOnNullUpdatedCertificate() {
        var file = mock(File.class);
        var certificate = ClientCertificates.of(file, file);
        var manager = new InternalRotatingClientCertificateManager(certificate);

        assertThrows(NullPointerException.class, () -> manager.rotate(null));
    }

    @Test
    void shouldSupplyNullOnSubsequentCalls() {
        var file = mock(File.class);
        var certificate = ClientCertificates.of(file, file);
        var manager = new InternalRotatingClientCertificateManager(certificate);
        var actualCertificate =
                manager.getClientCertificate().toCompletableFuture().join();

        for (var i = 0; i < 5; i++) {
            assertNull(manager.getClientCertificate().toCompletableFuture().join());
        }

        assertEquals(certificate, actualCertificate);
    }

    @Test
    void shouldUpdateCertificate() {
        var file = mock(File.class);
        var certificate = ClientCertificates.of(file, file);
        var manager = new InternalRotatingClientCertificateManager(certificate);
        var actualCertificate =
                manager.getClientCertificate().toCompletableFuture().join();
        manager.getClientCertificate().toCompletableFuture().join();

        manager.rotate(certificate);

        var updatedCertificate =
                manager.getClientCertificate().toCompletableFuture().join();
        assertEquals(certificate, actualCertificate);
        assertEquals(certificate, updatedCertificate);
    }
}
