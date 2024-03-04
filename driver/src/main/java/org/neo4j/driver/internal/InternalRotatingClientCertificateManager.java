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

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.neo4j.driver.ClientCertificate;
import org.neo4j.driver.ClientCertificates;
import org.neo4j.driver.RotatingClientCertificateManager;

public final class InternalRotatingClientCertificateManager implements RotatingClientCertificateManager {
    private CompletionStage<ClientCertificate> clientCertificateStage;
    private InternalClientCertificate clientCertificate;

    public InternalRotatingClientCertificateManager(ClientCertificate clientCertificate) {
        Objects.requireNonNull(clientCertificate);
        clientCertificateStage = CompletableFuture.completedStage(clientCertificate);
        this.clientCertificate = (InternalClientCertificate) clientCertificate;
    }

    @Override
    public synchronized CompletionStage<ClientCertificate> getClientCertificate() {
        if (clientCertificate.hasUpdate()) {
            var stage = clientCertificateStage;
            clientCertificate = (InternalClientCertificate) ClientCertificates.of(
                    clientCertificate.certificate(),
                    clientCertificate.privateKey(),
                    clientCertificate.password(),
                    false);
            clientCertificateStage = CompletableFuture.completedStage(clientCertificate);
            return stage;
        } else {
            return clientCertificateStage;
        }
    }

    @Override
    public void update(ClientCertificate clientCertificate) {
        Objects.requireNonNull(clientCertificate);
        var certificate = (InternalClientCertificate) clientCertificate;
        if (certificate.hasUpdate()) {
            synchronized (this) {
                clientCertificateStage = CompletableFuture.completedStage(certificate);
                this.clientCertificate = certificate;
            }
        }
    }
}
