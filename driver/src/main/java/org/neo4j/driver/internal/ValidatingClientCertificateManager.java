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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.neo4j.driver.ClientCertificate;
import org.neo4j.driver.ClientCertificateManager;
import org.neo4j.driver.exceptions.ClientException;

public class ValidatingClientCertificateManager implements ClientCertificateManager {
    private final ClientCertificateManager delegate;

    public ValidatingClientCertificateManager(ClientCertificateManager delegate) {
        this.delegate = delegate;
    }

    @Override
    public CompletionStage<ClientCertificate> getClientCertificate() {
        CompletionStage<ClientCertificate> certificateStage;
        try {
            certificateStage = delegate.getClientCertificate();
        } catch (Throwable throwable) {
            var message = "An exception has been thrown by the ClientCertificateManager.";
            return CompletableFuture.failedFuture(new ClientException(
                    GqlStatusError.UNKNOWN.getStatus(),
                    GqlStatusError.UNKNOWN.getStatusDescription(message),
                    "N/A",
                    message,
                    GqlStatusError.DIAGNOSTIC_RECORD,
                    throwable));
        }
        return certificateStage;
    }
}
