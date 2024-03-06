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
package neo4j.org.testkit.backend.messages.requests;

import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import lombok.Getter;
import lombok.Setter;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.responses.ClientCertificateProvider;
import neo4j.org.testkit.backend.messages.responses.ClientCertificateProviderRequest;
import neo4j.org.testkit.backend.messages.responses.TestkitCallback;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import org.neo4j.driver.ClientCertificate;
import org.neo4j.driver.ClientCertificateManager;
import org.neo4j.driver.ClientCertificates;

@Setter
@Getter
public class NewClientCertificateProvider extends AbstractBasicTestkitRequest {
    private NewClientCertificateProviderBody data;

    @Override
    protected TestkitResponse processAndCreateResponse(TestkitState testkitState) {
        var id = testkitState.newId();
        testkitState.addClientCertificateManager(id, new TestkitClientCertificateManager(id, testkitState));
        return ClientCertificateProvider.builder()
                .data(ClientCertificateProvider.ClientCertificateProviderBody.builder()
                        .id(id)
                        .build())
                .build();
    }

    private record TestkitClientCertificateManager(String id, TestkitState testkitState)
            implements ClientCertificateManager {
        @Override
        public CompletionStage<ClientCertificate> getClientCertificate() {
            var callbackId = testkitState.newId();

            var callback = ClientCertificateProviderRequest.builder()
                    .data(ClientCertificateProviderRequest.ClientCertificateProviderRequestBody.builder()
                            .clientCertificateProviderId(id)
                            .build())
                    .build();

            ClientCertificate clientCertificate = null;
            var callbackStage = dispatchTestkitCallback(testkitState, callback);
            try {
                var response = callbackStage.toCompletableFuture().get();
                if (response instanceof ClientCertificateProviderCompleted clientCertificateComplete) {
                    var data = clientCertificateComplete.getData();
                    var certificateData = data.getClientCertificate().getData();
                    var hasUpdate = data.isHasUpdate();
                    if (hasUpdate) {
                        clientCertificate = ClientCertificates.of(
                                Paths.get(certificateData.getCertfile()).toFile(),
                                Paths.get(certificateData.getKeyfile()).toFile(),
                                certificateData.getPassword());
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("Unexpected failure during Testkit callback", e);
            }
            return CompletableFuture.completedStage(clientCertificate);
        }

        private CompletionStage<TestkitCallbackResult> dispatchTestkitCallback(
                TestkitState testkitState, TestkitCallback response) {
            var future = new CompletableFuture<TestkitCallbackResult>();
            testkitState.getCallbackIdToFuture().put(response.getCallbackId(), future);
            testkitState.getResponseWriter().accept(response);
            return future;
        }
    }

    @Setter
    @Getter
    public static class NewClientCertificateProviderBody {}
}
