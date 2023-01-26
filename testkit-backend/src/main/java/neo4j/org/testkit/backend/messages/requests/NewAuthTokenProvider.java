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
package neo4j.org.testkit.backend.messages.requests;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.Setter;
import neo4j.org.testkit.backend.AuthTokenUtil;
import neo4j.org.testkit.backend.TestkitClock;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.responses.AuthTokenProvider;
import neo4j.org.testkit.backend.messages.responses.AuthTokenProviderRequest;
import neo4j.org.testkit.backend.messages.responses.TestkitCallback;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import org.neo4j.driver.AuthTokenManagers;
import org.neo4j.driver.internal.security.TemporalAuthTokenManager;

@Setter
@Getter
public class NewAuthTokenProvider extends AbstractBasicTestkitRequest {
    private NewAuthTokenProviderBody data;

    @Override
    protected TestkitResponse processAndCreateResponse(TestkitState testkitState) {
        var id = testkitState.newId();
        testkitState.addAuthProvider(
                id,
                new TemporalAuthTokenManager(new TestkitAuthTokenProvider(id, testkitState), TestkitClock.INSTANCE));
        return AuthTokenProvider.builder()
                .data(AuthTokenProvider.AuthTokenProviderBody.builder().id(id).build())
                .build();
    }

    @Setter
    @Getter
    public static class NewAuthTokenProviderBody {}

    private record TestkitAuthTokenProvider(String authProviderId, TestkitState testkitState)
            implements Supplier<CompletionStage<AuthTokenManagers.TemporalAuthData>> {
        @Override
        public CompletionStage<AuthTokenManagers.TemporalAuthData> get() {
            var callbackId = testkitState.newId();

            var callback = AuthTokenProviderRequest.builder()
                    .data(AuthTokenProviderRequest.AuthTokenProviderRequestBody.builder()
                            .id(callbackId)
                            .authTokenProviderId(authProviderId)
                            .build())
                    .build();

            var callbackStage = dispatchTestkitCallback(testkitState, callback);
            AuthTokenProviderCompleted resolutionCompleted;
            try {
                resolutionCompleted = (AuthTokenProviderCompleted)
                        callbackStage.toCompletableFuture().get();
            } catch (Exception e) {
                throw new RuntimeException("Unexpected failure during Testkit callback", e);
            }

            var authToken = AuthTokenUtil.parseAuthToken(
                    resolutionCompleted.getData().getAuth().getData().getToken());
            var expiresInMs = resolutionCompleted.getData().getAuth().getData().getExpiresInMs();
            var expirationTimestamp =
                    expiresInMs != null ? TestkitClock.INSTANCE.millis() + expiresInMs : Long.MAX_VALUE;
            return CompletableFuture.completedFuture(
                    AuthTokenManagers.TemporalAuthData.of(authToken, expirationTimestamp));
        }

        private CompletionStage<TestkitCallbackResult> dispatchTestkitCallback(
                TestkitState testkitState, TestkitCallback response) {
            CompletableFuture<TestkitCallbackResult> future = new CompletableFuture<>();
            testkitState.getCallbackIdToFuture().put(response.getCallbackId(), future);
            testkitState.getResponseWriter().accept(response);
            return future;
        }
    }
}
