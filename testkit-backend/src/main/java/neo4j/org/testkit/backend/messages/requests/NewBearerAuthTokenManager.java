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

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.Setter;
import neo4j.org.testkit.backend.AuthTokenUtil;
import neo4j.org.testkit.backend.TestkitClock;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.responses.BearerAuthTokenManager;
import neo4j.org.testkit.backend.messages.responses.BearerAuthTokenProviderRequest;
import neo4j.org.testkit.backend.messages.responses.TestkitCallback;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import org.neo4j.driver.AuthTokenAndExpiration;
import org.neo4j.driver.exceptions.AuthenticationException;
import org.neo4j.driver.exceptions.TokenExpiredException;
import org.neo4j.driver.internal.security.ExpirationBasedAuthTokenManager;

@Setter
@Getter
public class NewBearerAuthTokenManager extends AbstractBasicTestkitRequest {
    private NewBearerAuthTokenManagerBody data;

    @Override
    protected TestkitResponse processAndCreateResponse(TestkitState testkitState) {
        var id = testkitState.newId();
        testkitState.addAuthProvider(
                id,
                new ExpirationBasedAuthTokenManager(
                        new TestkitAuthTokenProvider(id, testkitState),
                        Set.of(TokenExpiredException.class, AuthenticationException.class),
                        TestkitClock.INSTANCE));
        return BearerAuthTokenManager.builder()
                .data(BearerAuthTokenManager.BearerAuthTokenManagerBody.builder()
                        .id(id)
                        .build())
                .build();
    }

    private record TestkitAuthTokenProvider(String authProviderId, TestkitState testkitState)
            implements Supplier<CompletionStage<AuthTokenAndExpiration>> {
        @Override
        public CompletionStage<AuthTokenAndExpiration> get() {
            var callbackId = testkitState.newId();

            var callback = BearerAuthTokenProviderRequest.builder()
                    .data(BearerAuthTokenProviderRequest.BearerAuthTokenProviderRequestBody.builder()
                            .id(callbackId)
                            .bearerAuthTokenManagerId(authProviderId)
                            .build())
                    .build();

            var callbackStage = dispatchTestkitCallback(testkitState, callback);
            BearerAuthTokenProviderCompleted resolutionCompleted;
            try {
                resolutionCompleted = (BearerAuthTokenProviderCompleted)
                        callbackStage.toCompletableFuture().get();
            } catch (Exception e) {
                throw new RuntimeException("Unexpected failure during Testkit callback", e);
            }

            var authToken = AuthTokenUtil.parseAuthToken(
                    resolutionCompleted.getData().getAuth().getData().getToken());
            var expiresInMs = resolutionCompleted.getData().getAuth().getData().getExpiresInMs();
            var expirationTimestamp =
                    expiresInMs != null ? TestkitClock.INSTANCE.millis() + expiresInMs : Long.MAX_VALUE;
            return CompletableFuture.completedFuture(authToken.expiringAt(expirationTimestamp));
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
    public static class NewBearerAuthTokenManagerBody {}
}
