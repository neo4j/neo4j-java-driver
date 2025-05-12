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

import java.util.concurrent.CompletionStage;
import lombok.Getter;
import lombok.Setter;
import neo4j.org.testkit.backend.AuthTokenUtil;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.responses.DriverIsAuthenticated;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import reactor.core.publisher.Mono;

@Setter
@Getter
public class VerifyAuthentication implements TestkitRequest {
    private VerifyAuthenticationBody data;

    @Override
    public TestkitResponse process(TestkitState testkitState) {
        var driverHolder = testkitState.getDriverHolder(data.getDriverId());
        @SuppressWarnings("resource")
        var driver = driverHolder.driver();
        var authToken = AuthTokenUtil.parseAuthToken(data.getAuthorizationToken());
        var authenticated = driver.verifyAuthentication(authToken);
        return DriverIsAuthenticated.builder()
                .data(DriverIsAuthenticated.DriverIsAuthenticatedBody.builder()
                        .id(testkitState.newId())
                        .authenticated(authenticated)
                        .build())
                .build();
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync(TestkitState testkitState) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Mono<TestkitResponse> processReactive(TestkitState testkitState) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Mono<TestkitResponse> processReactiveStreams(TestkitState testkitState) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Setter
    @Getter
    public static class VerifyAuthenticationBody {
        private String driverId;
        private AuthorizationToken authorizationToken;
    }
}
