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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import lombok.Getter;
import lombok.Setter;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.responses.DriverIsEncrypted;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import reactor.core.publisher.Mono;

@Setter
@Getter
public class CheckDriverIsEncrypted implements TestkitRequest {
    private CheckDriverIsEncryptedBody data;

    @Override
    public TestkitResponse process(TestkitState testkitState) {
        return createResponse(testkitState);
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync(TestkitState testkitState) {
        return CompletableFuture.completedFuture(createResponse(testkitState));
    }

    @Override
    public Mono<TestkitResponse> processReactive(TestkitState testkitState) {
        return Mono.just(createResponse(testkitState));
    }

    @Override
    public Mono<TestkitResponse> processReactiveStreams(TestkitState testkitState) {
        return processReactive(testkitState);
    }

    @SuppressWarnings("resource")
    private DriverIsEncrypted createResponse(TestkitState testkitState) {
        var driverHolder = testkitState.getDriverHolder(data.getDriverId());
        return DriverIsEncrypted.builder()
                .data(DriverIsEncrypted.DriverIsEncryptedBody.builder()
                        .encrypted(driverHolder.driver().isEncrypted())
                        .build())
                .build();
    }

    @Setter
    @Getter
    public static class CheckDriverIsEncryptedBody {
        private String driverId;
    }
}
