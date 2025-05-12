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
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.responses.Driver;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import reactor.core.publisher.Mono;

@Setter
@Getter
public class VerifyConnectivity implements TestkitRequest {
    private VerifyConnectivityBody data;

    @Override
    @SuppressWarnings("resource")
    public TestkitResponse process(TestkitState testkitState) {
        var id = data.getDriverId();
        testkitState.getDriverHolder(id).driver().verifyConnectivity();
        return createResponse(id);
    }

    @Override
    @SuppressWarnings("resource")
    public CompletionStage<TestkitResponse> processAsync(TestkitState testkitState) {
        var id = data.getDriverId();
        return testkitState
                .getDriverHolder(id)
                .driver()
                .verifyConnectivityAsync()
                .thenApply(ignored -> createResponse(id));
    }

    @Override
    public Mono<TestkitResponse> processReactive(TestkitState testkitState) {
        return Mono.fromCompletionStage(processAsync(testkitState));
    }

    @Override
    public Mono<TestkitResponse> processReactiveStreams(TestkitState testkitState) {
        return processReactive(testkitState);
    }

    private Driver createResponse(String id) {
        return Driver.builder().data(Driver.DriverBody.builder().id(id).build()).build();
    }

    @Setter
    @Getter
    public static class VerifyConnectivityBody {
        private String driverId;
    }
}
