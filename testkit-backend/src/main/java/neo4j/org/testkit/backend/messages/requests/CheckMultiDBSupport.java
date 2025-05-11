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
import neo4j.org.testkit.backend.messages.responses.MultiDBSupport;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import reactor.core.publisher.Mono;

@Setter
@Getter
public class CheckMultiDBSupport implements TestkitRequest {
    private CheckMultiDBSupportBody data;

    @Override
    public TestkitResponse process(TestkitState testkitState) {
        var driverId = data.getDriverId();
        @SuppressWarnings("resource")
        var available = testkitState.getDriverHolder(driverId).driver().supportsMultiDb();
        return createResponse(available);
    }

    @Override
    @SuppressWarnings("resource")
    public CompletionStage<TestkitResponse> processAsync(TestkitState testkitState) {
        return testkitState
                .getDriverHolder(data.getDriverId())
                .driver()
                .supportsMultiDbAsync()
                .thenApply(this::createResponse);
    }

    @Override
    public Mono<TestkitResponse> processReactive(TestkitState testkitState) {
        return Mono.fromCompletionStage(processAsync(testkitState));
    }

    @Override
    public Mono<TestkitResponse> processReactiveStreams(TestkitState testkitState) {
        return processReactive(testkitState);
    }

    private MultiDBSupport createResponse(boolean available) {
        return MultiDBSupport.builder()
                .data(MultiDBSupport.MultiDBSupportBody.builder()
                        .available(available)
                        .build())
                .build();
    }

    @Setter
    @Getter
    public static class CheckMultiDBSupportBody {
        private String driverId;
    }
}
