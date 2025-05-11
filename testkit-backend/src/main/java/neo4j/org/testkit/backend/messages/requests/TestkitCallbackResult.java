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
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.responses.TestkitCallback;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import reactor.core.publisher.Mono;

/**
 * This request is sent by Testkit in response to previously sent {@link TestkitCallback}.
 */
public interface TestkitCallbackResult extends TestkitRequest {
    String getCallbackId();

    @Override
    default TestkitResponse process(TestkitState testkitState) {
        testkitState.getCallbackIdToFuture().get(getCallbackId()).complete(this);
        return null;
    }

    @Override
    default CompletionStage<TestkitResponse> processAsync(TestkitState testkitState) {
        testkitState.getCallbackIdToFuture().get(getCallbackId()).complete(this);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    default Mono<TestkitResponse> processReactive(TestkitState testkitState) {
        testkitState.getCallbackIdToFuture().get(getCallbackId()).complete(this);
        return Mono.empty();
    }

    @Override
    default Mono<TestkitResponse> processReactiveStreams(TestkitState testkitState) {
        testkitState.getCallbackIdToFuture().get(getCallbackId()).complete(this);
        return Mono.empty();
    }
}
