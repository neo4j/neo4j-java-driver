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

import static reactor.adapter.JdkFlowAdapter.flowPublisherToFlux;

import java.util.concurrent.CompletionStage;
import lombok.Getter;
import lombok.Setter;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.responses.Session;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import reactor.core.publisher.Mono;

@Setter
@Getter
public class SessionClose implements TestkitRequest {
    private SessionCloseBody data;

    @Override
    public TestkitResponse process(TestkitState testkitState) {
        testkitState.getSessionHolder(data.getSessionId()).getSession().close();
        return createResponse();
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync(TestkitState testkitState) {
        return testkitState
                .getAsyncSessionHolder(data.getSessionId())
                .thenCompose(sessionHolder -> sessionHolder.getSession().closeAsync())
                .thenApply(ignored -> createResponse());
    }

    @Override
    public Mono<TestkitResponse> processReactive(TestkitState testkitState) {
        return testkitState
                .getReactiveSessionHolder(data.getSessionId())
                .flatMap(sessionHolder -> Mono.fromDirect(
                        flowPublisherToFlux(sessionHolder.getSession().close())))
                .then(Mono.just(createResponse()));
    }

    @Override
    public Mono<TestkitResponse> processReactiveStreams(TestkitState testkitState) {
        return testkitState
                .getReactiveSessionStreamsHolder(data.getSessionId())
                .flatMap(sessionHolder ->
                        Mono.fromDirect(sessionHolder.getSession().close()))
                .then(Mono.just(createResponse()));
    }

    private Session createResponse() {
        return Session.builder()
                .data(Session.SessionBody.builder().id(data.getSessionId()).build())
                .build();
    }

    @Setter
    @Getter
    private static class SessionCloseBody {
        private String sessionId;
    }
}
