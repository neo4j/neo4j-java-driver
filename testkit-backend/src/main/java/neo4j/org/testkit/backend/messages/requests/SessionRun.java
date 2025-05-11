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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import lombok.Getter;
import lombok.Setter;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.holder.ReactiveResultHolder;
import neo4j.org.testkit.backend.holder.ReactiveResultStreamsHolder;
import neo4j.org.testkit.backend.holder.ResultCursorHolder;
import neo4j.org.testkit.backend.holder.ResultHolder;
import neo4j.org.testkit.backend.messages.requests.deserializer.TestkitCypherParamDeserializer;
import neo4j.org.testkit.backend.messages.responses.Result;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import org.neo4j.driver.Query;
import reactor.core.publisher.Mono;

public class SessionRun extends AbstractTestkitRequestWithTransactionConfig<SessionRun.SessionRunBody> {
    @Override
    public TestkitResponse process(TestkitState testkitState) {
        var sessionHolder = testkitState.getSessionHolder(data.getSessionId());
        var session = sessionHolder.getSession();
        var query = Optional.ofNullable(data.params)
                .map(params -> new Query(data.cypher, data.params))
                .orElseGet(() -> new Query(data.cypher));
        var result = session.run(query, buildTxConfig());
        var id = testkitState.addResultHolder(new ResultHolder(sessionHolder, result));

        return createResponse(id, result.keys());
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync(TestkitState testkitState) {
        return testkitState.getAsyncSessionHolder(data.getSessionId()).thenCompose(sessionHolder -> {
            var session = sessionHolder.getSession();
            var query = Optional.ofNullable(data.params)
                    .map(params -> new Query(data.cypher, data.params))
                    .orElseGet(() -> new Query(data.cypher));

            return session.runAsync(query, buildTxConfig()).thenApply(resultCursor -> {
                var id = testkitState.addAsyncResultHolder(new ResultCursorHolder(sessionHolder, resultCursor));
                return createResponse(id, resultCursor.keys());
            });
        });
    }

    @Override
    public Mono<TestkitResponse> processReactive(TestkitState testkitState) {
        return testkitState.getReactiveSessionHolder(data.getSessionId()).flatMap(sessionHolder -> {
            var session = sessionHolder.getSession();
            var query = Optional.ofNullable(data.params)
                    .map(params -> new Query(data.cypher, data.params))
                    .orElseGet(() -> new Query(data.cypher));

            return Mono.fromDirect(flowPublisherToFlux(session.run(query, buildTxConfig())))
                    .map(result -> {
                        var id = testkitState.addReactiveResultHolder(new ReactiveResultHolder(sessionHolder, result));
                        return createResponse(id, result.keys());
                    });
        });
    }

    @Override
    public Mono<TestkitResponse> processReactiveStreams(TestkitState testkitState) {
        return testkitState.getReactiveSessionStreamsHolder(data.getSessionId()).flatMap(sessionHolder -> {
            var session = sessionHolder.getSession();
            var query = Optional.ofNullable(data.params)
                    .map(params -> new Query(data.cypher, data.params))
                    .orElseGet(() -> new Query(data.cypher));

            return Mono.fromDirect(session.run(query, buildTxConfig())).map(result -> {
                var id = testkitState.addReactiveResultStreamsHolder(
                        new ReactiveResultStreamsHolder(sessionHolder, result));
                return createResponse(id, result.keys());
            });
        });
    }

    private Result createResponse(String resultId, List<String> keys) {
        return Result.builder()
                .data(Result.ResultBody.builder().id(resultId).keys(keys).build())
                .build();
    }

    @Setter
    @Getter
    public static class SessionRunBody extends AbstractTestkitRequestWithTransactionConfig.TransactionConfigBody {
        @JsonDeserialize(using = TestkitCypherParamDeserializer.class)
        private Map<String, Object> params;

        private String sessionId;
        private String cypher;
    }
}
