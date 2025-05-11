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
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
import reactor.core.publisher.Mono;

@Setter
@Getter
public class TransactionRun implements TestkitRequest {
    protected TransactionRunBody data;

    @Override
    public TestkitResponse process(TestkitState testkitState) {
        var transactionHolder = testkitState.getTransactionHolder(data.getTxId());
        var result = transactionHolder
                .getTransaction()
                .run(data.getCypher(), data.getParams() != null ? data.getParams() : Collections.emptyMap());
        var resultId = testkitState.addResultHolder(new ResultHolder(transactionHolder, result));
        return createResponse(resultId, result.keys());
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync(TestkitState testkitState) {
        return testkitState.getAsyncTransactionHolder(data.getTxId()).thenCompose(transactionHolder -> transactionHolder
                .getTransaction()
                .runAsync(data.getCypher(), data.getParams() != null ? data.getParams() : Collections.emptyMap())
                .thenApply(resultCursor -> {
                    var resultId =
                            testkitState.addAsyncResultHolder(new ResultCursorHolder(transactionHolder, resultCursor));
                    return createResponse(resultId, resultCursor.keys());
                }));
    }

    @Override
    public Mono<TestkitResponse> processReactive(TestkitState testkitState) {
        return testkitState.getReactiveTransactionHolder(data.getTxId()).flatMap(transactionHolder -> {
            var tx = transactionHolder.getTransaction();
            Map<String, Object> params = data.getParams() != null ? data.getParams() : Collections.emptyMap();

            return Mono.fromDirect(flowPublisherToFlux(tx.run(data.getCypher(), params)))
                    .map(result -> {
                        var id = testkitState.addReactiveResultHolder(
                                new ReactiveResultHolder(transactionHolder, result));
                        return createResponse(id, result.keys());
                    });
        });
    }

    @Override
    public Mono<TestkitResponse> processReactiveStreams(TestkitState testkitState) {
        return testkitState.getReactiveTransactionStreamsHolder(data.getTxId()).flatMap(transactionHolder -> {
            var tx = transactionHolder.getTransaction();
            Map<String, Object> params = data.getParams() != null ? data.getParams() : Collections.emptyMap();

            return Mono.fromDirect(tx.run(data.getCypher(), params)).map(result -> {
                var id = testkitState.addReactiveResultStreamsHolder(
                        new ReactiveResultStreamsHolder(transactionHolder, result));
                return createResponse(id, result.keys());
            });
        });
    }

    protected Result createResponse(String resultId, List<String> keys) {
        return Result.builder()
                .data(Result.ResultBody.builder().id(resultId).keys(keys).build())
                .build();
    }

    @Setter
    @Getter
    public static class TransactionRunBody {
        private String txId;
        private String cypher;

        @JsonDeserialize(using = TestkitCypherParamDeserializer.class)
        private Map<String, Object> params;
    }
}
