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
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import neo4j.org.testkit.backend.messages.responses.Transaction;
import reactor.core.publisher.Mono;

@Getter
@Setter
public class TransactionCommit implements TestkitRequest {
    private TransactionCommitBody data;

    @Override
    public TestkitResponse process(TestkitState testkitState) {
        testkitState.getTransactionHolder(data.getTxId()).getTransaction().commit();
        return createResponse(data.getTxId());
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync(TestkitState testkitState) {
        return testkitState
                .getAsyncTransactionHolder(data.getTxId())
                .thenCompose(tx -> tx.getTransaction().commitAsync())
                .thenApply(ignored -> createResponse(data.getTxId()));
    }

    @Override
    public Mono<TestkitResponse> processReactive(TestkitState testkitState) {
        return testkitState
                .getReactiveTransactionHolder(data.getTxId())
                .flatMap(tx ->
                        Mono.fromDirect(flowPublisherToFlux(tx.getTransaction().commit())))
                .then(Mono.just(createResponse(data.getTxId())));
    }

    @Override
    public Mono<TestkitResponse> processReactiveStreams(TestkitState testkitState) {
        return testkitState
                .getReactiveTransactionStreamsHolder(data.getTxId())
                .flatMap(tx -> Mono.fromDirect(tx.getTransaction().commit()))
                .then(Mono.just(createResponse(data.getTxId())));
    }

    private Transaction createResponse(String txId) {
        return Transaction.builder()
                .data(Transaction.TransactionBody.builder().id(txId).build())
                .build();
    }

    @Getter
    @Setter
    public static class TransactionCommitBody {
        private String txId;
    }
}
