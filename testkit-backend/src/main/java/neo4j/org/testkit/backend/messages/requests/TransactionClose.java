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
import neo4j.org.testkit.backend.holder.AbstractTransactionHolder;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import neo4j.org.testkit.backend.messages.responses.Transaction;
import org.neo4j.driver.async.AsyncTransaction;
import reactor.core.publisher.Mono;

@Setter
@Getter
public class TransactionClose implements TestkitRequest {
    private TransactionCloseBody data;

    @Override
    public TestkitResponse process(TestkitState testkitState) {
        testkitState.getTransactionHolder(data.getTxId()).getTransaction().close();
        return createResponse(data.getTxId());
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync(TestkitState testkitState) {
        return testkitState
                .getAsyncTransactionHolder(data.getTxId())
                .thenApply(AbstractTransactionHolder::getTransaction)
                .thenCompose(AsyncTransaction::closeAsync)
                .thenApply(ignored -> createResponse(data.getTxId()));
    }

    @Override
    public Mono<TestkitResponse> processReactive(TestkitState testkitState) {
        return testkitState
                .getReactiveTransactionHolder(data.getTxId())
                .map(AbstractTransactionHolder::getTransaction)
                .flatMap(tx -> Mono.fromDirect(flowPublisherToFlux(tx.close())))
                .then(Mono.just(createResponse(data.getTxId())));
    }

    @Override
    public Mono<TestkitResponse> processReactiveStreams(TestkitState testkitState) {
        return testkitState
                .getReactiveTransactionStreamsHolder(data.getTxId())
                .map(AbstractTransactionHolder::getTransaction)
                .flatMap(tx -> Mono.fromDirect(tx.close()))
                .then(Mono.just(createResponse(data.getTxId())));
    }

    private Transaction createResponse(String txId) {
        return Transaction.builder()
                .data(Transaction.TransactionBody.builder().id(txId).build())
                .build();
    }

    @Setter
    @Getter
    private static class TransactionCloseBody {
        private String txId;
    }
}
