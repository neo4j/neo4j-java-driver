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
import neo4j.org.testkit.backend.holder.AsyncTransactionHolder;
import neo4j.org.testkit.backend.holder.ReactiveTransactionHolder;
import neo4j.org.testkit.backend.holder.ReactiveTransactionStreamsHolder;
import neo4j.org.testkit.backend.holder.TransactionHolder;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import neo4j.org.testkit.backend.messages.responses.Transaction;
import reactor.core.publisher.Mono;

public class SessionBeginTransaction
        extends AbstractTestkitRequestWithTransactionConfig<SessionBeginTransaction.SessionBeginTransactionBody> {
    @Override
    public TestkitResponse process(TestkitState testkitState) {
        var sessionHolder = testkitState.getSessionHolder(data.getSessionId());
        var session = sessionHolder.getSession();

        var transaction = session.beginTransaction(buildTxConfig());
        return transaction(testkitState.addTransactionHolder(new TransactionHolder(sessionHolder, transaction)));
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync(TestkitState testkitState) {
        return testkitState.getAsyncSessionHolder(data.getSessionId()).thenCompose(sessionHolder -> {
            var session = sessionHolder.getSession();

            return session.beginTransactionAsync(buildTxConfig())
                    .thenApply(tx -> transaction(
                            testkitState.addAsyncTransactionHolder(new AsyncTransactionHolder(sessionHolder, tx))));
        });
    }

    @Override
    public Mono<TestkitResponse> processReactive(TestkitState testkitState) {
        return testkitState.getReactiveSessionHolder(data.getSessionId()).flatMap(sessionHolder -> {
            var session = sessionHolder.getSession();

            return Mono.fromDirect(flowPublisherToFlux(session.beginTransaction(buildTxConfig())))
                    .map(tx -> transaction(testkitState.addReactiveTransactionHolder(
                            new ReactiveTransactionHolder(sessionHolder, tx))));
        });
    }

    @Override
    public Mono<TestkitResponse> processReactiveStreams(TestkitState testkitState) {
        return testkitState.getReactiveSessionStreamsHolder(data.getSessionId()).flatMap(sessionHolder -> {
            var session = sessionHolder.getSession();

            return Mono.fromDirect(session.beginTransaction(buildTxConfig()))
                    .map(tx -> transaction(testkitState.addReactiveTransactionStreamsHolder(
                            new ReactiveTransactionStreamsHolder(sessionHolder, tx))));
        });
    }

    private Transaction transaction(String txId) {
        return Transaction.builder()
                .data(Transaction.TransactionBody.builder().id(txId).build())
                .build();
    }

    @Getter
    @Setter
    public static class SessionBeginTransactionBody
            extends AbstractTestkitRequestWithTransactionConfig.TransactionConfigBody {
        private String sessionId;
    }
}
