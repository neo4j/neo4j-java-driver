/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
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
import java.util.Map;
import java.util.concurrent.CompletionStage;
import lombok.Getter;
import lombok.Setter;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.holder.AsyncTransactionHolder;
import neo4j.org.testkit.backend.holder.ReactiveTransactionHolder;
import neo4j.org.testkit.backend.holder.ReactiveTransactionStreamsHolder;
import neo4j.org.testkit.backend.holder.RxTransactionHolder;
import neo4j.org.testkit.backend.holder.SessionHolder;
import neo4j.org.testkit.backend.holder.TransactionHolder;
import neo4j.org.testkit.backend.messages.requests.deserializer.TestkitCypherParamDeserializer;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import neo4j.org.testkit.backend.messages.responses.Transaction;
import org.neo4j.driver.Session;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.reactive.ReactiveSession;
import org.neo4j.driver.reactive.RxSession;
import reactor.core.publisher.Mono;

@Setter
@Getter
public class SessionBeginTransaction extends WithTxConfig {
    private SessionBeginTransactionBody data;

    @Override
    public TestkitResponse process(TestkitState testkitState) {
        SessionHolder sessionHolder = testkitState.getSessionHolder(data.getSessionId());
        Session session = sessionHolder.getSession();

        org.neo4j.driver.Transaction transaction = session.beginTransaction(getTxConfig());
        return transaction(testkitState.addTransactionHolder(new TransactionHolder(sessionHolder, transaction)));
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync(TestkitState testkitState) {
        return testkitState.getAsyncSessionHolder(data.getSessionId()).thenCompose(sessionHolder -> {
            AsyncSession session = sessionHolder.getSession();
            TransactionConfig.Builder builder = TransactionConfig.builder();

            return session.beginTransactionAsync(getTxConfig())
                    .thenApply(tx -> transaction(
                            testkitState.addAsyncTransactionHolder(new AsyncTransactionHolder(sessionHolder, tx))));
        });
    }

    @Override
    @SuppressWarnings("deprecation")
    public Mono<TestkitResponse> processRx(TestkitState testkitState) {
        return testkitState.getRxSessionHolder(data.getSessionId()).flatMap(sessionHolder -> {
            RxSession session = sessionHolder.getSession();
            TransactionConfig.Builder builder = TransactionConfig.builder();

            return Mono.fromDirect(session.beginTransaction(getTxConfig()))
                    .map(tx -> transaction(
                            testkitState.addRxTransactionHolder(new RxTransactionHolder(sessionHolder, tx))));
        });
    }

    @Override
    public Mono<TestkitResponse> processReactive(TestkitState testkitState) {
        return testkitState.getReactiveSessionHolder(data.getSessionId()).flatMap(sessionHolder -> {
            ReactiveSession session = sessionHolder.getSession();
            TransactionConfig.Builder builder = TransactionConfig.builder();

            return Mono.fromDirect(flowPublisherToFlux(session.beginTransaction(getTxConfig())))
                    .map(tx -> transaction(testkitState.addReactiveTransactionHolder(
                            new ReactiveTransactionHolder(sessionHolder, tx))));
        });
    }

    @Override
    public Mono<TestkitResponse> processReactiveStreams(TestkitState testkitState) {
        return testkitState.getReactiveSessionStreamsHolder(data.getSessionId()).flatMap(sessionHolder -> {
            var session = sessionHolder.getSession();
            TransactionConfig.Builder builder = TransactionConfig.builder();

            return Mono.fromDirect(session.beginTransaction(getTxConfig()))
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
    public static class SessionBeginTransactionBody implements WithTxConfig.ITxConfigBody {
        private String sessionId;

        @JsonDeserialize(using = TestkitCypherParamDeserializer.class)
        private Map<String, Object> txMeta;

        private Integer timeout;
        private Boolean timeoutPresent = false;

        public void setTimeout(Integer timeout) {
            this.timeout = timeout;
            timeoutPresent = true;
        }
    }
}
