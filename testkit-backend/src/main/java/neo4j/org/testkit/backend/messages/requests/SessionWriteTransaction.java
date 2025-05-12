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
import static reactor.adapter.JdkFlowAdapter.publisherToFlowPublisher;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import lombok.Getter;
import lombok.Setter;
import neo4j.org.testkit.backend.AsyncTransactionContextAdapter;
import neo4j.org.testkit.backend.ReactiveTransactionContextAdapter;
import neo4j.org.testkit.backend.ReactiveTransactionContextStreamsAdapter;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.TransactionContextAdapter;
import neo4j.org.testkit.backend.holder.AsyncTransactionHolder;
import neo4j.org.testkit.backend.holder.ReactiveTransactionHolder;
import neo4j.org.testkit.backend.holder.ReactiveTransactionStreamsHolder;
import neo4j.org.testkit.backend.holder.SessionHolder;
import neo4j.org.testkit.backend.holder.TransactionHolder;
import neo4j.org.testkit.backend.messages.responses.RetryableDone;
import neo4j.org.testkit.backend.messages.responses.RetryableTry;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import org.neo4j.driver.TransactionCallback;
import org.neo4j.driver.async.AsyncTransactionCallback;
import org.neo4j.driver.reactive.ReactiveTransactionCallback;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

public class SessionWriteTransaction
        extends AbstractTestkitRequestWithTransactionConfig<SessionWriteTransaction.SessionWriteTransactionBody> {
    @Override
    public TestkitResponse process(TestkitState testkitState) {
        var sessionHolder = testkitState.getSessionHolder(data.getSessionId());
        var session = sessionHolder.getSession();
        session.executeWrite(handle(testkitState, sessionHolder), buildTxConfig());
        return retryableDone();
    }

    @Override
    @SuppressWarnings("DuplicatedCode")
    public CompletionStage<TestkitResponse> processAsync(TestkitState testkitState) {
        return testkitState
                .getAsyncSessionHolder(data.getSessionId())
                .thenCompose(sessionHolder -> {
                    var session = sessionHolder.getSession();

                    AsyncTransactionCallback<CompletionStage<Void>> workWrapper = tx -> {
                        var txId = testkitState.addAsyncTransactionHolder(
                                new AsyncTransactionHolder(sessionHolder, new AsyncTransactionContextAdapter(tx)));
                        testkitState.getResponseWriter().accept(retryableTry(txId));
                        var tryResult = new CompletableFuture<Void>();
                        sessionHolder.setTxWorkFuture(tryResult);
                        return tryResult;
                    };

                    return session.executeWriteAsync(workWrapper, buildTxConfig());
                })
                .thenApply(nothing -> retryableDone());
    }

    @Override
    @SuppressWarnings("DuplicatedCode")
    public Mono<TestkitResponse> processReactive(TestkitState testkitState) {
        return testkitState
                .getReactiveSessionHolder(data.getSessionId())
                .flatMap(sessionHolder -> {
                    ReactiveTransactionCallback<java.util.concurrent.Flow.Publisher<Void>> workWrapper = tx -> {
                        var txId = testkitState.addReactiveTransactionHolder(new ReactiveTransactionHolder(
                                sessionHolder, new ReactiveTransactionContextAdapter(tx)));
                        testkitState.getResponseWriter().accept(retryableTry(txId));
                        var tryResult = new CompletableFuture<Void>();
                        sessionHolder.setTxWorkFuture(tryResult);
                        return publisherToFlowPublisher(Mono.fromCompletionStage(tryResult));
                    };

                    return Mono.fromDirect(
                            flowPublisherToFlux(sessionHolder.getSession().executeWrite(workWrapper, buildTxConfig())));
                })
                .then(Mono.just(retryableDone()));
    }

    @Override
    @SuppressWarnings("DuplicatedCode")
    public Mono<TestkitResponse> processReactiveStreams(TestkitState testkitState) {
        return testkitState
                .getReactiveSessionStreamsHolder(data.getSessionId())
                .flatMap(sessionHolder -> {
                    org.neo4j.driver.reactivestreams.ReactiveTransactionCallback<Publisher<Void>> workWrapper = tx -> {
                        var txId =
                                testkitState.addReactiveTransactionStreamsHolder(new ReactiveTransactionStreamsHolder(
                                        sessionHolder, new ReactiveTransactionContextStreamsAdapter(tx)));
                        testkitState.getResponseWriter().accept(retryableTry(txId));
                        var tryResult = new CompletableFuture<Void>();
                        sessionHolder.setTxWorkFuture(tryResult);
                        return Mono.fromCompletionStage(tryResult);
                    };

                    return Mono.fromDirect(sessionHolder.getSession().executeWrite(workWrapper, buildTxConfig()));
                })
                .then(Mono.just(retryableDone()));
    }

    @SuppressWarnings("DuplicatedCode")
    private TransactionCallback<Void> handle(TestkitState testkitState, SessionHolder sessionHolder) {
        return tx -> {
            var txId = testkitState.addTransactionHolder(
                    new TransactionHolder(sessionHolder, new TransactionContextAdapter(tx)));
            testkitState.getResponseWriter().accept(retryableTry(txId));
            var txWorkFuture = new CompletableFuture<Void>();
            sessionHolder.setTxWorkFuture(txWorkFuture);

            try {
                return txWorkFuture.get();
            } catch (Throwable throwable) {
                var workThrowable = throwable;
                if (workThrowable instanceof ExecutionException) {
                    workThrowable = workThrowable.getCause();
                }
                if (workThrowable instanceof RuntimeException) {
                    throw (RuntimeException) workThrowable;
                }
                throw new RuntimeException("Unexpected exception occurred in transaction work function", workThrowable);
            }
        };
    }

    private RetryableTry retryableTry(String txId) {
        return RetryableTry.builder()
                .data(RetryableTry.RetryableTryBody.builder().id(txId).build())
                .build();
    }

    private RetryableDone retryableDone() {
        return RetryableDone.builder().build();
    }

    @Setter
    @Getter
    public static class SessionWriteTransactionBody
            extends AbstractTestkitRequestWithTransactionConfig.TransactionConfigBody {
        private String sessionId;
    }
}
