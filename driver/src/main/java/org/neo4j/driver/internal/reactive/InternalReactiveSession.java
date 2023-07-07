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
package org.neo4j.driver.internal.reactive;

import static reactor.adapter.JdkFlowAdapter.flowPublisherToFlux;
import static reactor.adapter.JdkFlowAdapter.publisherToFlowPublisher;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow.Publisher;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Query;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.internal.async.NetworkSession;
import org.neo4j.driver.internal.async.UnmanagedTransaction;
import org.neo4j.driver.internal.cursor.RxResultCursor;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.reactive.ReactiveResult;
import org.neo4j.driver.reactive.ReactiveSession;
import org.neo4j.driver.reactive.ReactiveTransaction;
import org.neo4j.driver.reactive.ReactiveTransactionCallback;
import reactor.core.publisher.Mono;

public class InternalReactiveSession extends AbstractReactiveSession<ReactiveTransaction>
        implements ReactiveSession, BaseReactiveQueryRunner {
    public InternalReactiveSession(NetworkSession session) {
        super(session);
    }

    @Override
    protected ReactiveTransaction createTransaction(UnmanagedTransaction unmanagedTransaction) {
        return new InternalReactiveTransaction(unmanagedTransaction);
    }

    @Override
    protected org.reactivestreams.Publisher<Void> closeTransaction(ReactiveTransaction transaction, boolean commit) {
        return ((InternalReactiveTransaction) transaction).close(commit);
    }

    @Override
    public Publisher<ReactiveTransaction> beginTransaction(TransactionConfig config) {
        return beginTransaction(config, null);
    }

    public Publisher<ReactiveTransaction> beginTransaction(TransactionConfig config, String txType) {
        return publisherToFlowPublisher(doBeginTransaction(config, txType));
    }

    @Override
    public <T> Publisher<T> executeRead(
            ReactiveTransactionCallback<? extends Publisher<T>> callback, TransactionConfig config) {
        return publisherToFlowPublisher(runTransaction(
                AccessMode.READ,
                tx -> flowPublisherToFlux(callback.execute(new DelegatingReactiveTransactionContext(tx))),
                config));
    }

    @Override
    public <T> Publisher<T> executeWrite(
            ReactiveTransactionCallback<? extends Publisher<T>> callback, TransactionConfig config) {
        return publisherToFlowPublisher(runTransaction(
                AccessMode.WRITE,
                tx -> flowPublisherToFlux(callback.execute(new DelegatingReactiveTransactionContext(tx))),
                config));
    }

    @Override
    public Publisher<ReactiveResult> run(Query query) {
        return run(query, TransactionConfig.empty());
    }

    @Override
    public Publisher<ReactiveResult> run(Query query, TransactionConfig config) {
        CompletionStage<RxResultCursor> cursorStage;
        try {
            cursorStage = session.runRx(query, config);
        } catch (Throwable t) {
            cursorStage = Futures.failedFuture(t);
        }

        return publisherToFlowPublisher(Mono.fromCompletionStage(cursorStage)
                .onErrorResume(error -> Mono.fromCompletionStage(session.releaseConnectionAsync())
                        .onErrorMap(releaseError -> Futures.combineErrors(error, releaseError))
                        .then(Mono.error(error)))
                .flatMap(cursor -> {
                    Mono<RxResultCursor> publisher;
                    var runError = cursor.getRunError();
                    if (runError != null) {
                        publisher = Mono.fromCompletionStage(session.releaseConnectionAsync())
                                .onErrorMap(releaseError -> Futures.combineErrors(runError, releaseError))
                                .then(Mono.error(runError));
                    } else {
                        publisher = Mono.just(cursor);
                    }
                    return publisher;
                })
                .map(InternalReactiveResult::new));
    }

    @Override
    public Set<Bookmark> lastBookmarks() {
        return new HashSet<>(session.lastBookmarks());
    }

    @Override
    public <T> Publisher<T> close() {
        return publisherToFlowPublisher(doClose());
    }
}
