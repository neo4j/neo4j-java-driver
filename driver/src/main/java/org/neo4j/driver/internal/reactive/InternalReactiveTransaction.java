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
package org.neo4j.driver.internal.reactive;

import static org.neo4j.driver.internal.observation.util.ObservationUtil.observeStreams;
import static org.neo4j.driver.internal.observation.util.ObservationUtil.observeStreamsWithoutStart;
import static reactor.adapter.JdkFlowAdapter.publisherToFlowPublisher;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow.Publisher;
import org.neo4j.driver.Query;
import org.neo4j.driver.internal.async.UnmanagedTransaction;
import org.neo4j.driver.internal.cursor.RxResultCursor;
import org.neo4j.driver.internal.observation.DriverObservationProvider;
import org.neo4j.driver.reactive.ReactiveResult;
import org.neo4j.driver.reactive.ReactiveTransaction;
import reactor.core.publisher.Mono;

public class InternalReactiveTransaction extends AbstractReactiveTransaction
        implements ReactiveTransaction, BaseReactiveQueryRunner {
    private final DriverObservationProvider observationProvider;

    protected InternalReactiveTransaction(UnmanagedTransaction tx, DriverObservationProvider observationProvider) {
        super(tx);
        this.observationProvider = Objects.requireNonNull(observationProvider);
    }

    @Override
    @SuppressWarnings({"DuplicatedCode"})
    public Publisher<ReactiveResult> run(Query query) {
        var runObservation = observationProvider
                .transactionRun(ReactiveTransaction.class, query.text(), query.parameters())
                .start();
        CompletionStage<RxResultCursor> cursorStage;
        try {
            cursorStage = tx.runRx(query, runObservation);
        } catch (Throwable t) {
            cursorStage = CompletableFuture.failedFuture(t);
        }

        return publisherToFlowPublisher(observeStreamsWithoutStart(
                runObservation,
                Mono.fromCompletionStage(cursorStage)
                        .flatMap(cursor -> {
                            Mono<RxResultCursor> publisher;
                            var runError = cursor.getRunError();
                            if (runError != null) {
                                publisher = Mono.error(runError);
                                tx.markTerminated(runError);
                            } else {
                                publisher = Mono.just(cursor);
                            }
                            return publisher;
                        })
                        .map(result -> new InternalReactiveResult(result, observationProvider)),
                false));
    }

    @Override
    public <T> Publisher<T> commit() {
        var commitObservation = observationProvider.transactionCommit(ReactiveTransaction.class);
        return publisherToFlowPublisher(observeStreams(commitObservation, doCommit(commitObservation)));
    }

    @Override
    public <T> Publisher<T> rollback() {
        var rollbackObservation = observationProvider.transactionRollback(ReactiveTransaction.class);
        return publisherToFlowPublisher(observeStreams(rollbackObservation, doRollback(rollbackObservation)));
    }

    @Override
    public Publisher<Void> close() {
        var closeObservation = observationProvider.transactionClose(ReactiveTransaction.class);
        return publisherToFlowPublisher(observeStreams(closeObservation, doClose(closeObservation)));
    }

    @Override
    public Publisher<Boolean> isOpen() {
        return publisherToFlowPublisher(doIsOpen());
    }
}
