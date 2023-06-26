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

import static reactor.adapter.JdkFlowAdapter.publisherToFlowPublisher;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow.Publisher;
import org.neo4j.driver.Query;
import org.neo4j.driver.internal.async.UnmanagedTransaction;
import org.neo4j.driver.internal.cursor.RxResultCursor;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.reactive.ReactiveResult;
import org.neo4j.driver.reactive.ReactiveTransaction;
import reactor.core.publisher.Mono;

public class InternalReactiveTransaction extends AbstractReactiveTransaction
        implements ReactiveTransaction, BaseReactiveQueryRunner {
    protected InternalReactiveTransaction(UnmanagedTransaction tx) {
        super(tx);
    }

    @Override
    public Publisher<ReactiveResult> run(Query query) {
        CompletionStage<RxResultCursor> cursorStage;
        try {
            cursorStage = tx.runRx(query);
        } catch (Throwable t) {
            cursorStage = Futures.failedFuture(t);
        }

        return publisherToFlowPublisher(Mono.fromCompletionStage(cursorStage)
                .flatMap(cursor -> {
                    Mono<RxResultCursor> publisher;
                    Throwable runError = cursor.getRunError();
                    if (runError != null) {
                        publisher = Mono.error(runError);
                        tx.markTerminated(runError);
                    } else {
                        publisher = Mono.just(cursor);
                    }
                    return publisher;
                })
                .map(InternalReactiveResult::new));
    }

    /**
     * <b>THIS IS A PRIVATE API</b>
     * <p>
     * Terminates the transaction by sending the Bolt {@code RESET} message and waiting for its response as long as the
     * transaction has not already been terminated, is not closed or closing.
     *
     * @return completion publisher (the {@code RESET} completion publisher if the message was sent)
     * @since 5.10
     */
    public Publisher<Void> terminate() {
        return publisherToFlowPublisher(Mono.fromCompletionStage(tx.terminateAsync()));
    }

    @Override
    public <T> Publisher<T> commit() {
        return publisherToFlowPublisher(doCommit());
    }

    @Override
    public <T> Publisher<T> rollback() {
        return publisherToFlowPublisher(doRollback());
    }

    @Override
    public Publisher<Void> close() {
        return publisherToFlowPublisher(doClose());
    }

    @Override
    public Publisher<Boolean> isOpen() {
        return publisherToFlowPublisher(doIsOpen());
    }
}
