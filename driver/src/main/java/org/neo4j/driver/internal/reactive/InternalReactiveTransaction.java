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

import java.util.concurrent.CompletionStage;
import org.neo4j.driver.Query;
import org.neo4j.driver.internal.async.UnmanagedTransaction;
import org.neo4j.driver.internal.cursor.RxResultCursor;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.reactive.ReactiveResult;
import org.neo4j.driver.reactive.ReactiveTransaction;
import org.reactivestreams.Publisher;
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

        return Mono.fromCompletionStage(cursorStage)
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
                .map(InternalReactiveResult::new);
    }

    /**
     * Marks transaction as terminated and sends {@code RESET} message over allocated connection.
     * <p>
     * <b>THIS METHOD IS NOT PART OF PUBLIC API</b>
     *
     * @return {@code RESET} response publisher
     */
    public Publisher<Void> interrupt() {
        return Mono.fromCompletionStage(tx.interruptAsync());
    }
}
