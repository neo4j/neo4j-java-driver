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
package org.neo4j.driver.stress;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.reactivestreams.ReactiveSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RxWriteQuery<C extends AbstractContext> extends AbstractRxQuery<C> {
    private final AbstractStressTestBase<C> stressTest;

    RxWriteQuery(AbstractStressTestBase<C> stressTest, Driver driver, boolean useBookmark) {
        super(driver, useBookmark);
        this.stressTest = stressTest;
    }

    @Override
    public CompletionStage<Void> execute(C context) {
        var queryFinished = new CompletableFuture<Void>();
        Flux.usingWhen(
                        Mono.fromSupplier(() -> newSession(AccessMode.WRITE, context)),
                        session -> Mono.fromDirect(session.run("CREATE ()"))
                                .flatMap(reactiveResult -> Mono.fromDirect(reactiveResult.consume()))
                                .doOnSuccess(ignored -> context.setBookmark(session.lastBookmarks())),
                        ReactiveSession::close)
                .subscribe(
                        summary -> {
                            assertEquals(1, summary.counters().nodesCreated());
                            context.nodeCreated();
                            queryFinished.complete(null);
                        },
                        error -> handleError(Futures.completionExceptionCause(error), context, queryFinished));

        return queryFinished;
    }

    private void handleError(Throwable error, C context, CompletableFuture<Void> queryFinished) {
        if (!stressTest.handleWriteFailure(error, context)) {
            queryFinished.completeExceptionally(error);
        } else {
            queryFinished.complete(null);
        }
    }
}
