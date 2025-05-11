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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.reactivestreams.ReactiveResult;
import org.neo4j.driver.reactivestreams.ReactiveSession;
import org.neo4j.driver.summary.ResultSummary;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RxReadQueryWithRetries<C extends AbstractContext> extends AbstractRxQuery<C> {
    public RxReadQueryWithRetries(Driver driver, boolean useBookmark) {
        super(driver, useBookmark);
    }

    @Override
    public CompletionStage<Void> execute(C context) {
        var queryFinished = new CompletableFuture<Void>();
        Flux.usingWhen(
                        Mono.fromSupplier(() -> newSession(AccessMode.READ, context)),
                        this::processAndGetSummary,
                        ReactiveSession::close)
                .subscribe(
                        summary -> {
                            queryFinished.complete(null);
                            context.readCompleted();
                        },
                        error -> {
                            // ignores the error
                            queryFinished.complete(null);
                        });
        return queryFinished;
    }

    private Publisher<ResultSummary> processAndGetSummary(ReactiveSession session) {
        return session.executeRead(tx -> {
            var result = Mono.fromDirect(tx.run("MATCH (n) RETURN n LIMIT 1"));
            var records = result.flatMapMany(ReactiveResult::records)
                    .singleOrEmpty()
                    .map(record -> record.get(0).asNode());
            var summaryMono = result.flatMap(reactiveResult -> Mono.fromDirect(reactiveResult.consume()))
                    .single();
            return records.then(summaryMono);
        });
    }
}
