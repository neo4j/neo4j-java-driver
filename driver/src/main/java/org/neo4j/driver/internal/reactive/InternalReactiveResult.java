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

import static org.neo4j.driver.internal.util.ErrorUtil.newResultConsumedError;
import static reactor.core.publisher.FluxSink.OverflowStrategy.IGNORE;

import java.util.List;
import java.util.concurrent.Flow;
import java.util.function.BiConsumer;
import org.neo4j.driver.Record;
import org.neo4j.driver.internal.cursor.RxResultCursor;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.reactive.ReactiveResult;
import org.neo4j.driver.summary.ResultSummary;
import reactor.adapter.JdkFlowAdapter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

public class InternalReactiveResult implements ReactiveResult {
    private final RxResultCursor cursor;

    public InternalReactiveResult(RxResultCursor cursor) {
        this.cursor = cursor;
    }

    @Override
    public List<String> keys() {
        return cursor.keys();
    }

    @Override
    public Flow.Publisher<Record> records() {
        return JdkFlowAdapter.publisherToFlowPublisher(Flux.create(
                sink -> {
                    if (cursor.isDone()) {
                        sink.error(newResultConsumedError());
                    } else {
                        cursor.installRecordConsumer(createRecordConsumer(sink));
                        sink.onCancel(cursor::cancel);
                        sink.onRequest(cursor::request);
                    }
                },
                IGNORE));
    }

    @Override
    public Flow.Publisher<ResultSummary> consume() {
        return JdkFlowAdapter.publisherToFlowPublisher(
                Mono.create(sink -> cursor.summaryAsync().whenComplete((summary, summaryCompletionError) -> {
                    Throwable error = Futures.completionExceptionCause(summaryCompletionError);
                    if (summary != null) {
                        sink.success(summary);
                    } else {
                        sink.error(error);
                    }
                })));
    }

    @Override
    public Flow.Publisher<Boolean> isOpen() {
        return JdkFlowAdapter.publisherToFlowPublisher(Mono.just(!cursor.isDone()));
    }

    /**
     * Defines how a subscriber shall consume records. A record consumer holds a reference to a subscriber. A publisher and/or a subscription who holds a
     * reference to this consumer shall release the reference to this object after subscription is done or cancelled so that the subscriber can be garbage
     * collected.
     *
     * @param sink the subscriber
     * @return a record consumer.
     */
    private BiConsumer<Record, Throwable> createRecordConsumer(FluxSink<Record> sink) {
        return (r, e) -> {
            if (r != null) {
                sink.next(r);
            } else if (e != null) {
                sink.error(e);
            } else {
                sink.complete();
            }
        };
    }
}
