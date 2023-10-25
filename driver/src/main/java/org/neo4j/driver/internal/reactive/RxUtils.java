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

import static java.util.Objects.requireNonNull;

import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.neo4j.driver.internal.util.Futures;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

public class RxUtils {
    /**
     * The publisher created by this method will either succeed without publishing anything or fail with an error.
     *
     * @param supplier supplies a {@link CompletionStage<Void>}.
     * @return A publisher that publishes nothing on completion or fails with an error.
     */
    public static <T> Publisher<T> createEmptyPublisher(Supplier<CompletionStage<Void>> supplier) {
        return Mono.create(sink -> supplier.get().whenComplete((ignore, completionError) -> {
            var error = Futures.completionExceptionCause(completionError);
            if (error != null) {
                sink.error(error);
            } else {
                sink.success();
            }
        }));
    }

    /**
     * The publisher created by this method will either succeed with exactly one item or fail with an error.
     *
     * @param supplier                    supplies a {@link CompletionStage<T>} that MUST produce a non-null result when completed successfully.
     * @param nullResultThrowableSupplier supplies a {@link Throwable} that is used as an error when the supplied completion stage completes successfully with
     *                                    null.
     * @param cancellationHandler handles cancellation, may be used to release associated resources
     * @param <T>                         the type of the item to publish.
     * @return A publisher that succeeds exactly one item or fails with an error.
     */
    public static <T> Mono<T> createSingleItemPublisher(
            Supplier<CompletionStage<T>> supplier,
            Supplier<Throwable> nullResultThrowableSupplier,
            Consumer<T> cancellationHandler) {
        requireNonNull(supplier, "supplier must not be null");
        requireNonNull(nullResultThrowableSupplier, "nullResultThrowableSupplier must not be null");
        requireNonNull(cancellationHandler, "cancellationHandler must not be null");
        return Mono.create(sink -> {
            var state = new SinkState<T>();
            sink.onRequest(ignored -> {
                CompletionStage<T> stage;
                synchronized (state) {
                    if (state.isCancelled()) {
                        return;
                    }
                    if (state.getStage() != null) {
                        return;
                    }
                    stage = supplier.get();
                    state.setStage(stage);
                }
                stage.whenComplete((item, completionError) -> {
                    if (completionError == null) {
                        if (item != null) {
                            sink.success(item);
                        } else {
                            sink.error(nullResultThrowableSupplier.get());
                        }
                    } else {
                        var error = Optional.ofNullable(Futures.completionExceptionCause(completionError))
                                .orElse(completionError);
                        sink.error(error);
                    }
                });
            });
            sink.onCancel(() -> {
                CompletionStage<T> stage;
                synchronized (state) {
                    if (state.isCancelled()) {
                        return;
                    }
                    state.setCancelled(true);
                    stage = state.getStage();
                }
                if (stage != null) {
                    stage.whenComplete((value, ignored) -> cancellationHandler.accept(value));
                }
            });
        });
    }

    private static class SinkState<T> {
        private CompletionStage<T> stage;
        private boolean cancelled;

        public CompletionStage<T> getStage() {
            return stage;
        }

        public void setStage(CompletionStage<T> stage) {
            this.stage = stage;
        }

        public boolean isCancelled() {
            return cancelled;
        }

        public void setCancelled(boolean cancelled) {
            this.cancelled = cancelled;
        }
    }
}
