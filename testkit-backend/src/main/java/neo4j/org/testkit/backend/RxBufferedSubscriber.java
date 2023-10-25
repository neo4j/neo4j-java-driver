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
package neo4j.org.testkit.backend;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.util.annotation.NonNull;

/**
 * Buffered subscriber for testing purposes.
 * <p>
 * It consumes incoming signals as soon as they arrive and prevents publishing thread from getting blocked.
 * <p>
 * The consumed signals can be retrieved one-by-one using {@link #next()}. It calls upstream {@link org.reactivestreams.Subscription#request(long)} with
 * configured fetch size only when next signal is requested and no signals are expected to be emitted either because they have not been requested yet or the
 * previous demand has been satisfied.
 *
 * @param <T>
 */
public class RxBufferedSubscriber<T> extends BaseSubscriber<T> {
    private final Lock lock = new ReentrantLock();
    private final long fetchSize;
    private final CompletableFuture<Subscription> subscriptionFuture = new CompletableFuture<>();
    private final FluxSink<T> itemsSink;
    private final OneSignalSubscriber<T> itemsSubscriber;
    private long pendingItems;
    private boolean nextInProgress;

    public RxBufferedSubscriber(long fetchSize) {
        this.fetchSize = fetchSize;
        var sinkRef = new AtomicReference<FluxSink<T>>();
        itemsSubscriber = new OneSignalSubscriber<>();
        Flux.<T>create(fluxSink -> {
                    sinkRef.set(fluxSink);
                    fluxSink.onRequest(ignored -> requestFromUpstream());
                })
                .subscribe(itemsSubscriber);
        itemsSink = sinkRef.get();
    }

    /**
     * Returns a {@link Mono} of next signal from this subscription.
     * <p>
     * If necessary, a request with configured fetch size is made for more signals to be published.
     * <p>
     * <b>Only a single in progress request is supported at a time.</b> The returned {@link Mono} must succeed or error before next call is permitted.
     * <p>
     * Both empty successful completion and error completion indicate the completion of the subscribed publisher. This method must not be called after this.
     *
     * @return the {@link Mono} of next signal.
     */
    public Mono<T> next() {
        executeWithLock(lock, () -> {
            if (nextInProgress) {
                throw new IllegalStateException("Only one in progress next is allowed at a time");
            }
            return nextInProgress = true;
        });
        return Mono.fromCompletionStage(subscriptionFuture)
                .then(Mono.create(itemsSubscriber::requestNext))
                .doOnSuccess(ignored -> executeWithLock(lock, () -> nextInProgress = false))
                .doOnError(ignored -> executeWithLock(lock, () -> nextInProgress = false));
    }

    @Override
    protected void hookOnSubscribe(@NonNull Subscription subscription) {
        subscriptionFuture.complete(subscription);
    }

    @Override
    protected void hookOnNext(@NonNull T value) {
        executeWithLock(lock, () -> pendingItems--);
        itemsSink.next(value);
    }

    @Override
    protected void hookOnComplete() {
        itemsSink.complete();
    }

    @Override
    protected void hookOnError(@NonNull Throwable throwable) {
        itemsSink.error(throwable);
    }

    private void requestFromUpstream() {
        boolean moreItemsPending = executeWithLock(lock, () -> {
            boolean morePending;
            if (pendingItems > 0) {
                morePending = true;
            } else {
                pendingItems = fetchSize;
                morePending = false;
            }
            return morePending;
        });
        if (moreItemsPending) {
            return;
        }
        var subscription = subscriptionFuture.getNow(null);
        if (subscription == null) {
            throw new IllegalStateException("Upstream subscription must not be null at this stage");
        }
        subscription.request(fetchSize);
    }

    public static <T> T executeWithLock(Lock lock, Supplier<T> supplier) {
        lock.lock();
        try {
            return supplier.get();
        } finally {
            lock.unlock();
        }
    }

    private static class OneSignalSubscriber<T> extends BaseSubscriber<T> {
        private final Lock lock = new ReentrantLock();
        private final CompletableFuture<Void> completionFuture = new CompletableFuture<>();
        private MonoSink<T> sink;
        private boolean emitted;

        public void requestNext(MonoSink<T> sink) {
            executeWithLock(lock, () -> {
                this.sink = sink;
                return emitted = false;
            });

            if (completionFuture.isDone()) {
                completionFuture.whenComplete((ignored, throwable) -> {
                    if (throwable != null) {
                        this.sink.error(throwable);
                    } else {
                        this.sink.success();
                    }
                });
            } else {
                upstream().request(1);
            }
        }

        @Override
        protected void hookOnSubscribe(@NonNull Subscription subscription) {
            // left empty to prevent requesting signals immediately
        }

        @Override
        protected void hookOnNext(@NonNull T value) {
            var sink = executeWithLock(lock, () -> {
                emitted = true;
                return this.sink;
            });
            sink.success(value);
        }

        @Override
        protected void hookOnComplete() {
            var sink = executeWithLock(lock, () -> {
                completionFuture.complete(null);
                return !emitted ? this.sink : null;
            });
            if (sink != null) {
                sink.success();
            }
        }

        @Override
        protected void hookOnError(@NonNull Throwable throwable) {
            var sink = executeWithLock(lock, () -> {
                completionFuture.completeExceptionally(throwable);
                return !emitted ? this.sink : null;
            });
            if (sink != null) {
                sink.error(throwable);
            }
        }
    }
}
