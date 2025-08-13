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
package org.neo4j.driver.internal.observation.util;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.neo4j.driver.internal.observation.DriverObservationProvider;
import org.neo4j.driver.internal.observation.Observation;
import org.neo4j.driver.internal.util.Futures;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public final class ObservationUtil {
    public static void observe(Observation observation, Runnable runnable) {
        observe(observation, () -> {
            runnable.run();
            return null;
        });
    }

    public static <T> T observe(Observation observation, Supplier<T> supplier) {
        observation.start();
        try (var scope = observation.openScope()) {
            return supplier.get();
        } catch (Throwable e) {
            observation.error(e);
            throw e;
        } finally {
            observation.stop();
        }
    }

    public static <T> T scoped(
            DriverObservationProvider observationProvider, Observation observation, Supplier<T> supplier) {
        var scopedObservation = observationProvider.scopedObservation();
        if (scopedObservation == null || !scopedObservation.equals(observation)) {
            try (var scope = observation.openScope()) {
                return supplier.get();
            }
        } else {
            return supplier.get();
        }
    }

    public static <T> CompletionStage<T> observeAsync(Observation observation, Supplier<CompletionStage<T>> supplier) {
        observation.start();
        return observeAsyncStarted(observation, supplier);
    }

    public static <T> CompletionStage<T> observeAsyncStarted(
            Observation observation, Supplier<CompletionStage<T>> supplier) {
        CompletionStage<T> observed;
        try (var scope = observation.openScope()) {
            observed = supplier.get();
        } catch (Throwable e) {
            observation.error(e);
            observation.stop();
            throw e;
        }
        return observed.whenComplete((result, throwable) -> {
            if (throwable != null) {
                observation.error(Futures.completionExceptionCause(throwable));
            }
            observation.stop();
        });
    }

    public static <T> Publisher<T> observeStreamsWithoutStart(
            Observation observation, Publisher<T> publisher, boolean stopOnCancel) {
        if (publisher instanceof Mono<T> mono) {
            return mono.doOnError(observation::error)
                    .doOnCancel(() -> {
                        if (stopOnCancel) {
                            observation.stop();
                        }
                    })
                    .doOnNext(ignored -> observation.stop());
        } else {
            return Flux.from(publisher)
                    .doOnError(observation::error)
                    .doOnCancel(() -> {
                        if (stopOnCancel) {
                            observation.stop();
                        }
                    })
                    .doOnComplete(observation::stop);
        }
    }

    public static <T> Publisher<T> observeStreams(Observation observation, Publisher<T> publisher) {
        return observeStreams(observation, publisher, true, false);
    }

    public static <T> Publisher<T> observeStreams(
            Observation observation, Publisher<T> publisher, boolean startOnRequest, boolean stopOnCancel) {
        if (!startOnRequest) {
            observation.start();
        }
        var started = new AtomicBoolean();
        if (publisher instanceof Mono<T> mono) {
            return mono.doOnRequest(ignored -> {
                        if (startOnRequest && !started.get()) {
                            observation.start();
                            started.set(true);
                        }
                    })
                    .doOnError(throwable -> {
                        observation.error(throwable);
                        observation.stop();
                    })
                    .doOnCancel(() -> {
                        if (stopOnCancel) {
                            observation.stop();
                        }
                    })
                    .doOnSuccess(ignored -> observation.stop());
        } else {
            return Flux.from(publisher)
                    .doOnRequest(ignored -> {
                        if (startOnRequest && !started.get()) {
                            observation.start();
                            started.set(true);
                        }
                    })
                    .doOnError(observation::error)
                    .doOnCancel(() -> {
                        if (stopOnCancel) {
                            observation.stop();
                        }
                    })
                    .doOnComplete(observation::stop);
        }
    }
}
