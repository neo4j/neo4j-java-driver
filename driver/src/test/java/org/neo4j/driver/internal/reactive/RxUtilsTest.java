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

import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.neo4j.driver.internal.reactive.RxUtils.createEmptyPublisher;
import static org.neo4j.driver.internal.reactive.RxUtils.createSingleItemPublisher;
import static org.neo4j.driver.internal.util.Futures.failedFuture;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.internal.util.Futures;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.test.StepVerifier;

class RxUtilsTest {
    @Test
    void emptyPublisherShouldComplete() {
        Publisher<Void> emptyPublisher = createEmptyPublisher(Futures::completedWithNull);
        StepVerifier.create(emptyPublisher).verifyComplete();
    }

    @Test
    void emptyPublisherShouldErrorWhenSupplierErrors() {
        RuntimeException error = new RuntimeException("Error");
        Publisher<Void> emptyPublisher = createEmptyPublisher(() -> failedFuture(error));

        StepVerifier.create(emptyPublisher).verifyErrorMatches(Predicate.isEqual(error));
    }

    @Test
    void singleItemPublisherShouldCompleteWithValue() {
        Publisher<String> publisher = createSingleItemPublisher(
                () -> CompletableFuture.completedFuture("One"), () -> mock(Throwable.class), (ignored) -> {});
        StepVerifier.create(publisher).expectNext("One").verifyComplete();
    }

    @Test
    void singleItemPublisherShouldErrorWhenFutureCompletesWithNull() {
        Throwable error = mock(Throwable.class);
        Publisher<String> publisher =
                createSingleItemPublisher(Futures::completedWithNull, () -> error, (ignored) -> {});

        StepVerifier.create(publisher).verifyErrorMatches(actualError -> error == actualError);
    }

    @Test
    void singleItemPublisherShouldErrorWhenSupplierErrors() {
        RuntimeException error = mock(RuntimeException.class);
        Publisher<String> publisher =
                createSingleItemPublisher(() -> failedFuture(error), () -> mock(Throwable.class), (ignored) -> {});

        StepVerifier.create(publisher).verifyErrorMatches(actualError -> error == actualError);
    }

    @Test
    void singleItemPublisherShouldHandleCancellationAfterRequestProcessingBegins() {
        // GIVEN
        String value = "value";
        CompletableFuture<String> valueFuture = new CompletableFuture<>();
        CompletableFuture<Void> supplierInvokedFuture = new CompletableFuture<>();
        Supplier<CompletionStage<String>> valueFutureSupplier = () -> {
            supplierInvokedFuture.complete(null);
            return valueFuture;
        };
        @SuppressWarnings("unchecked")
        Consumer<String> cancellationHandler = mock(Consumer.class);
        Publisher<String> publisher =
                createSingleItemPublisher(valueFutureSupplier, () -> mock(Throwable.class), cancellationHandler);

        // WHEN
        publisher.subscribe(new BaseSubscriber<String>() {
            @Override
            protected void hookOnSubscribe(Subscription subscription) {
                subscription.request(1);
                supplierInvokedFuture.thenAccept(ignored -> {
                    subscription.cancel();
                    valueFuture.complete(value);
                });
            }
        });

        // THEN
        valueFuture.join();
        then(cancellationHandler).should().accept(value);
    }
}
