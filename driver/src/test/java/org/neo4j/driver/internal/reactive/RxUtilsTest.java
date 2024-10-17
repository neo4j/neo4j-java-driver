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

import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.neo4j.driver.internal.reactive.RxUtils.createEmptyPublisher;
import static org.neo4j.driver.internal.reactive.RxUtils.createSingleItemPublisher;

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
import reactor.util.annotation.NonNull;

class RxUtilsTest {
    @Test
    void emptyPublisherShouldComplete() {
        Publisher<Void> emptyPublisher = createEmptyPublisher(Futures::completedWithNull);
        StepVerifier.create(emptyPublisher).verifyComplete();
    }

    @Test
    void emptyPublisherShouldErrorWhenSupplierErrors() {
        var error = new RuntimeException("Error");
        Publisher<Void> emptyPublisher = createEmptyPublisher(() -> failedFuture(error));

        StepVerifier.create(emptyPublisher).verifyErrorMatches(Predicate.isEqual(error));
    }

    @Test
    void singleItemPublisherShouldCompleteWithValue() {
        var publisher = createSingleItemPublisher(
                () -> CompletableFuture.completedFuture("One"), () -> mock(Throwable.class), (ignored) -> {});
        StepVerifier.create(publisher).expectNext("One").verifyComplete();
    }

    @Test
    void singleItemPublisherShouldErrorWhenFutureCompletesWithNull() {
        var error = new RuntimeException();
        Publisher<String> publisher =
                createSingleItemPublisher(Futures::completedWithNull, () -> error, (ignored) -> {});

        StepVerifier.create(publisher).verifyErrorMatches(actualError -> error == actualError);
    }

    @Test
    void singleItemPublisherShouldErrorWhenSupplierErrors() {
        var error = new RuntimeException();
        Publisher<String> publisher =
                createSingleItemPublisher(() -> failedFuture(error), () -> mock(Throwable.class), (ignored) -> {});

        StepVerifier.create(publisher).verifyErrorMatches(actualError -> error == actualError);
    }

    @Test
    void singleItemPublisherShouldHandleCancellationAfterRequestProcessingBegins() {
        // GIVEN
        var value = "value";
        var valueFuture = new CompletableFuture<String>();
        var supplierInvokedFuture = new CompletableFuture<Void>();
        Supplier<CompletionStage<String>> valueFutureSupplier = () -> {
            supplierInvokedFuture.complete(null);
            return valueFuture;
        };
        @SuppressWarnings("unchecked")
        Consumer<String> cancellationHandler = mock(Consumer.class);
        var publisher =
                createSingleItemPublisher(valueFutureSupplier, () -> mock(Throwable.class), cancellationHandler);

        // WHEN
        publisher.subscribe(new BaseSubscriber<>() {
            @Override
            protected void hookOnSubscribe(@NonNull Subscription subscription) {
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
