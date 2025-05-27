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
package org.neo4j.driver.integration.reactive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.driver.internal.util.Neo4jFeature.BOLT_V4;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.driver.Config;
import org.neo4j.driver.ConnectionPoolMetrics;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.internal.util.EnabledOnNeo4jWith;
import org.neo4j.driver.reactivestreams.ReactiveResult;
import org.neo4j.driver.reactivestreams.ReactiveSession;
import org.neo4j.driver.testutil.DatabaseExtension;
import org.neo4j.driver.testutil.LoggingUtil;
import org.neo4j.driver.testutil.ParallelizableIT;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.NonNull;

@EnabledOnNeo4jWith(BOLT_V4)
@ParallelizableIT
public class ReactiveStreamsSessionIT {
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    @ParameterizedTest
    @MethodSource("managedTransactionsReturningReactiveResultPublisher")
    @SuppressWarnings("resource")
    void shouldErrorWhenReactiveResultIsReturned(Function<ReactiveSession, Publisher<ReactiveResult>> fn) {
        // GIVEN
        var session = neo4j.driver().session(ReactiveSession.class);

        // WHEN & THEN
        var error = assertThrows(
                ClientException.class, () -> Flux.from(fn.apply(session)).blockFirst());
        assertEquals(
                "org.neo4j.driver.reactivestreams.ReactiveResult is not a valid return value, it should be consumed before producing a return value",
                error.getMessage());
        Flux.from(session.close()).blockFirst();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @SuppressWarnings("BusyWait")
    void shouldReleaseResultsOnSubscriptionCancellation(boolean request) throws InterruptedException {
        var messages = Collections.synchronizedList(new ArrayList<String>());
        @SuppressWarnings("deprecation")
        var config = Config.builder()
                .withDriverMetrics()
                .withLogging(LoggingUtil.boltLogging(messages))
                .build();
        try (var driver = neo4j.customDriver(config)) {
            // verify the database is available as runs may not report errors due to the subscription cancellation
            driver.verifyConnectivity();
            var tasksNumber = 100;
            var subscriptionFutures = IntStream.range(0, tasksNumber)
                    .mapToObj(ignored -> CompletableFuture.supplyAsync(() -> {
                        var subscriptionFuture = new CompletableFuture<Subscription>();
                        driver.session(ReactiveSession.class)
                                .run("UNWIND range (0,10000) AS x RETURN x")
                                .subscribe(new BaseSubscriber<>() {
                                    @Override
                                    protected void hookOnSubscribe(@NonNull Subscription subscription) {
                                        // use subscription from another thread to avoid immediate cancellation
                                        // within the subscribe method
                                        subscriptionFuture.complete(subscription);
                                    }

                                    @Override
                                    protected void hookOnNext(@NonNull ReactiveResult result) {
                                        Mono.fromDirect(result.consume()).subscribe();
                                    }
                                });
                        return subscriptionFuture.thenApplyAsync(subscription -> {
                            if (request) {
                                subscription.request(1);
                            }
                            subscription.cancel();
                            return subscription;
                        });
                    }))
                    .map(future -> future.thenCompose(Function.identity()))
                    .toArray(CompletableFuture[]::new);

            CompletableFuture.allOf(subscriptionFutures).join();

            // Subscription cancellation does not guarantee neither onComplete nor onError signal.
            var timeout = Instant.now().plus(5, ChronoUnit.MINUTES);
            var totalInUseConnections = -1;
            while (Instant.now().isBefore(timeout)) {
                totalInUseConnections = driver.metrics().connectionPoolMetrics().stream()
                        .map(ConnectionPoolMetrics::inUse)
                        .mapToInt(Integer::intValue)
                        .sum();
                if (totalInUseConnections == 0) {
                    return;
                }
                Thread.sleep(100);
            }
            fail(String.format(
                    "not all connections have been released\n%d are still in use\nlatest metrics: %s\nmessage log: \n%s",
                    totalInUseConnections, driver.metrics().connectionPoolMetrics(), String.join("\n", messages)));
        }
    }

    @Test
    void shouldRollbackResultOnSubscriptionCancellation() {
        var config = Config.builder().withMaxConnectionPoolSize(1).build();
        try (var driver = neo4j.customDriver(config)) {
            var session = driver.session(ReactiveSession.class);
            var nodeId = UUID.randomUUID().toString();
            var cancellationFuture = new CompletableFuture<Void>();

            session.run("CREATE ({id: $id})", Map.of("id", nodeId)).subscribe(new BaseSubscriber<>() {
                @Override
                protected void hookOnSubscribe(@NonNull Subscription subscription) {
                    subscription.cancel();
                    cancellationFuture.complete(null);
                }
            });

            cancellationFuture.join();

            var nodesNum = Mono.fromDirect(session.run("MATCH (n {id: $id}) RETURN n", Map.of("id", nodeId)))
                    .flatMapMany(ReactiveResult::records)
                    .count()
                    .block();
            assertEquals(0, nodesNum);
        }
    }

    @Test
    void shouldEmitAllSuccessfullyEmittedValues() {
        @SuppressWarnings("resource")
        var session = neo4j.driver().session(ReactiveSession.class);
        var succeed = new AtomicBoolean();
        var numbers = Flux.from(session.executeRead(tx -> {
                    var numbersFlux = Mono.from(tx.run("UNWIND range(0, 5) AS x RETURN x"))
                            .flatMapMany(ReactiveResult::records)
                            .map(record -> record.get("x").asInt());
                    return succeed.getAndSet(true)
                            ? numbersFlux
                            : numbersFlux.handle((value, sink) -> {
                                if (value == 2) {
                                    sink.error(new ServiceUnavailableException("simulated"));
                                } else {
                                    sink.next(value);
                                }
                            });
                }))
                .collectList()
                .block();
        assertEquals(List.of(0, 1, 0, 1, 2, 3, 4, 5), numbers);
    }

    static List<Function<ReactiveSession, Publisher<ReactiveResult>>>
            managedTransactionsReturningReactiveResultPublisher() {
        return List.of(
                session -> session.executeWrite(tx -> tx.run("RETURN 1")),
                session -> session.executeRead(tx -> tx.run("RETURN 1")));
    }
}
