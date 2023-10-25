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

import static java.util.Collections.emptyIterator;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.driver.internal.util.Neo4jFeature.BOLT_V4;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.DatabaseException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.driver.internal.util.EnabledOnNeo4jWith;
import org.neo4j.driver.reactive.RxResult;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.reactive.RxTransaction;
import org.neo4j.driver.reactive.RxTransactionWork;
import org.neo4j.driver.testutil.DatabaseExtension;
import org.neo4j.driver.testutil.ParallelizableIT;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@EnabledOnNeo4jWith(BOLT_V4)
@ParallelizableIT
@SuppressWarnings("deprecation")
class RxSessionIT {
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    @Test
    @SuppressWarnings("resource")
    void shouldAllowSessionRun() {
        // When
        var session = neo4j.driver().rxSession();
        var res = session.run("UNWIND [1,2,3,4] AS a RETURN a");

        // Then I should be able to iterate over the result
        StepVerifier.create(Flux.from(res.records()).map(r -> r.get("a").asInt()))
                .expectNext(1)
                .expectNext(2)
                .expectNext(3)
                .expectNext(4)
                .expectComplete()
                .verify();
    }

    @Test
    @SuppressWarnings("resource")
    void shouldBeAbleToReuseSessionAfterFailure() {
        // Given
        var session = neo4j.driver().rxSession();
        var res1 = session.run("INVALID");

        StepVerifier.create(res1.records()).expectError(ClientException.class).verify();

        // When
        var res2 = session.run("RETURN 1");

        // Then
        StepVerifier.create(res2.records())
                .assertNext(record -> assertEquals(record.get("1").asLong(), 1L))
                .expectComplete()
                .verify();
    }

    @Test
    @SuppressWarnings("resource")
    void shouldRunAsyncTransactionWithoutRetries() {
        var session = neo4j.driver().rxSession();
        var work = new InvocationTrackingWork("CREATE (:Apa) RETURN 42");
        var publisher = session.writeTransaction(work);

        StepVerifier.create(publisher).expectNext(42).verifyComplete();

        assertEquals(1, work.invocationCount());
        assertEquals(1, countNodesByLabel("Apa"));
    }

    @Test
    @SuppressWarnings("resource")
    void shouldRunAsyncTransactionWithRetriesOnAsyncFailures() {
        var session = neo4j.driver().rxSession();
        var work = new InvocationTrackingWork("CREATE (:Node) RETURN 24")
                .withAsyncFailures(
                        new ServiceUnavailableException("Oh!"),
                        new SessionExpiredException("Ah!"),
                        new TransientException("Code", "Message"));

        var publisher = session.writeTransaction(work);
        StepVerifier.create(publisher).expectNext(24).verifyComplete();

        assertEquals(4, work.invocationCount());
        assertEquals(1, countNodesByLabel("Node"));
        assertNoParallelScheduler();
    }

    @Test
    @SuppressWarnings("resource")
    void shouldRunAsyncTransactionWithRetriesOnSyncFailures() {
        var session = neo4j.driver().rxSession();
        var work = new InvocationTrackingWork("CREATE (:Test) RETURN 12")
                .withSyncFailures(
                        new TransientException("Oh!", "Deadlock!"),
                        new ServiceUnavailableException("Oh! Network Failure"));

        var publisher = session.writeTransaction(work);
        StepVerifier.create(publisher).expectNext(12).verifyComplete();

        assertEquals(3, work.invocationCount());
        assertEquals(1, countNodesByLabel("Test"));
        assertNoParallelScheduler();
    }

    @Test
    @SuppressWarnings("resource")
    void shouldRunAsyncTransactionThatCanNotBeRetried() {
        var session = neo4j.driver().rxSession();
        var work = new InvocationTrackingWork("UNWIND [10, 5, 0] AS x CREATE (:Hi) RETURN 10/x");
        var publisher = session.writeTransaction(work);

        StepVerifier.create(publisher)
                .expectNext(1)
                .expectNext(2)
                .expectErrorSatisfies(error -> assertThat(error, instanceOf(ClientException.class)))
                .verify();

        assertEquals(1, work.invocationCount());
        assertEquals(0, countNodesByLabel("Hi"));
        assertNoParallelScheduler();
    }

    @Test
    @SuppressWarnings("resource")
    void shouldRunAsyncTransactionThatCanNotBeRetriedAfterATransientFailure() {
        var session = neo4j.driver().rxSession();
        // first throw TransientException directly from work, retry can happen afterwards
        // then return a future failed with DatabaseException, retry can't happen afterwards
        var work = new InvocationTrackingWork("CREATE (:Person) RETURN 1")
                .withSyncFailures(new TransientException("Oh!", "Deadlock!"))
                .withAsyncFailures(new DatabaseException("Oh!", "OutOfMemory!"));
        var publisher = session.writeTransaction(work);

        StepVerifier.create(publisher)
                .expectErrorSatisfies(e -> {
                    assertThat(e, instanceOf(DatabaseException.class));
                    assertEquals(1, e.getSuppressed().length);
                    assertThat(e.getSuppressed()[0], instanceOf(TransientException.class));
                })
                .verify();

        assertEquals(2, work.invocationCount());
        assertEquals(0, countNodesByLabel("Person"));
        assertNoParallelScheduler();
    }

    @ParameterizedTest
    @MethodSource("managedTransactionsReturningReactiveResultPublisher")
    @SuppressWarnings("resource")
    void shouldErrorWhenReactiveResultIsReturned(Function<RxSession, Publisher<RxResult>> fn) {
        // GIVEN
        var session = neo4j.driver().rxSession();

        // WHEN & THEN
        var error = assertThrows(
                ClientException.class, () -> Flux.from(fn.apply(session)).blockFirst());
        assertEquals(
                "org.neo4j.driver.reactive.RxResult is not a valid return value, it should be consumed before producing a return value",
                error.getMessage());
        Flux.from(session.close()).blockFirst();
    }

    @SuppressWarnings("deprecation")
    static List<Function<RxSession, Publisher<RxResult>>> managedTransactionsReturningReactiveResultPublisher() {
        return List.of(
                session -> session.writeTransaction(tx -> Flux.just(tx.run("RETURN 1"))),
                session -> session.readTransaction(tx -> Flux.just(tx.run("RETURN 1"))));
    }

    private void assertNoParallelScheduler() {
        var threadSet = Thread.getAllStackTraces().keySet();
        for (var t : threadSet) {
            var name = t.getName();
            assertThat(name, not(startsWith("parallel")));
        }
    }

    @SuppressWarnings("resource")
    private long countNodesByLabel(String label) {
        try (var session = neo4j.driver().session()) {
            var result = session.run("MATCH (n:" + label + ") RETURN count(n)");
            return result.single().get(0).asLong();
        }
    }

    private static class InvocationTrackingWork implements RxTransactionWork<Publisher<Integer>> {
        final String query;
        final AtomicInteger invocationCount;

        Iterator<RuntimeException> asyncFailures = emptyIterator();
        Iterator<RuntimeException> syncFailures = emptyIterator();

        InvocationTrackingWork(String query) {
            this.query = query;
            this.invocationCount = new AtomicInteger();
        }

        InvocationTrackingWork withAsyncFailures(RuntimeException... failures) {
            asyncFailures = Arrays.asList(failures).iterator();
            return this;
        }

        InvocationTrackingWork withSyncFailures(RuntimeException... failures) {
            syncFailures = Arrays.asList(failures).iterator();
            return this;
        }

        int invocationCount() {
            return invocationCount.get();
        }

        @Override
        public Publisher<Integer> execute(RxTransaction tx) {
            invocationCount.incrementAndGet();

            if (syncFailures.hasNext()) {
                throw syncFailures.next();
            }

            if (asyncFailures.hasNext()) {
                return Mono.error(asyncFailures.next());
            }

            return Flux.from(tx.run(query).records()).map(r -> r.get(0).asInt());
        }
    }
}
