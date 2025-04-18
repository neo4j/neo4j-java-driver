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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.driver.Config;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.TransactionTerminatedException;
import org.neo4j.driver.internal.reactivestreams.InternalReactiveTransaction;
import org.neo4j.driver.reactivestreams.ReactiveResult;
import org.neo4j.driver.reactivestreams.ReactiveSession;
import org.neo4j.driver.testutil.DatabaseExtension;
import org.neo4j.driver.testutil.ParallelizableIT;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@ParallelizableIT
class ReactiveTransactionIT {
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    @Test
    @SuppressWarnings("resource")
    void shouldPreventPullAfterTransactionTermination() {
        // Given
        var session = neo4j.driver().session(ReactiveSession.class);
        var tx = Mono.fromDirect(session.beginTransaction()).block();
        assertNotNull(tx);
        var streamSize = Config.defaultConfig().fetchSize() + 1;
        var result0 = Mono.fromDirect(tx.run("UNWIND range(1, $limit) AS x RETURN x", Map.of("limit", streamSize)))
                .block();
        var result1 = Mono.fromDirect(tx.run("UNWIND range(1, $limit) AS x RETURN x", Map.of("limit", streamSize)))
                .block();

        // When
        var terminationException = assertThrows(
                ClientException.class, () -> Mono.fromDirect(tx.run("invalid")).block());
        assertEquals(terminationException.code(), "Neo.ClientError.Statement.SyntaxError");

        // Then
        assertNotNull(result0);
        assertNotNull(result1);
        for (var result : List.of(result0, result1)) {
            var exception = assertThrows(
                    ClientException.class, () -> Flux.from(result.records()).blockFirst());
            assertEquals(terminationException, exception.getCause());
        }
        Mono.fromDirect(tx.close()).block();
    }

    @Test
    @SuppressWarnings("resource")
    void shouldPreventDiscardAfterTransactionTermination() {
        // Given
        var session = neo4j.driver().session(ReactiveSession.class);
        var tx = Mono.fromDirect(session.beginTransaction()).block();
        assertNotNull(tx);
        var streamSize = Config.defaultConfig().fetchSize() + 1;
        var result0 = Mono.fromDirect(tx.run("UNWIND range(1, $limit) AS x RETURN x", Map.of("limit", streamSize)))
                .block();
        var result1 = Mono.fromDirect(tx.run("UNWIND range(1, $limit) AS x RETURN x", Map.of("limit", streamSize)))
                .block();

        // When
        var terminationException = assertThrows(
                ClientException.class, () -> Mono.fromDirect(tx.run("invalid")).block());
        assertEquals(terminationException.code(), "Neo.ClientError.Statement.SyntaxError");

        // Then
        assertNotNull(result0);
        assertNotNull(result1);
        for (var result : List.of(result0, result1)) {
            var exception = assertThrows(ClientException.class, () -> Mono.fromDirect(result.consume())
                    .block());
            assertEquals(terminationException, exception.getCause());
        }
        Mono.fromDirect(tx.close()).block();
    }

    @Test
    @SuppressWarnings("resource")
    void shouldPreventRunAfterTransactionTermination() {
        // Given
        var session = neo4j.driver().session(ReactiveSession.class);
        var tx = Mono.fromDirect(session.beginTransaction()).block();
        assertNotNull(tx);
        var terminationException = assertThrows(
                ClientException.class, () -> Mono.fromDirect(tx.run("invalid")).block());
        assertEquals(terminationException.code(), "Neo.ClientError.Statement.SyntaxError");

        // When
        var exception = assertThrows(TransactionTerminatedException.class, () -> Mono.fromDirect(tx.run("RETURN 1"))
                .block());

        // Then
        assertEquals(terminationException, exception.getCause());
        Mono.fromDirect(tx.close()).block();
    }

    @Test
    @SuppressWarnings("resource")
    void shouldPreventPullAfterDriverTransactionTermination() {
        // Given
        var session = neo4j.driver().session(ReactiveSession.class);
        var tx = (InternalReactiveTransaction)
                Mono.fromDirect(session.beginTransaction()).block();
        assertNotNull(tx);
        var streamSize = Config.defaultConfig().fetchSize() + 1;
        var result0 = Mono.fromDirect(tx.run("UNWIND range(1, $limit) AS x RETURN x", Map.of("limit", streamSize)))
                .block();
        var result1 = Mono.fromDirect(tx.run("UNWIND range(1, $limit) AS x RETURN x", Map.of("limit", streamSize)))
                .block();

        // When
        Mono.fromDirect(tx.terminate()).block();

        // Then
        assertNotNull(result0);
        assertNotNull(result1);
        for (var result : List.of(result0, result1)) {
            assertThrows(TransactionTerminatedException.class, () -> Flux.from(result.records())
                    .blockFirst());
        }
        Mono.fromDirect(tx.close()).block();
    }

    @Test
    @SuppressWarnings("resource")
    void shouldPreventDiscardAfterDriverTransactionTermination() {
        // Given
        var session = neo4j.driver().session(ReactiveSession.class);
        var tx = (InternalReactiveTransaction)
                Mono.fromDirect(session.beginTransaction()).block();
        assertNotNull(tx);
        var streamSize = Config.defaultConfig().fetchSize() + 1;
        var result0 = Mono.fromDirect(tx.run("UNWIND range(1, $limit) AS x RETURN x", Map.of("limit", streamSize)))
                .block();
        var result1 = Mono.fromDirect(tx.run("UNWIND range(1, $limit) AS x RETURN x", Map.of("limit", streamSize)))
                .block();

        // When
        Mono.fromDirect(tx.terminate()).block();

        // Then
        assertNotNull(result0);
        assertNotNull(result1);
        for (var result : List.of(result0, result1)) {
            assertThrows(TransactionTerminatedException.class, () -> Mono.fromDirect(result.consume())
                    .block());
        }
        Mono.fromDirect(tx.close()).block();
    }

    @Test
    @SuppressWarnings("resource")
    void shouldPreventRunAfterDriverTransactionTermination() {
        // Given
        var session = neo4j.driver().session(ReactiveSession.class);
        var tx = (InternalReactiveTransaction)
                Mono.fromDirect(session.beginTransaction()).block();
        assertNotNull(tx);
        var streamSize = Config.defaultConfig().fetchSize() + 1;
        Mono.fromDirect(tx.run("UNWIND range(1, $limit) AS x RETURN x", Map.of("limit", streamSize)))
                .block();

        // When
        Mono.fromDirect(tx.terminate()).block();

        // Then
        assertThrows(
                TransactionTerminatedException.class, () -> Mono.fromDirect(tx.run("UNWIND range(0, 5) AS x RETURN x"))
                        .block());
        Mono.fromDirect(tx.close()).block();
    }

    @Test
    @SuppressWarnings("resource")
    void shouldTerminateTransactionAndHandleFailureResponseOrPreventFurtherPulls() {
        // Given
        var session = neo4j.driver().session(ReactiveSession.class);
        var tx = (InternalReactiveTransaction)
                Mono.fromDirect(session.beginTransaction()).block();
        assertNotNull(tx);
        var streamSize = Config.defaultConfig().fetchSize() + 1;
        var result = Mono.fromDirect(tx.run("UNWIND range(1, $limit) AS x RETURN x", Map.of("limit", streamSize)))
                .block();

        // When
        Mono.fromDirect(tx.terminate()).block();

        // Then
        assertNotNull(result);
        assertThrows(TransactionTerminatedException.class, () -> Flux.from(result.records())
                .blockLast());
        Mono.fromDirect(tx.close()).block();
    }

    @Test
    @SuppressWarnings("resource")
    @Timeout(value = 20, unit = TimeUnit.MINUTES)
    void shouldBeAbleToRunMultipleQueriesWhileFetchingRecords() {
        // Given
        var session = neo4j.driver().session(ReactiveSession.class);
        var tx = Mono.fromDirect(session.beginTransaction()).block();
        assertNotNull(tx);
        var recordsFutures = new CompletableFuture<?>[100];

        // When
        for (var i = 0; i < recordsFutures.length; i++) {
            var recordsFuture = new CompletableFuture<Void>();
            recordsFutures[i] = recordsFuture;
            // This effectively leads to the main thread submitting new queries while the driver thread is fetching
            // records from results as they become available. The objective is to make sure the driver handles shared
            // Bolt connection as expected.
            Mono.fromDirect(tx.run("UNWIND range(0, 10) AS x RETURN x"))
                    .flatMapMany(ReactiveResult::records)
                    .collectList()
                    .doOnNext(ignored -> recordsFuture.complete(null))
                    .subscribe();
        }

        // Then
        CompletableFuture.allOf(recordsFutures).join();
        Mono.fromDirect(tx.close()).block();
    }
}
