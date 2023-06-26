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
package org.neo4j.driver.integration.reactive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.driver.Config;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.TransactionTerminatedException;
import org.neo4j.driver.internal.reactivestreams.InternalReactiveTransaction;
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
    void shouldPreventPullAfterTransactionTermination() {
        // Given
        var session = neo4j.driver().session(ReactiveSession.class);
        var tx = Mono.fromDirect(session.beginTransaction()).block();
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
        for (var result : List.of(result0, result1)) {
            var exception = assertThrows(
                    ClientException.class, () -> Flux.from(result.records()).blockFirst());
            assertEquals(terminationException, exception);
        }
        Mono.fromDirect(tx.close()).block();
    }

    @Test
    void shouldPreventDiscardAfterTransactionTermination() {
        // Given
        var session = neo4j.driver().session(ReactiveSession.class);
        var tx = Mono.fromDirect(session.beginTransaction()).block();
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
        for (var result : List.of(result0, result1)) {
            var exception = assertThrows(ClientException.class, () -> Mono.fromDirect(result.consume())
                    .block());
            assertEquals(terminationException, exception);
        }
        Mono.fromDirect(tx.close()).block();
    }

    @Test
    void shouldPreventRunAfterTransactionTermination() {
        // Given
        var session = neo4j.driver().session(ReactiveSession.class);
        var tx = Mono.fromDirect(session.beginTransaction()).block();
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
    void shouldPreventPullAfterDriverTransactionTermination() {
        // Given
        var session = neo4j.driver().session(ReactiveSession.class);
        var tx = (InternalReactiveTransaction)
                Mono.fromDirect(session.beginTransaction()).block();
        var streamSize = Config.defaultConfig().fetchSize() + 1;
        var result0 = Mono.fromDirect(tx.run("UNWIND range(1, $limit) AS x RETURN x", Map.of("limit", streamSize)))
                .block();
        var result1 = Mono.fromDirect(tx.run("UNWIND range(1, $limit) AS x RETURN x", Map.of("limit", streamSize)))
                .block();

        // When
        Mono.fromDirect(tx.terminate()).block();

        // Then
        for (var result : List.of(result0, result1)) {
            assertThrows(TransactionTerminatedException.class, () -> Flux.from(result.records())
                    .blockFirst());
        }
        Mono.fromDirect(tx.close()).block();
    }

    @Test
    void shouldPreventDiscardAfterDriverTransactionTermination() {
        // Given
        var session = neo4j.driver().session(ReactiveSession.class);
        var tx = (InternalReactiveTransaction)
                Mono.fromDirect(session.beginTransaction()).block();
        var streamSize = Config.defaultConfig().fetchSize() + 1;
        var result0 = Mono.fromDirect(tx.run("UNWIND range(1, $limit) AS x RETURN x", Map.of("limit", streamSize)))
                .block();
        var result1 = Mono.fromDirect(tx.run("UNWIND range(1, $limit) AS x RETURN x", Map.of("limit", streamSize)))
                .block();

        // When
        Mono.fromDirect(tx.terminate()).block();

        // Then
        for (var result : List.of(result0, result1)) {
            assertThrows(TransactionTerminatedException.class, () -> Mono.fromDirect(result.consume())
                    .block());
        }
        Mono.fromDirect(tx.close()).block();
    }

    @Test
    void shouldPreventRunAfterDriverTransactionTermination() {
        // Given
        var session = neo4j.driver().session(ReactiveSession.class);
        var tx = (InternalReactiveTransaction)
                Mono.fromDirect(session.beginTransaction()).block();
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
    void shouldTerminateTransactionAndHandleFailureResponseOrPreventFurtherPulls() {
        // Given
        var session = neo4j.driver().session(ReactiveSession.class);
        var tx = (InternalReactiveTransaction)
                Mono.fromDirect(session.beginTransaction()).block();
        var streamSize = Config.defaultConfig().fetchSize() + 1;
        var result = Mono.fromDirect(tx.run("UNWIND range(1, $limit) AS x RETURN x", Map.of("limit", streamSize)))
                .block();

        // When
        Mono.fromDirect(tx.terminate()).block();

        // Then
        assertThrows(TransactionTerminatedException.class, () -> Flux.from(result.records())
                .blockLast());
        Mono.fromDirect(tx.close()).block();
    }
}
