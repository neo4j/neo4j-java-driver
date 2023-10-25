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

import static org.neo4j.driver.internal.util.Neo4jFeature.BOLT_V4;

import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.driver.exceptions.TransactionNestingException;
import org.neo4j.driver.internal.util.EnabledOnNeo4jWith;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.reactive.RxTransaction;
import org.neo4j.driver.testutil.DatabaseExtension;
import org.neo4j.driver.testutil.ParallelizableIT;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@EnabledOnNeo4jWith(BOLT_V4)
@ParallelizableIT
@SuppressWarnings("deprecation")
class RxNestedQueriesIT {
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    @Test
    @SuppressWarnings("resource")
    void shouldErrorForNestingQueriesAmongSessionRuns() {
        var size = 12555;

        var nodeIds = Flux.usingWhen(
                Mono.fromSupplier(() -> neo4j.driver().rxSession()),
                session -> Flux.from(session.run(
                                        "UNWIND range(1, $size) AS x RETURN x", Collections.singletonMap("size", size))
                                .records())
                        .limitRate(20)
                        .flatMap(record -> {
                            var x = record.get("x").asInt();
                            var innerResult = session.run(
                                    "CREATE (n:Node {id: $x}) RETURN n.id", Collections.singletonMap("x", x));
                            return innerResult.records();
                        })
                        .map(r -> r.get(0).asInt()),
                RxSession::close);

        StepVerifier.create(nodeIds)
                .expectError(TransactionNestingException.class)
                .verify();
    }

    @Test
    @SuppressWarnings("resource")
    void shouldErrorForNestingQueriesAmongTransactionFunctions() {
        var size = 12555;
        var nodeIds = Flux.usingWhen(
                Mono.fromSupplier(() -> neo4j.driver().rxSession()),
                session -> Flux.from(session.readTransaction(tx -> tx.run(
                                        "UNWIND range(1, $size) AS x RETURN x", Collections.singletonMap("size", size))
                                .records()))
                        .limitRate(20)
                        .flatMap(record -> {
                            var x = record.get("x").asInt();
                            return session.writeTransaction(tx -> tx.run(
                                            "CREATE (n:Node {id: $x}) RETURN n.id", Collections.singletonMap("x", x))
                                    .records());
                        })
                        .map(r -> r.get(0).asInt()),
                RxSession::close);

        StepVerifier.create(nodeIds)
                .expectError(TransactionNestingException.class)
                .verify();
    }

    @Test
    @SuppressWarnings("resource")
    void shouldErrorForNestingQueriesAmongSessionRunAndTransactionFunction() {
        var size = 12555;
        var nodeIds = Flux.usingWhen(
                Mono.fromSupplier(() -> neo4j.driver().rxSession()),
                session -> Flux.from(session.run(
                                        "UNWIND range(1, $size) AS x RETURN x", Collections.singletonMap("size", size))
                                .records())
                        .limitRate(20)
                        .flatMap(record -> {
                            var x = record.get("x").asInt();
                            return session.writeTransaction(tx -> tx.run(
                                            "CREATE (n:Node {id: $x}) RETURN n.id", Collections.singletonMap("x", x))
                                    .records());
                        })
                        .map(r -> r.get(0).asInt()),
                RxSession::close);

        StepVerifier.create(nodeIds)
                .expectError(TransactionNestingException.class)
                .verify();
    }

    @Test
    @SuppressWarnings("resource")
    void shouldErrorForNestingQueriesAmongTransactionFunctionAndSessionRun() {
        var size = 12555;
        var nodeIds = Flux.usingWhen(
                Mono.fromSupplier(() -> neo4j.driver().rxSession()),
                session -> Flux.from(session.readTransaction(tx -> tx.run(
                                        "UNWIND range(1, $size) AS x RETURN x", Collections.singletonMap("size", size))
                                .records()))
                        .limitRate(20)
                        .flatMap(record -> {
                            var x = record.get("x").asInt();
                            return session.run("CREATE (n:Node {id: $x}) RETURN n.id", Collections.singletonMap("x", x))
                                    .records();
                        })
                        .map(r -> r.get(0).asInt()),
                RxSession::close);

        StepVerifier.create(nodeIds)
                .expectError(TransactionNestingException.class)
                .verify();
    }

    @Test
    @SuppressWarnings("resource")
    void shouldHandleNestedQueriesInTheSameTransaction() {
        var size = 12555;

        var session = neo4j.driver().rxSession();
        var nodeIds = Flux.usingWhen(
                session.beginTransaction(),
                tx -> {
                    var result = tx.run("UNWIND range(1, $size) AS x RETURN x", Collections.singletonMap("size", size));
                    return Flux.from(result.records())
                            .limitRate(20)
                            .flatMap(record -> {
                                var x = record.get("x").asInt();
                                var innerResult = tx.run(
                                        "CREATE (n:Node {id: $x}) RETURN n.id", Collections.singletonMap("x", x));
                                return innerResult.records();
                            })
                            .map(record -> record.get(0).asInt());
                },
                RxTransaction::commit,
                (tx, error) -> tx.rollback(),
                RxTransaction::close);

        StepVerifier.create(nodeIds).expectNextCount(size).verifyComplete();
    }
}
