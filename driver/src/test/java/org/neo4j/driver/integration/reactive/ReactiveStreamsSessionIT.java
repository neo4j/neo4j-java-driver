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
import static org.neo4j.driver.internal.util.Neo4jFeature.BOLT_V4;

import java.util.List;
import java.util.function.Function;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.util.EnabledOnNeo4jWith;
import org.neo4j.driver.reactivestreams.ReactiveResult;
import org.neo4j.driver.reactivestreams.ReactiveSession;
import org.neo4j.driver.testutil.DatabaseExtension;
import org.neo4j.driver.testutil.ParallelizableIT;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

@EnabledOnNeo4jWith(BOLT_V4)
@ParallelizableIT
public class ReactiveStreamsSessionIT {
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    @ParameterizedTest
    @MethodSource("managedTransactionsReturningReactiveResultPublisher")
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

    static List<Function<ReactiveSession, Publisher<ReactiveResult>>>
            managedTransactionsReturningReactiveResultPublisher() {
        return List.of(
                session -> session.executeWrite(tx -> tx.run("RETURN 1")),
                session -> session.executeRead(tx -> tx.run("RETURN 1")));
    }
}
