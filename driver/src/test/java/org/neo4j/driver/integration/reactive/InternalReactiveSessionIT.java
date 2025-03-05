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
import static reactor.adapter.JdkFlowAdapter.flowPublisherToFlux;

import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.bolt.connection.TelemetryApi;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.internal.reactive.InternalReactiveSession;
import org.neo4j.driver.internal.telemetry.ApiTelemetryWork;
import org.neo4j.driver.internal.util.EnabledOnNeo4jWith;
import org.neo4j.driver.reactive.ReactiveSession;
import org.neo4j.driver.reactive.ReactiveTransaction;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.testutil.DatabaseExtension;
import org.neo4j.driver.testutil.ParallelizableIT;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@EnabledOnNeo4jWith(BOLT_V4)
@ParallelizableIT
class InternalReactiveSessionIT {
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    private InternalReactiveSession session;

    @BeforeEach
    @SuppressWarnings("resource")
    void setUp() {
        session = (InternalReactiveSession) neo4j.driver().session(ReactiveSession.class);
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"IMPLICIT", ""})
    void shouldAcceptTxTypeWhenAvailable(String txType) {
        // GIVEN
        var txConfig = TransactionConfig.empty();
        var apiTelemetryWork = new ApiTelemetryWork(TelemetryApi.UNMANAGED_TRANSACTION);
        var txMono = Mono.fromDirect(flowPublisherToFlux(session.beginTransaction(txConfig, txType, apiTelemetryWork)));
        Function<ReactiveTransaction, Mono<ResultSummary>> txUnit =
                tx -> Mono.fromDirect(flowPublisherToFlux(tx.run("RETURN 1")))
                        .flatMap(result -> Mono.fromDirect(flowPublisherToFlux(result.consume())));
        Function<ReactiveTransaction, Mono<Void>> commit = tx -> Mono.fromDirect(flowPublisherToFlux(tx.commit()));

        // WHEN
        var summaryMono = Mono.usingWhen(txMono, txUnit, commit);

        // THEN
        StepVerifier.create(summaryMono).expectNextCount(1).expectComplete().verify();
    }
}
