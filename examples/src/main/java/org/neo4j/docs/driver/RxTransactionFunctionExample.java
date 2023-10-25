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
package org.neo4j.docs.driver;

import org.neo4j.driver.Query;
import org.neo4j.driver.reactive.ReactiveResult;
import org.neo4j.driver.reactive.ReactiveSession;
import org.neo4j.driver.summary.ResultSummary;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import static reactor.adapter.JdkFlowAdapter.flowPublisherToFlux;
import static reactor.adapter.JdkFlowAdapter.publisherToFlowPublisher;

public class RxTransactionFunctionExample extends BaseApplication {
    public RxTransactionFunctionExample(String uri, String user, String password) {
        super(uri, user, password);
    }

    // tag::rx-transaction-function[]
    public Flux<ResultSummary> printAllProducts() {
        var query = new Query("MATCH (p:Product) WHERE p.id = $id RETURN p.title", Collections.singletonMap("id", 0));

        return Flux.usingWhen(
                Mono.fromSupplier(() -> driver.session(ReactiveSession.class)),
                session -> flowPublisherToFlux(session.executeRead(tx -> {
                    var resultRef = new AtomicReference<ReactiveResult>();
                    var flux = flowPublisherToFlux(tx.run(query))
                            .doOnNext(resultRef::set)
                            .flatMap(result -> flowPublisherToFlux(result.records()))
                            .doOnNext(record -> System.out.println(record.get(0).asString()))
                            .then(Mono.defer(() -> Mono.from(flowPublisherToFlux(resultRef.get().consume()))));
                    return publisherToFlowPublisher(flux);
                })),
                session -> flowPublisherToFlux(session.close()));
    }
    // end::rx-transaction-function[]
}
