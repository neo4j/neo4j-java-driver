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
package org.neo4j.docs.driver;

// tag::rx-result-consume-import[]

import org.neo4j.driver.reactive.RxResult;
import reactor.adapter.JdkFlowAdapter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
// end::rx-result-consume-import[]

@SuppressWarnings("deprecation")
public class RxResultConsumeExample extends BaseApplication {
    public RxResultConsumeExample(String uri, String user, String password) {
        super(uri, user, password);
    }

    // tag::rx-result-consume[]
    public Flux<String> getPeople() {
        String query = "MATCH (a:Person) RETURN a.name ORDER BY a.name";

        return Flux.usingWhen(
                Mono.fromSupplier(driver::rxSession),
                session -> JdkFlowAdapter.flowPublisherToFlux(session.readTransaction(tx -> {
                    RxResult result = tx.run(query);
                    return JdkFlowAdapter.publisherToFlowPublisher(Flux.from(JdkFlowAdapter.flowPublisherToFlux(result.records()))
                            .map(record -> record.get(0).asString()));
                })),
                session -> JdkFlowAdapter.flowPublisherToFlux(session.close()));
    }
    // end::rx-result-consume[]

    // tag::RxJava-result-consume[]
//    public Flowable<String> getPeopleRxJava() {
//        String query = "MATCH (a:Person) RETURN a.name ORDER BY a.name";
//        Map<String, Object> parameters = Collections.singletonMap("id", 0);
//
//        return Flowable.using(
//                driver::rxSession,
//                session -> session.readTransaction(tx -> {
//                    RxResult result = tx.run(query, parameters);
//                    return Flowable.fromPublisher(result.records())
//                            .map(record -> record.get(0).asString());
//                }),
//                session -> Observable.fromPublisher(session.close()).subscribe());
//    }
    // end::RxJava-result-consume[]
}
