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

import io.reactivex.Flowable;
import reactor.core.publisher.Flux;

import java.util.Collections;
import java.util.Map;

import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.reactive.RxTransaction;

public class RxExplicitTransactionExample extends BaseApplication
{
    public RxExplicitTransactionExample( String uri, String user, String password )
    {
        super( uri, user, password );
    }

    public Flux<String> readSingleProductReactor()
    {
        // tag::reactor-explicit-transaction[]
        String query = "MATCH (p:Product) WHERE p.id = $id RETURN p.title";
        Map<String,Object> parameters = Collections.singletonMap( "id", 0 );

        RxSession session = driver.rxSession();
        // It is recommended to use Flux.usingWhen for explicit transactions and Flux.using for autocommit transactions (session).
        // This is because an explicit transaction needs to be supplied via a another resource publisher session.beginTransaction.
        return Flux.usingWhen( session.beginTransaction(),
                tx -> Flux.from( tx.run( query, parameters ).records() ).map( record -> record.get( 0 ).asString() ),
                RxTransaction::commit,
                RxTransaction::rollback );
        // end::reactor-explicit-transaction[]
    }

    public Flowable<String> readSingleProductRxJava()
    {
        // tag::RxJava-explicit-transaction[]
        String query = "MATCH (p:Product) WHERE p.id = $id RETURN p.title";
        Map<String,Object> parameters = Collections.singletonMap( "id", 0 );

        RxSession session = driver.rxSession();
        return Flowable.fromPublisher( session.beginTransaction() )
                .flatMap( tx -> Flowable.fromPublisher( tx.run( query, parameters ).records() ).map( record -> record.get( 0 ).asString() )
                        .doOnComplete( tx::commit )
                        .doOnError( error -> tx.rollback() ) );
        // end::RxJava-explicit-transaction[]
    }
}
