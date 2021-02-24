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
// tag::reactor-unmanaged-transaction-import[]
import io.reactivex.Observable;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.Map;

import org.neo4j.driver.reactive.RxQueryRunner;
import org.neo4j.driver.reactive.RxSession;

// tag::reactor-unmanaged-transaction-import[]
public class RxUnmanagedTransactionExample extends BaseApplication
{
    public RxUnmanagedTransactionExample(String uri, String user, String password )
    {
        super( uri, user, password );
    }

    // tag::reactor-unmanaged-transaction[]
    static class QueryRunnerAndCallbacks
    {
        final RxQueryRunner queryRunner;

        final Publisher<Void> commit;
        final Publisher<Void> rollback;

        QueryRunnerAndCallbacks( RxQueryRunner queryRunner, Publisher<Void> commit, Publisher<Void> rollback )
        {
            this.queryRunner = queryRunner;
            this.commit = commit;
            this.rollback = rollback;
        }
    }

    public Flux<String> readSingleProduct()
    {
        String query = "MATCH (p:Product) WHERE p.id = $id RETURN p.title";
        Map<String,Object> parameters = Collections.singletonMap( "id", 0 );

        // The additional holder is required to make both usingWhen constructs close
        // the resources in the correct order.
        Mono<QueryRunnerAndCallbacks> queryRunnerSupplier = Mono.using(
                driver::rxSession,
                session -> Mono.from( session.beginTransaction() ).map(tx -> new QueryRunnerAndCallbacks( tx, tx.commit(), tx.rollback() ) ),
                RxSession::close
        );

        return Flux.usingWhen(
                queryRunnerSupplier,
                queryRunnerAndCallbacks -> Flux.from( queryRunnerAndCallbacks.queryRunner.run( query, parameters ).records() ).map( record -> record.get( 0 ).asString() ),
                queryRunnerAndCallbacks -> queryRunnerAndCallbacks.commit,
                (queryRunnerAndCallbacks, error) -> queryRunnerAndCallbacks.rollback,
                queryRunnerAndCallbacks -> queryRunnerAndCallbacks.rollback
        );
    }
    // end::reactor-unmanaged-transaction[]

    // tag::RxJava-unmanaged-transaction[]
    public Flowable<String> readSingleProductRxJava()
    {
        String query = "MATCH (p:Product) WHERE p.id = $id RETURN p.title";
        Map<String,Object> parameters = Collections.singletonMap( "id", 0 );

        return Flowable.using(
                driver::rxSession,
                session -> Flowable.fromPublisher( session.beginTransaction() )
                        .flatMap( tx ->
                                Flowable.fromPublisher( tx.run( query, parameters ).records() )
                                        .map( record -> record.get( 0 ).asString() )
                                        .concatWith( tx.commit() )
                                        .onErrorResumeNext( error -> {
                                            // We rollback and rethrow the error. For a real application, you may want to handle the error directly here
                                            return Flowable.<String>fromPublisher( tx.rollback() ).concatWith( Flowable.error( error ) );
                                        } )
                        ),
                session -> Observable.fromPublisher(session.close()).onErrorResumeNext( Observable.empty() ).subscribe()
        );
    }
    // end::RxJava-unmanaged-transaction[]
}
