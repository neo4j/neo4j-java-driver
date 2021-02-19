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
package org.neo4j.driver.internal.reactive;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.test.StepVerifier;

import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

import org.neo4j.driver.internal.util.Futures;

import static org.neo4j.driver.internal.util.Futures.failedFuture;
import static org.neo4j.driver.internal.reactive.RxUtils.createEmptyPublisher;
import static org.neo4j.driver.internal.reactive.RxUtils.createMono;

class RxUtilsTest
{
    @Test
    void emptyPublisherShouldComplete() throws Throwable
    {
        Publisher<Void> emptyPublisher = createEmptyPublisher( Futures::completedWithNull );
        StepVerifier.create( emptyPublisher ).verifyComplete();
    }

    @Test
    void emptyPublisherShouldErrorIfSupplierErrored() throws Throwable
    {
        RuntimeException error = new RuntimeException( "Error" );
        Publisher<Void> emptyPublisher = createEmptyPublisher( () -> failedFuture( error ) );

        StepVerifier.create( emptyPublisher ).verifyErrorMatches( Predicate.isEqual( error ) );
    }

    @Test
    void monoPublisherShouldCompleteWithValue() throws Throwable
    {
        Publisher<String> mono = createMono( () -> CompletableFuture.completedFuture( "One" ) );
        StepVerifier.create( mono ).expectNext( "One" ).verifyComplete();
    }

    @Test
    void monoPublisherShouldErrorIfSupplierErrored() throws Throwable
    {
        RuntimeException error = new RuntimeException( "Error" );
        Publisher<String> mono = createMono( () -> failedFuture( error ) );

        StepVerifier.create( mono ).verifyErrorMatches( Predicate.isEqual( error ) );
    }
}
