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

import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.neo4j.driver.internal.util.Futures;

public class RxUtils
{
    /**
     * The publisher created by this method will either succeed without publishing anything or fail with an error.
     * @param supplier supplies a {@link CompletionStage<Void>}.
     * @return A publisher that publishes nothing on completion or fails with an error.
     */
    public static <T> Publisher<T> createEmptyPublisher( Supplier<CompletionStage<Void>> supplier )
    {
        return Mono.create( sink -> supplier.get().whenComplete( ( ignore, completionError ) -> {
            Throwable error = Futures.completionExceptionCause( completionError );
            if ( error != null )
            {
                sink.error( error );
            }
            else
            {
                sink.success();
            }
        } ) );
    }

    /**
     * Create a {@link Mono<T>} publisher from the given {@link CompletionStage<T>} supplier.
     * @param supplier supplies a {@link CompletionStage<T>}.
     * @param <T> the type of the item to publish.
     * @return A {@link Mono<T>} publisher.
     */
    public static <T> Publisher<T> createMono( Supplier<CompletionStage<T>> supplier )
    {
        return Mono.create( sink -> supplier.get().whenComplete( ( item, completionError ) -> {
            Throwable error = Futures.completionExceptionCause( completionError );
            if ( item != null )
            {
                sink.success( item );
            }
            if ( error != null )
            {
                sink.error( error );
            }
            else
            {
                sink.success();
            }
        } ) );
    }
}
