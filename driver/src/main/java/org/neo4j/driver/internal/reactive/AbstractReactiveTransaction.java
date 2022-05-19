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

import static org.neo4j.driver.internal.reactive.RxUtils.createEmptyPublisher;

import org.neo4j.driver.internal.async.UnmanagedTransaction;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

abstract class AbstractReactiveTransaction {
    protected final UnmanagedTransaction tx;

    protected AbstractReactiveTransaction(UnmanagedTransaction tx) {
        this.tx = tx;
    }

    public <T> Publisher<T> commit() {
        return createEmptyPublisher(tx::commitAsync);
    }

    public <T> Publisher<T> rollback() {
        return createEmptyPublisher(tx::rollbackAsync);
    }

    public Publisher<Void> close() {
        return close(false);
    }

    public Publisher<Boolean> isOpen() {
        return Mono.just(tx.isOpen());
    }

    Publisher<Void> close(boolean commit) {
        return createEmptyPublisher(() -> tx.closeAsync(commit));
    }
}
