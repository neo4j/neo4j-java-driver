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
package org.neo4j.driver.internal.reactive;

import static org.neo4j.driver.internal.reactive.RxUtils.createEmptyPublisher;

import org.neo4j.driver.internal.async.UnmanagedTransaction;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

public abstract class AbstractReactiveTransaction {
    protected final UnmanagedTransaction tx;

    protected AbstractReactiveTransaction(UnmanagedTransaction tx) {
        this.tx = tx;
    }

    protected <T> Publisher<T> doCommit() {
        return createEmptyPublisher(tx::commitAsync);
    }

    protected <T> Publisher<T> doRollback() {
        return createEmptyPublisher(tx::rollbackAsync);
    }

    protected Publisher<Void> doClose() {
        return close(false);
    }

    protected Publisher<Boolean> doIsOpen() {
        return Mono.just(tx.isOpen());
    }

    public Publisher<Void> close(boolean commit) {
        return createEmptyPublisher(() -> tx.closeAsync(commit));
    }
}
