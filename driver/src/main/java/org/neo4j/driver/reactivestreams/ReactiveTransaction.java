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
package org.neo4j.driver.reactivestreams;

import org.neo4j.driver.Transaction;
import org.reactivestreams.Publisher;

/**
 * Same as {@link Transaction} except this reactive transaction exposes a reactive API.
 *
 * @see Transaction
 * @see ReactiveSession
 * @see Publisher
 * @since 5.2
 */
public interface ReactiveTransaction extends ReactiveQueryRunner {
    /**
     * Commits the transaction. It completes without publishing anything if transaction is committed successfully. Otherwise, errors when there is any error to
     * commit.
     *
     * @param <T> makes it easier to be chained after other publishers.
     * @return an empty publisher.
     */
    <T> Publisher<T> commit();

    /**
     * Rolls back the transaction. It completes without publishing anything if transaction is rolled back successfully. Otherwise, errors when there is any
     * error to roll back.
     *
     * @param <T> makes it easier to be chained after other publishers.
     * @return an empty publisher.
     */
    <T> Publisher<T> rollback();

    /**
     * Close the transaction. If the transaction has been {@link #commit() committed} or {@link #rollback() rolled back}, the close is optional and no operation
     * is performed. Otherwise, the transaction will be rolled back by default by this method.
     *
     * @return new {@link Publisher} that gets completed when close is successful, otherwise an error is signalled.
     */
    Publisher<Void> close();

    /**
     * Determine if transaction is open.
     *
     * @return a publisher emitting {@code true} if transaction is open and {@code false} otherwise.
     */
    Publisher<Boolean> isOpen();
}
