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
package org.neo4j.driver.reactive;

import org.reactivestreams.Publisher;

import org.neo4j.driver.Transaction;

/**
 * Same as {@link Transaction} except this reactive transaction exposes a reactive API.
 * @see Transaction
 * @see RxSession
 * @see Publisher
 * @since 4.0
 */
public interface RxTransaction extends RxQueryRunner
{
    /**
     * Commits the transaction.
     * It completes without publishing anything if transaction is committed successfully.
     * Otherwise, errors when there is any error to commit.
     * @param <T> makes it easier to be chained after other publishers.
     * @return an empty publisher.
     */
    <T> Publisher<T> commit();

    /**
     * Rolls back the transaction.
     * It completes without publishing anything if transaction is rolled back successfully.
     * Otherwise, errors when there is any error to roll back.
     * @param <T> makes it easier to be chained after other publishers.
     * @return an empty publisher.
     */
    <T> Publisher<T> rollback();
}
