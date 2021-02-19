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

/**
 * Callback that executes operations against a given {@link RxTransaction}.
 * To be used with {@link RxSession#readTransaction(RxTransactionWork)} and
 * {@link RxSession#writeTransaction(RxTransactionWork)} methods.
 *
 * @param <T> the return type of this work.
 * @since 4.0
 */
public interface RxTransactionWork<T>
{
    /**
     * Executes all given operations against the same transaction.
     *
     * @param tx the transaction to use.
     * @return some result object or {@code null} if none.
     */
    T execute( RxTransaction tx );
}
