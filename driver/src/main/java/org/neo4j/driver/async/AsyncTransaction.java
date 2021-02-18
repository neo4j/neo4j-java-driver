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
package org.neo4j.driver.async;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Function;

import org.neo4j.driver.Session;
import org.neo4j.driver.Query;
import org.neo4j.driver.QueryRunner;

/**
 * Logical container for an atomic unit of work.
 * A driver Transaction object corresponds to a server transaction.
 * <p>
 * Transactions are typically obtained in a {@link CompletionStage} and all
 * operations chain on this stage. Explicit commit with {@link #commitAsync()}
 * or rollback with {@link #rollbackAsync()} is required. Without explicit
 * commit/rollback corresponding transaction will remain open in the database.
 * <pre>
 * {@code
 * session.beginTransactionAsync()
 *        .thenCompose(tx ->
 *               tx.runAsync("CREATE (a:Person {name: $name})", parameters("name", "Alice"))
 *                 .exceptionally(e -> {
 *                    e.printStackTrace();
 *                    return null;
 *                 })
 *                 .thenApply(ignore -> tx)
 *        ).thenCompose(Transaction::commitAsync);
 * }
 * </pre>
 * Async calls are: {@link #commitAsync()}, {@link #rollbackAsync()} and various overloads of
 * {@link #runAsync(Query)}.
 *
 * @see Session#run
 * @see QueryRunner
 * @since 4.0
 */
public interface AsyncTransaction extends AsyncQueryRunner
{
    /**
     * Commit this transaction in asynchronous fashion. This operation is typically executed as part of the
     * {@link CompletionStage} chain that starts with a transaction.
     * There is no need to close transaction after calling this method.
     * Transaction object should not be used after calling this method.
     * <p>
     * Returned stage can be completed by an IO thread which should never block. Otherwise IO operations on this and
     * potentially other network connections might deadlock. Please do not chain blocking operations like
     * {@link CompletableFuture#get()} on the returned stage. Consider using asynchronous calls throughout the chain or offloading blocking
     * operation to a different {@link Executor}. This can be done using methods with "Async" suffix like
     * {@link CompletionStage#thenApplyAsync(Function)} or {@link CompletionStage#thenApplyAsync(Function, Executor)}.
     *
     * @return new {@link CompletionStage} that gets completed with {@code null} when commit is successful. Stage can
     * be completed exceptionally when commit fails.
     */
    CompletionStage<Void> commitAsync();

    /**
     * Rollback this transaction in asynchronous fashion. This operation is typically executed as part of the
     * {@link CompletionStage} chain that starts with a transaction.
     * There is no need to close transaction after calling this method.
     * Transaction object should not be used after calling this method.
     * <p>
     * Returned stage can be completed by an IO thread which should never block. Otherwise IO operations on this and
     * potentially other network connections might deadlock. Please do not chain blocking operations like
     * {@link CompletableFuture#get()} on the returned stage. Consider using asynchronous calls throughout the chain or offloading blocking
     * operation to a different {@link Executor}. This can be done using methods with "Async" suffix like
     * {@link CompletionStage#thenApplyAsync(Function)} or {@link CompletionStage#thenApplyAsync(Function, Executor)}.
     *
     * @return new {@link CompletionStage} that gets completed with {@code null} when rollback is successful. Stage can
     * be completed exceptionally when rollback fails.
     */
    CompletionStage<Void> rollbackAsync();
}
