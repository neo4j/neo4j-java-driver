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

import org.neo4j.driver.Query;
import org.reactivestreams.Publisher;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Session;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.Values;
import org.neo4j.driver.Bookmark;

/**
 * A reactive session is the same as {@link Session} except it provides a reactive API.
 * @see Session
 * @see RxResult
 * @see RxTransaction
 * @see Publisher
 * @since 4.0
 */
public interface RxSession extends RxQueryRunner
{
    /**
     * Begin a new <em>unmanaged {@linkplain RxTransaction transaction}</em>. At
     * most one transaction may exist in a session at any point in time. To
     * maintain multiple concurrent transactions, use multiple concurrent
     * sessions.
     * <p>
     * It by default is executed in a Network IO thread, as a result no blocking operation is allowed in this thread.
     *
     * @return a new {@link RxTransaction}
     */
    Publisher<RxTransaction> beginTransaction();

    /**
     * Begin a new <em>unmanaged {@linkplain RxTransaction transaction}</em> with the specified {@link TransactionConfig configuration}.
     * At most one transaction may exist in a session at any point in time. To
     * maintain multiple concurrent transactions, use multiple concurrent sessions.
     * <p>
     * It by default is executed in a Network IO thread, as a result no blocking operation is allowed in this thread.
     *
     * @param config configuration for the new transaction.
     * @return a new {@link RxTransaction}
     */
    Publisher<RxTransaction> beginTransaction( TransactionConfig config );

    /**
     * Execute given unit of reactive work in a {@link AccessMode#READ read} reactive transaction.
     <p>
     * Transaction will automatically be committed unless given unit of work fails or
     * {@link RxTransaction#commit() transaction commit} fails.
     * It will also not be committed if explicitly rolled back via {@link RxTransaction#rollback()}.
     * <p>
     * Returned publisher and given {@link RxTransactionWork} is completed/executed by an IO thread which should never block.
     * Otherwise IO operations on this and potentially other network connections might deadlock.
     * Please do not chain blocking operations like {@link CompletableFuture#get()} on the returned publisher and do not use them inside the
     * {@link RxTransactionWork}.
     *
     * @param work the {@link RxTransactionWork} to be applied to a new read transaction.
     * Operation executed by the given work must NOT include any blocking operation.
     * @param <T> the return type of the given unit of work.
     * @return a {@link Publisher publisher} completed with the same result as returned by the given unit of work.
     * publisher can be completed exceptionally if given work or commit fails.
     *
     */
    <T> Publisher<T> readTransaction( RxTransactionWork<? extends Publisher<T>> work );

    /**
     * Execute given unit of reactive work in a {@link AccessMode#READ read} reactive transaction with
     * the specified {@link TransactionConfig configuration}.
     <p>
     * Transaction will automatically be committed unless given unit of work fails or
     * {@link RxTransaction#commit() transaction commit} fails.
     * It will also not be committed if explicitly rolled back via {@link RxTransaction#rollback()}.
     * <p>
     * Returned publisher and given {@link RxTransactionWork} is completed/executed by an IO thread which should never block.
     * Otherwise IO operations on this and potentially other network connections might deadlock.
     * Please do not chain blocking operations like {@link CompletableFuture#get()} on the returned publisher and do not use them inside the
     * {@link RxTransactionWork}.
     *
     * @param work the {@link RxTransactionWork} to be applied to a new read transaction.
     * Operation executed by the given work must NOT include any blocking operation.
     * @param config the transaction configuration.
     * @param <T> the return type of the given unit of work.
     * @return a {@link Publisher publisher} completed with the same result as returned by the given unit of work.
     * publisher can be completed exceptionally if given work or commit fails.
     *
     */
    <T> Publisher<T> readTransaction( RxTransactionWork<? extends Publisher<T>> work, TransactionConfig config );

    /**
     * Execute given unit of reactive work in a {@link AccessMode#WRITE write} reactive transaction.
     <p>
     * Transaction will automatically be committed unless given unit of work fails or
     * {@link RxTransaction#commit() transaction commit} fails.
     * It will also not be committed if explicitly rolled back via {@link RxTransaction#rollback()}.
     * <p>
     * Returned publisher and given {@link RxTransactionWork} is completed/executed by an IO thread which should never block.
     * Otherwise IO operations on this and potentially other network connections might deadlock.
     * Please do not chain blocking operations like {@link CompletableFuture#get()} on the returned publisher and do not use them inside the
     * {@link RxTransactionWork}.
     *
     * @param work the {@link RxTransactionWork} to be applied to a new read transaction.
     * Operation executed by the given work must NOT include any blocking operation.
     * @param <T> the return type of the given unit of work.
     * @return a {@link Publisher publisher} completed with the same result as returned by the given unit of work.
     * publisher can be completed exceptionally if given work or commit fails.
     *
     */
    <T> Publisher<T> writeTransaction( RxTransactionWork<? extends Publisher<T>> work );

    /**
     * Execute given unit of reactive work in a {@link AccessMode#WRITE write} reactive transaction with
     * the specified {@link TransactionConfig configuration}.
     <p>
     * Transaction will automatically be committed unless given unit of work fails or
     * {@link RxTransaction#commit() transaction commit} fails.
     * It will also not be committed if explicitly rolled back via {@link RxTransaction#rollback()}.
     * <p>
     * Returned publisher and given {@link RxTransactionWork} is completed/executed by an IO thread which should never block.
     * Otherwise IO operations on this and potentially other network connections might deadlock.
     * Please do not chain blocking operations like {@link CompletableFuture#get()} on the returned publisher and do not use them inside the
     * {@link RxTransactionWork}.
     *
     * @param work the {@link RxTransactionWork} to be applied to a new read transaction.
     * Operation executed by the given work must NOT include any blocking operation.
     * @param config the transaction configuration.
     * @param <T> the return type of the given unit of work.
     * @return a {@link Publisher publisher} completed with the same result as returned by the given unit of work.
     * publisher can be completed exceptionally if given work or commit fails.
     *
     */
    <T> Publisher<T> writeTransaction( RxTransactionWork<? extends Publisher<T>> work, TransactionConfig config );

    /**
     * Run a query with parameters in an auto-commit transaction with specified {@link TransactionConfig} and return a reactive result stream.
     * The query is not executed when the reactive result is returned.
     * Instead, the publishers in the result will actually start the execution of the query.
     *
     * @param query text of a Neo4j query.
     * @param config configuration for the new transaction.
     * @return a reactive result.
     */
    RxResult run(String query, TransactionConfig config );

    /**
     * Run a query with parameters in an auto-commit transaction with specified {@link TransactionConfig} and return a reactive result stream.
     * The query is not executed when the reactive result is returned.
     * Instead, the publishers in the result will actually start the execution of the query.
     * <p>
     * This method takes a set of parameters that will be injected into the query by Neo4j.
     * Using parameters is highly encouraged, it helps avoid dangerous cypher injection attacks
     * and improves database performance as Neo4j can re-use query plans more often.
     * <p>
     * This version of run takes a {@link Map} of parameters.
     * The values in the map must be values that can be converted to Neo4j types.
     * See {@link Values#parameters(Object...)} for a list of allowed types.
     *
     * <h2>Example</h2>
     * <pre>
     * {@code
     * Map<String, Object> metadata = new HashMap<>();
     * metadata.put("type", "update name");
     *
     * TransactionConfig config = TransactionConfig.builder()
     *                 .withTimeout(Duration.ofSeconds(3))
     *                 .withMetadata(metadata)
     *                 .build();
     *
     * Map<String, Object> parameters = new HashMap<>();
     * parameters.put("myNameParam", "Bob");
     *
     * RxResult result = rxSession.run("MATCH (n) WHERE n.name = $myNameParam RETURN (n)", parameters, config);
     * }
     * </pre>
     *
     * @param query text of a Neo4j query.
     * @param parameters input data for the query.
     * @param config configuration for the new transaction.
     * @return a reactive result.
     */
    RxResult run(String query, Map<String,Object> parameters, TransactionConfig config );

    /**
     * Run a query in an auto-commit transaction with specified {@link TransactionConfig configuration} and return a reactive result stream.
     * The query is not executed when the reactive result is returned.
     * Instead, the publishers in the result will actually start the execution of the query.
     * <h2>Example</h2>
     * <pre>
     * {@code
     * Map<String, Object> metadata = new HashMap<>();
     * metadata.put("type", "update name");
     *
     * TransactionConfig config = TransactionConfig.builder()
     *                 .withTimeout(Duration.ofSeconds(3))
     *                 .withMetadata(metadata)
     *                 .build();
     *
     * Query query = new Query("MATCH (n) WHERE n.name = $myNameParam RETURN n.age");
     * RxResult result = rxSession.run(query.withParameters(Values.parameters("myNameParam", "Bob")));
     * }
     * </pre>
     *
     * @param query a Neo4j query.
     * @param config configuration for the new transaction.
     * @return a reactive result.
     */
    RxResult run(Query query, TransactionConfig config );

    /**
     * Return the bookmark received following the last completed query within this session.
     * The last completed query can be run in a {@linkplain RxTransaction transaction}
     * started using {@linkplain #beginTransaction() beginTransaction} or directly via {@link #run(Query) run}.
     *
     * @return a reference to a previous transaction.
     */
    Bookmark lastBookmark();

    /**
     * Signal that you are done using this session.
     * In the default driver usage, closing and accessing sessions is very low cost.
     * <p>
     * This operation is not needed if 1) all results created in the session have been fully consumed and
     * 2) all transactions opened by this session have been either committed or rolled back.
     * <p>
     * This method is a fallback if you failed to fulfill the two requirements above.
     * This publisher is completed when all outstanding queries in the session have completed,
     * meaning any writes you performed are guaranteed to be durably stored.
     * It might be completed exceptionally when there are unconsumed errors from previous queries or transactions.
     *
     * @param <T> makes it easier to be chained.
     * @return an empty publisher that represents the reactive close.
     */
    <T> Publisher<T> close();
}
