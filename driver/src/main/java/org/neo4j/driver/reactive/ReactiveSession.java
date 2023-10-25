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
package org.neo4j.driver.reactive;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow.Publisher;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.BaseSession;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Query;
import org.neo4j.driver.Session;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.Values;

/**
 * A reactive session is the same as {@link Session} except it provides a reactive API.
 *
 * @see Session
 * @see ReactiveResult
 * @see ReactiveTransaction
 * @see Publisher
 * @since 5.0
 */
public interface ReactiveSession extends BaseSession, ReactiveQueryRunner {
    /**
     * Begin a new <em>unmanaged {@linkplain ReactiveTransaction transaction}</em>. At most one transaction may exist in a session at any point in time. To
     * maintain multiple concurrent transactions, use multiple concurrent sessions.
     * <p>
     * It by default is executed in a Network IO thread, as a result no blocking operation is allowed in this thread.
     *
     * @return a new {@link ReactiveTransaction}
     */
    default Publisher<ReactiveTransaction> beginTransaction() {
        return beginTransaction(TransactionConfig.empty());
    }

    /**
     * Begin a new <em>unmanaged {@linkplain ReactiveTransaction transaction}</em> with the specified {@link TransactionConfig configuration}. At most one
     * transaction may exist in a session at any point in time. To maintain multiple concurrent transactions, use multiple concurrent sessions.
     * <p>
     * It by default is executed in a Network IO thread, as a result no blocking operation is allowed in this thread.
     *
     * @param config configuration for the new transaction.
     * @return a new {@link ReactiveTransaction}
     */
    Publisher<ReactiveTransaction> beginTransaction(TransactionConfig config);

    /**
     * Execute a unit of work as a single, managed transaction with {@link AccessMode#READ read} access mode and retry behaviour. The transaction allows for one
     * or more statements to be run.
     * <p>
     * The driver will attempt committing the transaction when the provided unit of work completes successfully. Any exception emitted by the unit of work will
     * result in a rollback attempt and abortion of execution unless exception is considered to be valid for retry attempt by the driver.
     * <p>
     * The provided unit of work should not return {@link ReactiveResult} object as it won't be valid outside the scope of the transaction.
     * <p>
     * It is prohibited to block the thread completing the returned {@link CompletionStage}. Please avoid blocking operations or hand processing over to a
     * different thread.
     * <p>
     * The driver uses the provided {@link ReactiveTransactionCallback} to get a publisher and emits its
     * signals via the resulting publisher. If the supplied publisher emits a
     * {@link org.neo4j.driver.exceptions.RetryableException} and the driver is in a position to retry, it calls the
     * provided callback again to get a new publisher and attempts to stream its signals. In case of retries, the
     * resulting publisher contains the successfully emitted values from all retry attempts. For instance, if a
     * retryable exception occurs after streaming values [v1, v2, v3] and a successful retry emits values [v1, v2, v3,
     * v4] then the resulting publisher emits the following values: [v1, v2, v3, v1, v2, v3, v4].
     *
     * @param callback the callback representing the unit of work.
     * @param <T>      the return type of the given unit of work.
     * @return a publisher that emits the result of the unit of work and success signals on success or error otherwise.
     */
    default <T> Publisher<T> executeRead(ReactiveTransactionCallback<? extends Publisher<T>> callback) {
        return executeRead(callback, TransactionConfig.empty());
    }

    /**
     * Execute a unit of work as a single, managed transaction with {@link AccessMode#READ read} access mode and retry behaviour. The transaction allows for one
     * or more statements to be run.
     * <p>
     * The driver will attempt committing the transaction when the provided unit of work completes successfully. Any exception emitted by the unit of work will
     * result in a rollback attempt and abortion of execution unless exception is considered to be valid for retry attempt by the driver.
     * <p>
     * The provided unit of work should not return {@link ReactiveResult} object as it won't be valid outside the scope of the transaction.
     * <p>
     * It is prohibited to block the thread completing the returned {@link CompletionStage}. Please avoid blocking operations or hand processing over to a
     * different thread.
     * <p>
     * The driver uses the provided {@link ReactiveTransactionCallback} to get a publisher and emits its
     * signals via the resulting publisher. If the supplied publisher emits a
     * {@link org.neo4j.driver.exceptions.RetryableException} and the driver is in a position to retry, it calls the
     * provided callback again to get a new publisher and attempts to stream its signals. In case of retries, the
     * resulting publisher contains the successfully emitted values from all retry attempts. For instance, if a
     * retryable exception occurs after streaming values [v1, v2, v3] and a successful retry emits values [v1, v2, v3,
     * v4] then the resulting publisher emits the following values: [v1, v2, v3, v1, v2, v3, v4].
     *
     * @param callback the callback representing the unit of work.
     * @param config   configuration for all transactions started to execute the unit of work.
     * @param <T>      the return type of the given unit of work.
     * @return a publisher that emits the result of the unit of work and success signals on success or error otherwise.
     */
    <T> Publisher<T> executeRead(
            ReactiveTransactionCallback<? extends Publisher<T>> callback, TransactionConfig config);

    /**
     * Execute a unit of work as a single, managed transaction with {@link AccessMode#WRITE write} access mode and retry behaviour. The transaction allows for
     * one or more statements to be run.
     * <p>
     * The driver will attempt committing the transaction when the provided unit of work completes successfully. Any exception emitted by the unit of work will
     * result in a rollback attempt and abortion of execution unless exception is considered to be valid for retry attempt by the driver.
     * <p>
     * The provided unit of work should not return {@link ReactiveResult} object as it won't be valid outside the scope of the transaction.
     * <p>
     * It is prohibited to block the thread completing the returned {@link CompletionStage}. Please avoid blocking operations or hand processing over to a
     * different thread.
     * <p>
     * The driver uses the provided {@link ReactiveTransactionCallback} to get a publisher and emits its
     * signals via the resulting publisher. If the supplied publisher emits a
     * {@link org.neo4j.driver.exceptions.RetryableException} and the driver is in a position to retry, it calls the
     * provided callback again to get a new publisher and attempts to stream its signals. In case of retries, the
     * resulting publisher contains the successfully emitted values from all retry attempts. For instance, if a
     * retryable exception occurs after streaming values [v1, v2, v3] and a successful retry emits values [v1, v2, v3,
     * v4] then the resulting publisher emits the following values: [v1, v2, v3, v1, v2, v3, v4].
     *
     * @param callback the callback representing the unit of work.
     * @param <T>      the return type of the given unit of work.
     * @return a publisher that emits the result of the unit of work and success signals on success or error otherwise.
     */
    default <T> Publisher<T> executeWrite(ReactiveTransactionCallback<? extends Publisher<T>> callback) {
        return executeWrite(callback, TransactionConfig.empty());
    }

    /**
     * Execute a unit of work as a single, managed transaction with {@link AccessMode#WRITE write} access mode and retry behaviour. The transaction allows for
     * one or more statements to be run.
     * <p>
     * The driver will attempt committing the transaction when the provided unit of work completes successfully. Any exception emitted by the unit of work will
     * result in a rollback attempt and abortion of execution unless exception is considered to be valid for retry attempt by the driver.
     * <p>
     * The provided unit of work should not return {@link ReactiveResult} object as it won't be valid outside the scope of the transaction.
     * <p>
     * It is prohibited to block the thread completing the returned {@link CompletionStage}. Please avoid blocking operations or hand processing over to a
     * different thread.
     * <p>
     * The driver uses the provided {@link ReactiveTransactionCallback} to get a publisher and emits its
     * signals via the resulting publisher. If the supplied publisher emits a
     * {@link org.neo4j.driver.exceptions.RetryableException} and the driver is in a position to retry, it calls the
     * provided callback again to get a new publisher and attempts to stream its signals. In case of retries, the
     * resulting publisher contains the successfully emitted values from all retry attempts. For instance, if a
     * retryable exception occurs after streaming values [v1, v2, v3] and a successful retry emits values [v1, v2, v3,
     * v4] then the resulting publisher emits the following values: [v1, v2, v3, v1, v2, v3, v4].
     *
     * @param callback the callback representing the unit of work.
     * @param config   configuration for all transactions started to execute the unit of work.
     * @param <T>      the return type of the given unit of work.
     * @return a publisher that emits the result of the unit of work and success signals on success or error otherwise.
     */
    <T> Publisher<T> executeWrite(
            ReactiveTransactionCallback<? extends Publisher<T>> callback, TransactionConfig config);

    /**
     * Run a query with parameters in an auto-commit transaction with specified {@link TransactionConfig} and return a publisher of {@link ReactiveResult}.
     * <p>
     * Invoking this method will result in a Bolt RUN message exchange with server and the returned publisher will either emit an instance of {@link
     * ReactiveResult} on success or an error otherwise.
     *
     * @param query  text of a Neo4j query.
     * @param config configuration for the new transaction.
     * @return a publisher of reactive result.
     */
    default Publisher<ReactiveResult> run(String query, TransactionConfig config) {
        return run(new Query(query), config);
    }

    /**
     * Run a query with parameters in an auto-commit transaction with specified {@link TransactionConfig} and return a publisher of {@link ReactiveResult}.
     * <p>
     * Invoking this method will result in a Bolt RUN message exchange with server and the returned publisher will either emit an instance of {@link ReactiveResult} on success or an error otherwise.
     * <p>
     * This method takes a set of parameters that will be injected into the query by Neo4j. Using parameters is highly encouraged, it helps avoid dangerous
     * cypher injection attacks and improves database performance as Neo4j can re-use query plans more often.
     * <p>
     * This version of run takes a {@link Map} of parameters. The values in the map must be values that can be converted to Neo4j types. See {@link
     * Values#parameters(Object...)} for a list of allowed types.
     * <h4>Example</h4>
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
     * reactiveSession.run("MATCH (n) WHERE n.name = $myNameParam RETURN (n)", parameters, config);
     * }
     * </pre>
     *
     * @param query      text of a Neo4j query.
     * @param parameters input data for the query.
     * @param config     configuration for the new transaction.
     * @return a publisher of reactive result.
     */
    default Publisher<ReactiveResult> run(String query, Map<String, Object> parameters, TransactionConfig config) {
        return run(new Query(query, parameters), config);
    }

    /**
     * Run a query in an auto-commit transaction with specified {@link TransactionConfig configuration} and return a publisher of {@link ReactiveResult}.
     * <p>
     * Invoking this method will result in a Bolt RUN message exchange with server and the returned publisher will either emit an instance of {@link
     * ReactiveResult} on success or an error otherwise.
     * <h4>Example</h4>
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
     *
     * reactiveSession.run(query.withParameters(Values.parameters("myNameParam", "Bob")));
     * }
     * </pre>
     *
     * @param query  a Neo4j query.
     * @param config configuration for the new transaction.
     * @return a publisher of reactive result.
     */
    Publisher<ReactiveResult> run(Query query, TransactionConfig config);

    /**
     * Return a set of last bookmarks.
     * <p>
     * When no new bookmark is received, the initial bookmarks are returned. This may happen when no work has been done using the session. Multivalued {@link
     * Bookmark} instances will be mapped to distinct {@link Bookmark} instances. If no initial bookmarks have been provided, an empty set is returned.
     *
     * @return the immutable set of last bookmarks.
     */
    Set<Bookmark> lastBookmarks();

    /**
     * Signal that you are done using this session. In the default driver usage, closing and accessing sessions is very low cost.
     * <p>
     * This operation is not needed if 1) all results created in the session have been fully consumed and 2) all transactions opened by this session have been
     * either committed or rolled back.
     * <p>
     * This method is a fallback if you failed to fulfill the two requirements above. This publisher is completed when all outstanding queries in the session
     * have completed, meaning any writes you performed are guaranteed to be durably stored. It might be completed exceptionally when there are unconsumed
     * errors from previous queries or transactions.
     *
     * @param <T> makes it easier to be chained.
     * @return an empty publisher that represents the reactive close.
     */
    <T> Publisher<T> close();
}
