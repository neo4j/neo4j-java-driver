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
package org.neo4j.driver;

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.util.Resource;

/**
 * Provides a context of work for database interactions.
 * <p>
 * A <em>Session</em> is a logical container for a causally chained series of
 * {@linkplain Transaction transactions}. Client applications typically work
 * with <em>managed transactions</em>, which come in two flavours: transaction
 * functions and auto-commit transactions. Managed transactions automatically
 * handle the transaction boundaries (<code>BEGIN</code> and
 * <code>COMMIT</code>/<code>ROLLBACK</code>) and can also provide other
 * supporting features; transaction functions offer retry capabilities, for
 * example.
 * <p>
 * Unmanaged transactions are also available but tend to be used by libraries
 * or tooling that require more fine-grained control.
 * <p>
 * Typically, a session will acquire a TCP connection from a connection pool
 * in order to carry out a transaction. Once the transaction has completed,
 * and the entire result has been consumed, this connection will be released
 * back into the pool. One connection can therefore be adopted by several
 * sessions over its lifetime, although it will only be owned by one at a
 * time. Client applications should never need to deal directly with
 * connection management.
 * <p>
 * Session implementations are not generally thread-safe. Therefore, multiple
 * sessions should be used when an application requires multiple concurrent
 * threads of database work to be carried out.
 *
 * @since 1.0 (Removed async API to {@link AsyncSession} in 4.0)
 */
public interface Session extends BaseSession, Resource, QueryRunner {
    /**
     * Begin a new <em>unmanaged {@linkplain Transaction transaction}</em>. At
     * most one transaction may exist in a session at any point in time. To
     * maintain multiple concurrent transactions, use multiple concurrent
     * sessions.
     *
     * @return a new {@link Transaction}
     */
    Transaction beginTransaction();

    /**
     * Begin a new <em>unmanaged {@linkplain Transaction transaction}</em> with
     * the specified {@link TransactionConfig configuration}. At most one
     * transaction may exist in a session at any point in time. To maintain
     * multiple concurrent transactions, use multiple concurrent sessions.
     *
     * @param config configuration for the new transaction.
     * @return a new {@link Transaction}
     */
    Transaction beginTransaction(TransactionConfig config);

    /**
     * Execute a unit of work as a single, managed transaction with {@link AccessMode#READ read} access mode and retry behaviour. The transaction allows for one
     * or more statements to be run.
     * <p>
     * The driver will attempt committing the transaction when the provided unit of work completes successfully. Any exception emitted by the unit of work will
     * result in a rollback attempt and abortion of execution unless exception is considered to be valid for retry attempt by the driver.
     * <p>
     * The provided unit of work should not return {@link Result} object as it won't be valid outside the scope of the transaction.
     *
     * @param callback the callback representing the unit of work.
     * @param <T>      the return type of the given unit of work.
     * @return a result as returned by the given unit of work.
     */
    default <T> T executeRead(TransactionCallback<T> callback) {
        return executeRead(callback, TransactionConfig.empty());
    }

    /**
     * Execute a unit of work as a single, managed transaction with {@link AccessMode#READ read} access mode and retry behaviour. The transaction allows for one
     * or more statements to be run.
     * <p>
     * The driver will attempt committing the transaction when the provided unit of work completes successfully. Any exception emitted by the unit of work will
     * result in a rollback attempt and abortion of execution unless exception is considered to be valid for retry attempt by the driver.
     * <p>
     * The provided unit of work should not return {@link Result} object as it won't be valid outside the scope of the transaction.
     *
     * @param callback the callback representing the unit of work.
     * @param config   the transaction configuration for the managed transaction.
     * @param <T>      the return type of the given unit of work.
     * @return a result as returned by the given unit of work.
     */
    <T> T executeRead(TransactionCallback<T> callback, TransactionConfig config);

    /**
     * Execute a unit of work as a single, managed transaction with {@link AccessMode#WRITE write} access mode and retry behaviour. The transaction allows for
     * one or more statements to be run.
     * <p>
     * The driver will attempt committing the transaction when the provided unit of work completes successfully. Any exception emitted by the unit of work will
     * result in a rollback attempt and abortion of execution unless exception is considered to be valid for retry attempt by the driver.
     * <p>
     * The provided unit of work should not return {@link Result} object as it won't be valid outside the scope of the transaction.
     *
     * @param callback the callback representing the unit of work.
     * @param <T>      the return type of the given unit of work.
     * @return a result as returned by the given unit of work.
     */
    default <T> T executeWrite(TransactionCallback<T> callback) {
        return executeWrite(callback, TransactionConfig.empty());
    }

    /**
     * Execute a unit of work as a single, managed transaction with {@link AccessMode#WRITE write} access mode and retry behaviour. The transaction allows for one or more statements to be run.
     * <p>
     * The driver will attempt committing the transaction when the provided unit of work completes successfully. Any exception emitted by the unit of work
     * will result in a rollback attempt.
     * <p>
     * This method works equivalently to {@link #executeWrite(TransactionCallback)}, but does not have a return value.
     *
     * @param contextConsumer the consumer representing the unit of work.
     */
    default void executeWriteWithoutResult(Consumer<TransactionContext> contextConsumer) {
        executeWrite(tc -> {
            contextConsumer.accept(tc);
            return null;
        });
    }

    /**
     * Execute a unit of work as a single, managed transaction with {@link AccessMode#WRITE write} access mode and retry behaviour. The transaction allows for one or more statements to be run.
     * <p>
     * The driver will attempt committing the transaction when the provided unit of work completes successfully. Any exception emitted by the unit of work
     * will result in a rollback attempt and abortion of execution unless exception is considered to be valid for retry attempt by the driver.
     * <p>
     * The provided unit of work should not return {@link Result} object as it won't be valid outside the scope of the transaction.
     *
     * @param callback the callback representing the unit of work.
     * @param config   the transaction configuration for the managed transaction.
     * @param <T>      the return type of the given unit of work.
     * @return a result as returned by the given unit of work.
     */
    <T> T executeWrite(TransactionCallback<T> callback, TransactionConfig config);

    /**
     * Execute a unit of work as a single, managed transaction with {@link AccessMode#WRITE write} access mode and retry behaviour. The transaction allows for one or more statements to be run.
     * <p>
     * The driver will attempt committing the transaction when the provided unit of work completes successfully. Any exception emitted by the unit of work
     * will result in a rollback attempt and abortion of execution unless exception is considered to be valid for retry attempt by the driver.
     * <p>
     * This method works equivalently to {@link #executeWrite(TransactionCallback, TransactionConfig)}, but does not have a return value.
     *
     * @param contextConsumer the consumer representing the unit of work.
     * @param config          the transaction configuration for the managed transaction.
     */
    default void executeWriteWithoutResult(Consumer<TransactionContext> contextConsumer, TransactionConfig config) {
        executeWrite(
                tc -> {
                    contextConsumer.accept(tc);
                    return null;
                },
                config);
    }

    /**
     * Run a query in a managed auto-commit transaction with the specified {@link TransactionConfig configuration}, and return a result stream.
     *
     * @param query  text of a Neo4j query.
     * @param config configuration for the new transaction.
     * @return a stream of result values and associated metadata.
     */
    Result run(String query, TransactionConfig config);

    /**
     * Run a query with parameters in a managed auto-commit transaction with the
     * specified {@link TransactionConfig configuration}, and return a result stream.
     * <p>
     * This method takes a set of parameters that will be injected into the
     * query by Neo4j. Using parameters is highly encouraged, it helps avoid
     * dangerous cypher injection attacks and improves database performance as
     * Neo4j can re-use query plans more often.
     * <p>
     * This version of run takes a {@link Map} of parameters. The values in the map
     * must be values that can be converted to Neo4j types. See {@link Values#parameters(Object...)} for
     * a list of allowed types.
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
     * Result result = session.run("MATCH (n) WHERE n.name = $myNameParam RETURN (n)", parameters, config);
     * }
     * </pre>
     *
     * @param query text of a Neo4j query.
     * @param parameters input data for the query.
     * @param config configuration for the new transaction.
     * @return a stream of result values and associated metadata.
     */
    Result run(String query, Map<String, Object> parameters, TransactionConfig config);

    /**
     * Run a query in a managed auto-commit transaction with the specified
     * {@link TransactionConfig configuration}, and return a result stream.
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
     * Result result = session.run(query.withParameters(Values.parameters("myNameParam", "Bob")));
     * }
     * </pre>
     *
     * @param query a Neo4j query.
     * @param config configuration for the new transaction.
     * @return a stream of result values and associated metadata.
     */
    Result run(Query query, TransactionConfig config);

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
     */
    @Override
    void close();
}
