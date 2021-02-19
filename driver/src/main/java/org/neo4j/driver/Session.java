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
package org.neo4j.driver;

import java.util.Map;

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
public interface Session extends Resource, QueryRunner
{
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
    Transaction beginTransaction( TransactionConfig config );

    /**
     * Execute a unit of work in a managed {@link AccessMode#READ read} transaction.
     * <p>
     * This transaction will automatically be committed unless an exception is
     * thrown during query execution or by the user code.
     * <p>
     * Managed transactions should not generally be explicitly committed (via
     * {@link Transaction#commit()}).
     *
     * @param work the {@link TransactionWork} to be applied to a new read transaction.
     * @param <T> the return type of the given unit of work.
     * @return a result as returned by the given unit of work.
     */
    <T> T readTransaction( TransactionWork<T> work );

    /**
     * Execute a unit of work in a managed {@link AccessMode#READ read} transaction
     * with the specified {@link TransactionConfig configuration}.
     * <p>
     * This transaction will automatically be committed unless an exception is
     * thrown during query execution or by the user code.
     * <p>
     * Managed transactions should not generally be explicitly committed (via
     * {@link Transaction#commit()}).
     *
     * @param work the {@link TransactionWork} to be applied to a new read transaction.
     * @param config configuration for all transactions started to execute the unit of work.
     * @param <T> the return type of the given unit of work.
     * @return a result as returned by the given unit of work.
     */
    <T> T readTransaction( TransactionWork<T> work, TransactionConfig config );

    /**
     * Execute a unit of work in a managed {@link AccessMode#WRITE write} transaction.
     * <p>
     * This transaction will automatically be committed unless an exception is
     * thrown during query execution or by the user code.
     * <p>
     * Managed transactions should not generally be explicitly committed (via
     * {@link Transaction#commit()}).
     *
     * @param work the {@link TransactionWork} to be applied to a new write transaction.
     * @param <T> the return type of the given unit of work.
     * @return a result as returned by the given unit of work.
     */
    <T> T writeTransaction( TransactionWork<T> work );

    /**
     * Execute a unit of work in a managed {@link AccessMode#WRITE write} transaction
     * with the specified {@link TransactionConfig configuration}.
     * <p>
     * This transaction will automatically be committed unless an exception is
     * thrown during query execution or by the user code.
     * <p>
     * Managed transactions should not generally be explicitly committed (via
     * {@link Transaction#commit()}).
     *
     * @param work the {@link TransactionWork} to be applied to a new write transaction.
     * @param config configuration for all transactions started to execute the unit of work.
     * @param <T> the return type of the given unit of work.
     * @return a result as returned by the given unit of work.
     */
    <T> T writeTransaction( TransactionWork<T> work, TransactionConfig config );

    /**
     * Run a query in a managed auto-commit transaction with the specified
     * {@link TransactionConfig configuration}, and return a result stream.
     *
     * @param query text of a Neo4j query.
     * @param config configuration for the new transaction.
     * @return a stream of result values and associated metadata.
     */
    Result run(String query, TransactionConfig config );

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
     * Result result = session.run("MATCH (n) WHERE n.name = $myNameParam RETURN (n)", parameters, config);
     * }
     * </pre>
     *
     * @param query text of a Neo4j query.
     * @param parameters input data for the query.
     * @param config configuration for the new transaction.
     * @return a stream of result values and associated metadata.
     */
    Result run(String query, Map<String,Object> parameters, TransactionConfig config );

    /**
     * Run a query in a managed auto-commit transaction with the specified
     * {@link TransactionConfig configuration}, and return a result stream.
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
     * Result result = session.run(query.withParameters(Values.parameters("myNameParam", "Bob")));
     * }
     * </pre>
     *
     * @param query a Neo4j query.
     * @param config configuration for the new transaction.
     * @return a stream of result values and associated metadata.
     */
    Result run(Query query, TransactionConfig config );

    /**
     * Return the bookmark received following the last completed
     * {@linkplain Transaction transaction}. If no bookmark was received
     * or if this transaction was rolled back, the bookmark value will
     * be null.
     *
     * @return a reference to a previous transaction
     */
    Bookmark lastBookmark();

    /**
     * Reset the current session. This sends an immediate RESET signal to the server which both interrupts
     * any query that is currently executing and ignores any subsequently queued queries. Following
     * the reset, the current transaction will have been rolled back and any outstanding failures will
     * have been acknowledged.
     *
     * @deprecated This method should not be used and violates the expected usage pattern of {@link Session} objects.
     * They are expected to be not thread-safe and should not be shared between thread. However this method is only
     * useful when {@link Session} object is passed to another monitoring thread that calls it when appropriate.
     * It is not useful when {@link Session} is used in a single thread because in this case {@link #close()}
     * can be used. Since version 3.1, Neo4j database allows users to specify maximum transaction execution time and
     * contains procedures to list and terminate running queries. These functions should be used instead of calling
     * this method.
     */
    @Deprecated
    void reset();

    /**
     * Signal that you are done using this session. In the default driver usage, closing and accessing sessions is
     * very low cost.
     */
    @Override
    void close();
}
