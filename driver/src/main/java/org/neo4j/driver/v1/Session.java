/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.driver.v1;

import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Function;

import org.neo4j.driver.v1.util.Resource;

/**
 * Provides a context of work for database interactions.
 * <p>
 * A <em>Session</em> hosts a series of {@linkplain Transaction transactions}
 * carried out against a database. Within the database, all statements are
 * carried out within a transaction. Within application code, however, it is
 * not always necessary to explicitly {@link #beginTransaction() begin a
 * transaction}. If a statement is {@link #run} directly against a {@link
 * Session}, the server will automatically <code>BEGIN</code> and
 * <code>COMMIT</code> that statement within its own transaction. This type
 * of transaction is known as an <em>autocommit transaction</em>.
 * <p>
 * Explicit transactions allow multiple statements to be committed as part of
 * a single atomic operation and can be rolled back if necessary. They can also
 * be used to ensure <em>causal consistency</em>, meaning that an application
 * can run a series of queries on different members of a cluster, while
 * ensuring that each query sees the state of graph at least as up-to-date as
 * the graph seen by the previous query. For more on causal consistency, see
 * the Neo4j clustering manual.
 * <p>
 * Typically, a session will acquire a TCP connection to execute query or
 * transaction. Such a connection will be acquired from a connection pool
 * and released back there when query result is consumed or transaction is
 * committed or rolled back. One connection can therefore be adopted by many
 * sessions, although by only one at a time. Application code should never need
 * to deal directly with connection management.
 * <p>
 * A session inherits its destination address and permissions from its
 * underlying connection. This means that for a single query/transaction one
 * session may only ever target one machine within a cluster and does not
 * support re-authentication. To achieve otherwise requires creation of a
 * separate session.
 * <p>
 * Similarly, multiple sessions should be used when working with concurrency;
 * session implementations are not thread safe.
 *
 * @since 1.0
 */
public interface Session extends Resource, StatementRunner
{
    /**
     * Begin a new <em>explicit {@linkplain Transaction transaction}</em>. At
     * most one transaction may exist in a session at any point in time. To
     * maintain multiple concurrent transactions, use multiple concurrent
     * sessions.
     * <p>
     * This operation works the same way as {@link #beginTransactionAsync()} but blocks until transaction is actually
     * started.
     *
     * @return a new {@link Transaction}
     */
    Transaction beginTransaction();

    Transaction beginTransaction( TransactionConfig config );

    /**
     * Begin a new <em>explicit {@linkplain Transaction transaction}</em>,
     * requiring that the server hosting is at least as up-to-date as the
     * transaction referenced by the supplied <em>bookmark</em>.
     *
     * @param bookmark a reference to a previous transaction
     * @return a new {@link Transaction}
     * @deprecated This method is deprecated in favour of {@link Driver#session(Iterable)} that accepts an initial
     * bookmark. Session will ensure that all nested transactions are chained with bookmarks to guarantee
     * causal consistency. <b>This method will be removed in the next major release.</b>
     */
    @Deprecated
    Transaction beginTransaction( String bookmark );

    /**
     * Begin a new <em>explicit {@linkplain Transaction transaction}</em>. At
     * most one transaction may exist in a session at any point in time. To
     * maintain multiple concurrent transactions, use multiple concurrent
     * sessions.
     * <p>
     * This operation is asynchronous and returns a {@link CompletionStage}. This stage is completed with a new
     * {@link Transaction} object when begin operation is successful. It is completed exceptionally if
     * transaction can't be started.
     * <p>
     * Returned stage can be completed by an IO thread which should never block. Otherwise IO operations on this and
     * potentially other network connections might deadlock. Please do not chain blocking operations like
     * {@link #run(String)} on the returned stage. Driver will throw {@link IllegalStateException} when blocking API
     * call is executed in IO thread. Consider using asynchronous calls throughout the chain or offloading blocking
     * operation to a different {@link Executor}. This can be done using methods with "Async" suffix like
     * {@link CompletionStage#thenApplyAsync(Function)} or {@link CompletionStage#thenApplyAsync(Function, Executor)}.
     *
     * @return a {@link CompletionStage completion stage} that represents the asynchronous begin of a transaction.
     */
    CompletionStage<Transaction> beginTransactionAsync();

    CompletionStage<Transaction> beginTransactionAsync( TransactionConfig config );

    /**
     * Execute given unit of work in a  {@link AccessMode#READ read} transaction.
     * <p>
     * Transaction will automatically be committed unless exception is thrown from the unit of work itself or from
     * {@link Transaction#close()} or transaction is explicitly marked for failure via {@link Transaction#failure()}.
     * <p>
     * This operation works the same way as {@link #readTransactionAsync(TransactionWork)} but blocks until given
     * blocking unit of work is completed.
     *
     * @param work the {@link TransactionWork} to be applied to a new read transaction.
     * @param <T> the return type of the given unit of work.
     * @return a result as returned by the given unit of work.
     */
    <T> T readTransaction( TransactionWork<T> work );

    <T> T readTransaction( TransactionWork<T> work, TransactionConfig config );

    /**
     * Execute given unit of asynchronous work in a  {@link AccessMode#READ read} asynchronous transaction.
     * <p>
     * Transaction will automatically be committed unless given unit of work fails or
     * {@link Transaction#commitAsync() async transaction commit} fails. It will also not be committed if explicitly
     * rolled back via {@link Transaction#rollbackAsync()}.
     * <p>
     * Returned stage and given {@link TransactionWork} can be completed/executed by an IO thread which should never
     * block. Otherwise IO operations on this and potentially other network connections might deadlock. Please do not
     * chain blocking operations like {@link #run(String)} on the returned stage and do not use them inside the
     * {@link TransactionWork}. Driver will throw {@link IllegalStateException} when blocking API
     * call is executed in IO thread. Consider using asynchronous calls throughout the chain or offloading blocking
     * operation to a different {@link Executor}. This can be done using methods with "Async" suffix like
     * {@link CompletionStage#thenApplyAsync(Function)} or {@link CompletionStage#thenApplyAsync(Function, Executor)}.
     *
     * @param work the {@link TransactionWork} to be applied to a new read transaction. Operation executed by the
     * given work must be asynchronous.
     * @param <T> the return type of the given unit of work.
     * @return a {@link CompletionStage completion stage} completed with the same result as returned by the given
     * unit of work. Stage can be completed exceptionally if given work or commit fails.
     */
    <T> CompletionStage<T> readTransactionAsync( TransactionWork<CompletionStage<T>> work );

    <T> CompletionStage<T> readTransactionAsync( TransactionWork<CompletionStage<T>> work, TransactionConfig config );

    /**
     * Execute given unit of work in a  {@link AccessMode#WRITE write} transaction.
     * <p>
     * Transaction will automatically be committed unless exception is thrown from the unit of work itself or from
     * {@link Transaction#close()} or transaction is explicitly marked for failure via {@link Transaction#failure()}.
     * <p>
     * This operation works the same way as {@link #writeTransactionAsync(TransactionWork)} but blocks until given
     * blocking unit of work is completed.
     *
     * @param work the {@link TransactionWork} to be applied to a new write transaction.
     * @param <T> the return type of the given unit of work.
     * @return a result as returned by the given unit of work.
     */
    <T> T writeTransaction( TransactionWork<T> work );

    <T> T writeTransaction( TransactionWork<T> work, TransactionConfig config );

    /**
     * Execute given unit of asynchronous work in a  {@link AccessMode#WRITE write} asynchronous transaction.
     * <p>
     * Transaction will automatically be committed unless given unit of work fails or
     * {@link Transaction#commitAsync() async transaction commit} fails. It will also not be committed if explicitly
     * rolled back via {@link Transaction#rollbackAsync()}.
     * <p>
     * Returned stage and given {@link TransactionWork} can be completed/executed by an IO thread which should never
     * block. Otherwise IO operations on this and potentially other network connections might deadlock. Please do not
     * chain blocking operations like {@link #run(String)} on the returned stage and do not use them inside the
     * {@link TransactionWork}. Driver will throw {@link IllegalStateException} when blocking API
     * call is executed in IO thread. Consider using asynchronous calls throughout the chain or offloading blocking
     * operation to a different {@link Executor}. This can be done using methods with "Async" suffix like
     * {@link CompletionStage#thenApplyAsync(Function)} or {@link CompletionStage#thenApplyAsync(Function, Executor)}.
     *
     * @param work the {@link TransactionWork} to be applied to a new write transaction. Operation executed by the
     * given work must be asynchronous.
     * @param <T> the return type of the given unit of work.
     * @return a {@link CompletionStage completion stage} completed with the same result as returned by the given
     * unit of work. Stage can be completed exceptionally if given work or commit fails.
     */
    <T> CompletionStage<T> writeTransactionAsync( TransactionWork<CompletionStage<T>> work );

    <T> CompletionStage<T> writeTransactionAsync( TransactionWork<CompletionStage<T>> work, TransactionConfig config );

    StatementResult run( String statement, TransactionConfig config );

    StatementResult run( String statement, Map<String,Object> parameters, TransactionConfig config );

    StatementResult run( Statement statement, TransactionConfig config );

    CompletionStage<StatementResultCursor> runAsync( String statement, TransactionConfig config );

    CompletionStage<StatementResultCursor> runAsync( String statement, Map<String,Object> parameters, TransactionConfig config );

    CompletionStage<StatementResultCursor> runAsync( Statement statement, TransactionConfig config );

    /**
     * Return the bookmark received following the last completed
     * {@linkplain Transaction transaction}. If no bookmark was received
     * or if this transaction was rolled back, the bookmark value will
     * be null.
     *
     * @return a reference to a previous transaction
     */
    String lastBookmark();

    /**
     * Reset the current session. This sends an immediate RESET signal to the server which both interrupts
     * any statement that is currently executing and ignores any subsequently queued statements. Following
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
     * <p>
     * This operation works the same way as {@link #closeAsync()} but blocks until session is actually closed.
     */
    @Override
    void close();

    /**
     * Signal that you are done using this session. In the default driver usage, closing and accessing sessions is
     * very low cost.
     * <p>
     * This operation is asynchronous and returns a {@link CompletionStage}. Stage is completed when all outstanding
     * statements in the session have completed, meaning any writes you performed are guaranteed to be durably stored.
     * It might be completed exceptionally when there are unconsumed errors from previous statements or transactions.
     *
     * @return a {@link CompletionStage completion stage} that represents the asynchronous close.
     */
    CompletionStage<Void> closeAsync();
}
