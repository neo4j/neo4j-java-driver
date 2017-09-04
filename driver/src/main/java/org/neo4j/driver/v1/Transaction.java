/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.neo4j.driver.internal.netty.ListenableFuture;
import org.neo4j.driver.v1.util.Resource;

/**
 * Logical container for an atomic unit of work.
 * <p>
 * A driver Transaction object corresponds to a server transaction.
 * Transactions are typically wrapped in a try-with-resources block
 * which ensures that <code>COMMIT</code> or <code>ROLLBACK</code>
 * occurs correctly on close.Note that <code>ROLLBACK</code> is the
 * default action unless {@link #success()} is called before closing.
 * <pre class="docTest:TransactionDocIT#classDoc">
 * {@code
 * try(Transaction tx = session.beginTransaction())
 * {
 *     tx.run("CREATE (a:Person {name: {x}})", parameters("x", "Alice"));
 *     tx.success();
 * }
 * }
 * </pre>
 *
 * @see Session#run
 * @see StatementRunner
 * @since 1.0
 */
public interface Transaction extends Resource, StatementRunner
{
    /**
     * Mark this transaction as successful. You must call this method before calling {@link #close()} to have your
     * transaction committed.
     */
    void success();

    /**
     * Mark this transaction as failed. When you call {@link #close()}, the transaction will value rolled back.
     *
     * After this method has been called, there is nothing that can be done to "un-mark" it. This is a safety feature
     * to make sure no other code calls {@link #success()} and makes a transaction commit that was meant to be rolled
     * back.
     *
     * Example:
     *
     * <pre class="docTest:TransactionDocIT#failure">
     * {@code
     * try(Transaction tx = session.beginTransaction() )
     * {
     *     tx.run("CREATE (a:Person {name: {x}})", parameters("x", "Alice"));
     *     tx.failure();
     * }
     * }
     * </pre>
     */
    void failure();

    /**
     * Closing the transaction will complete it - it will commit if {@link #success()} has been called.
     * When this method returns, all outstanding statements in the transaction are guaranteed to
     * have completed, meaning any writes you performed are guaranteed to be durably stored.
     */
    @Override
    void close();

    ListenableFuture<Void> commitAsync();

    ListenableFuture<Void> rollbackAsync();
}
