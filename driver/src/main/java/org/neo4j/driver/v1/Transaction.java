/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

/**
 * Represents a transaction in the Neo4j database.
 *
 * This interface may seem surprising in that it does not have explicit "commit" or "rollback" methods.
 * It is designed to minimize the complexity of the code you need to write to use transactions in a safe way, ensuring
 * that transactions are properly rolled back even if there is an exception while the transaction is running.
 *
 * <h2>Example:</h2>
 *
 * <pre class="docTest:TransactionDocIT#classDoc">
 * {@code
 * try( Transaction tx = session.beginTransaction() )
 * {
 *     tx.run( "CREATE (n)" );
 *     tx.success();
 * }
 * }
 * </pre>
 */
public interface Transaction extends AutoCloseable, StatementRunner
{
    /**
     * Mark this transaction as successful. You must call this method before calling {@link #close()} to have your
     * transaction committed.
     */
    void success();

    /**
     * Mark this transaction as failed. When you call {@link #close()}, the transaction will get rolled back.
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
     *     tx.run( "CREATE (n)" );
     *     tx.failure();
     * }
     * }
     * </pre>
     */
    void failure();

    @Override
    void close();
}
