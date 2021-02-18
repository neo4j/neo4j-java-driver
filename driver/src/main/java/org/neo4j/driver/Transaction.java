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

import org.neo4j.driver.util.Resource;

/**
 * Logical container for an atomic unit of work.
 * A driver Transaction object corresponds to a server transaction.
 * <p>
 * Transactions are typically wrapped in a try-with-resources block
 * which ensures in case of any error in try block, the transaction is
 * automatically rolled back on close. Note that <code>ROLLBACK</code> is the
 * default action unless {@link #commit()} is called before closing.
 * <pre>{@code
 * try(Transaction tx = session.beginTransaction())
 * {
 *     tx.run("CREATE (a:Person {name: $name})", parameters("name", "Alice"));
 *     tx.commit();
 * }
 * }</pre>
 * Blocking calls are: {@link #commit()}, {@link #rollback()}, {@link #close()}
 * and various overloads of {@link #run(Query)}.
 *
 * @see Session#run
 * @see QueryRunner
 * @since 1.0
 */
public interface Transaction extends Resource, QueryRunner
{
    /**
     * Commit this current transaction.
     * When this method returns, all outstanding queries in the transaction are guaranteed to
     * have completed, meaning any writes you performed are guaranteed to be durably stored.
     * No more queries can be executed inside this transaction once this transaction is committed.
     * After this method is called, the transaction cannot be committed or rolled back again.
     * You must call this method before calling {@link #close()} to have your transaction committed.
     * If a transaction is not committed or rolled back before close,
     * the transaction will be rolled back by default in {@link #close}.
     * <pre>Example:
     * {@code
     * try(Transaction tx = session.beginTransaction() )
     * {
     *     tx.run("CREATE (a:Person {name: $name})", parameters("name", "Alice"));
     *     tx.commit();
     * }
     * }</pre>
     */
    void commit();

    /**
     * Roll back this current transaction.
     * No more queries can be executed inside this transaction once this transaction is committed.
     * After this method has been called, the transaction cannot be committed or rolled back again.
     * If a transaction is not committed or rolled back before close,
     * the transaction will be rolled back by default in {@link #close}.
     * <pre>Example:
     * {@code
     * try(Transaction tx = session.beginTransaction() )
     * {
     *     tx.run("CREATE (a:Person {name: $name})", parameters("name", "Alice"));
     *     tx.rollback();
     * }
     * }</pre>
     */
    void rollback();

    /**
     * Close the transaction.
     * If the transaction has been {@link #commit() committed} or {@link #rollback() rolled back},
     * the close is optional and no operation is performed inside.
     * Otherwise, the transaction will be rolled back by default by this method.
     * <pre>Example:
     * {@code
     * try(Transaction tx = session.beginTransaction() )
     * {
     *     tx.run("CREATE (a:Person {name: $name})", parameters("name", "Alice"));
     * }
     * }</pre>
     */
    @Override
    void close();
}
