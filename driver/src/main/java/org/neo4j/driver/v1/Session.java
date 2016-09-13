/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.driver.v1.util.Resource;

/**
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
 * Typically, a session will wrap a TCP connection. Such a connection will be
 * acquired from a connection pool and released back there when the session is
 * destroyed. One connection can therefore be adopted by many sessions,
 * although by only one at a time. Application code should never need to deal
 * directly with connection management.
 * <p>
 * A session inherits its destination address and permissions from its
 * underlying connection. This means that one session may only ever target one
 * machine within a cluster and does not support re-authentication. To achieve
 * otherwise requires creation of a separate session.
 * <p>
 * Similarly, multiple sessions should be used when working with concurrency;
 * session implementations are generally not thread safe.
 *
 * @since 1.0
 */
public interface Session extends Resource, StatementRunner
{
    String LOG_NAME = "session";

    /**
     * Begin a new <em>explicit {@linkplain Transaction transaction}</em>. At
     * most one transaction may exist in a session at any point in time. To
     * maintain multiple concurrent transactions, use multiple concurrent
     * sessions.
     *
     * @return a new {@link Transaction}
     */
    Transaction beginTransaction();

    /**
     * Begin a new <em>explicit {@linkplain Transaction transaction}</em>,
     * requiring that the server hosting is at least as up-to-date as the
     * transaction referenced by the supplied <em>bookmark</em>.
     *
     * @param bookmark a reference to a previous transaction
     * @return a new {@link Transaction}
     */
    Transaction beginTransaction( String bookmark );

    /**
     * Return the bookmark received following the last completed
     * {@linkplain Transaction transaction}. If no bookmark was received
     * or if this transaction was rolled back, the bookmark value will
     * be null.
     *
     * @return a reference to a previous transac'tion
     */
    String lastBookmark();

    /**
     * Reset the current session. This sends an immediate RESET signal to the server which both interrupts
     * any statement that is currently executing and ignores any subsequently queued statements. Following
     * the reset, the current transaction will have been rolled back and any outstanding failures will
     * have been acknowledged.
     */
    void reset();

    /**
     * Signal that you are done using this session. In the default driver usage, closing
     * and accessing sessions is very low cost, because sessions are pooled by {@link Driver}.
     *
     * When this method returns, all outstanding statements in the session are guaranteed to
     * have completed, meaning any writes you performed are guaranteed to be durably stored.
     */
    @Override
    void close();

    /**
     * Returns a string telling which version of the server the session is connected to.
     * @return The server version of <code>null</code> if not available.
     */
    String server();
}
