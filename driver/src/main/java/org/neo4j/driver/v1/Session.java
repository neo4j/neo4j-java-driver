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
 * A live session with a Neo4j instance.
 * <p>
 * Sessions serve two purposes. For one, they are an optimization. By keeping state on the database side, we can
 * avoid re-transmitting certain metadata over and over.
 * <p>
 * Sessions also serve a role in transaction isolation and ordering semantics. Neo4j requires
 * "sticky sessions", meaning all requests within one session must always go to the same Neo4j instance.
 * <p>
 * Session objects are not thread safe, if you want to run concurrent operations against the database,
 * simply create multiple sessions objects.
 */
public interface Session extends AutoCloseable, StatementRunner
{
    /**
     * Begin a new transaction in this session. A session can have at most one transaction running at a time, if you
     * want to run multiple concurrent transactions, you should use multiple concurrent sessions.
     * <p>
     * All data operations in Neo4j are transactional. However, for convenience we provide a {@link #run(String)}
     * method directly on this session interface as well. When you use that method, your statement automatically gets
     * wrapped in a transaction.
     * <p>
     * If you want to run multiple statements in the same transaction, you should wrap them in a transaction using this
     * method.
     *
     * @return a new transaction
     */
    Transaction beginTransaction();

    @Override
    void close();
}
