/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.reactivestreams.Publisher;

import java.util.Map;

import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.TransactionConfig;
import org.neo4j.driver.v1.Values;

/**
 * A reactive session is the same as {@link Session} except it provides a reactive API.
 * @see Session
 * @see RxResult
 * @see RxTransaction
 * @see Publisher
 * @since 2.0
 */
public interface RxSession extends RxStatementRunner
{
    /**
     * Begin a new <em>explicit {@linkplain RxTransaction transaction}</em>. At
     * most one transaction may exist in a session at any point in time. To
     * maintain multiple concurrent transactions, use multiple concurrent
     * sessions.
     * <p>
     * It by default is executed in Netty IO thread, as a result no blocking operation is allowed in this thread.
     *
     * @return a new {@link RxTransaction}
     */
    Publisher<RxTransaction> beginTransaction();

    /**
     * Begin a new <em>explicit {@linkplain RxTransaction transaction}</em> with the specified {@link TransactionConfig configuration}.
     * At most one transaction may exist in a session at any point in time. To
     * maintain multiple concurrent transactions, use multiple concurrent
     * sessions.
     * <p>
     * It by default is executed in Netty IO thread, as a result no blocking operation is allowed in this thread.
     *
     * @param config configuration for the new transaction.
     * @return a new {@link RxTransaction}
     */
    Publisher<RxTransaction> beginTransaction( TransactionConfig config );

    <T> Publisher<T> readTransaction( RxTransactionWork<Publisher<T>> work );

    <T> Publisher<T> readTransaction( RxTransactionWork<Publisher<T>> work, TransactionConfig config );

    <T> Publisher<T> writeTransaction( RxTransactionWork<Publisher<T>> work );

    <T> Publisher<T> writeTransaction( RxTransactionWork<Publisher<T>> work, TransactionConfig config );

    /**
     * Run a statement with parameters in an auto-commit transaction with specified {@link TransactionConfig} and return a reactive result stream.
     * The statement is not executed when the reactive result is returned.
     * Instead, the publishers in the result will actually start the execution of the statement.
     *
     * @param statement text of a Neo4j statement.
     * @param config configuration for the new transaction.
     * @return a reactive result.
     */
    RxResult run( String statement, TransactionConfig config );

    /**
     * Run a statement with parameters in an auto-commit transaction with specified {@link TransactionConfig} and return a reactive result stream.
     * The statement is not executed when the reactive result is returned.
     * Instead, the publishers in the result will actually start the execution of the statement.
     * <p>
     * This method takes a set of parameters that will be injected into the statement by Neo4j.
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
     * RxResult result = rxSession.run("MATCH (n) WHERE n.name = {myNameParam} RETURN (n)", parameters, config);
     * }
     * </pre>
     *
     * @param statement text of a Neo4j statement.
     * @param parameters input data for the statement.
     * @param config configuration for the new transaction.
     * @return a reactive result.
     */
    RxResult run( String statement, Map<String,Object> parameters, TransactionConfig config );

    /**
     * Run a statement in an auto-commit transaction with specified {@link TransactionConfig configuration} and return a reactive result stream.
     * The statement is not executed when the reactive result is returned.
     * Instead, the publishers in the result will actually start the execution of the statement.
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
     * Statement statement = new Statement("MATCH (n) WHERE n.name=$myNameParam RETURN n.age");
     * RxResult result = rxSession.run(statement.withParameters(Values.parameters("myNameParam", "Bob")));
     * }
     * </pre>
     *
     * @param statement a Neo4j statement.
     * @param config configuration for the new transaction.
     * @return a reactive result.
     */
    RxResult run( Statement statement, TransactionConfig config );

    /**
     * Return the bookmark received following the last completed statement within this session.
     * The last completed statement can be run in a {@linkplain RxTransaction transaction}
     * started using {@linkplain #beginTransaction() beginTransaction} or directly via {@link #run(Statement) run}.
     *
     * @return a reference to a previous transaction.
     */
    String lastBookmark();

    /**
     * Signal that you are done using this session.
     * In the default driver usage, closing and accessing sessions is very low cost.
     * <p>
     * This operation is not needed if 1) all results created in the session have been fully consumed and
     * 2) all transactions opened by this session have been either committed or rolled back.
     * <p>
     * This method is a fallback if you failed to fulfill the two requirements above.
     * This publisher is completed when all outstanding statements in the session have completed,
     * meaning any writes you performed are guaranteed to be durably stored.
     * It might be completed exceptionally when there are unconsumed errors from previous statements or transactions.
     *
     * @param <T> makes it easier to be chained.
     * @return an empty publisher that represents the reactive close.
     */
    <T> Publisher<T> close();
}
