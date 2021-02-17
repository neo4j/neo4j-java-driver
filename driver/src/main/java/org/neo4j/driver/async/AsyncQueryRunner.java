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
package org.neo4j.driver.async;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Function;

import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;

/**
 * Asynchronous interface for components that can execute Neo4j queries.
 *
 * <h2>Important notes on semantics</h2>
 * <p>
 * Queries run in the same {@link AsyncQueryRunner} are guaranteed
 * to execute in order, meaning changes made by one query will be seen
 * by all subsequent queries in the same {@link AsyncQueryRunner}.
 * <p>
 * However, to allow handling very large results, and to improve performance,
 * result streams are retrieved lazily from the network. This means that when
 * async {@link #runAsync(Query)}
 * methods return a result, the query has only started executing - it may not
 * have completed yet. Most of the time, you will not notice this, because the
 * driver automatically waits for queries to complete at specific points to
 * fulfill its contracts.
 * <p>
 * Specifically, the driver will ensure all outstanding queries are completed
 * whenever you:
 *
 * <ul>
 * <li>Read from or discard a result, for instance via
 * {@link ResultCursor#nextAsync()}, {@link ResultCursor#consumeAsync()}</li>
 * <li>Explicitly commit/rollback a transaction using {@link AsyncTransaction#commitAsync()}, {@link AsyncTransaction#rollbackAsync()}</li>
 * <li>Close a session using {@link AsyncSession#closeAsync()}</li>
 * </ul>
 * <p>
 * As noted, most of the time, you will not need to consider this - your writes will
 * always be durably stored as long as you either use the results, explicitly commit
 * {@link AsyncTransaction transactions} or close the session you used using {@link AsyncSession#closeAsync()}.
 * <p>
 * While these semantics introduce some complexity, it gives the driver the ability
 * to handle infinite result streams (like subscribing to events), significantly lowers
 * the memory overhead for your application and improves performance.
 *
 * <h2>Asynchronous API</h2>
 * <p>
 * All overloads of {@link #runAsync(Query)} execute queries in async fashion and return {@link CompletionStage} of
 * a new {@link ResultCursor}. Stage can be completed exceptionally when error happens, e.g. connection can't
 * be acquired from the pool.
 * <p>
 * <b>Note:</b> Returned stage can be completed by an IO thread which should never block. Otherwise IO operations on
 * this and potentially other network connections might deadlock. Please do not chain blocking operations like
 * {@link CompletableFuture#get()} on the returned stage. Consider using asynchronous calls throughout the chain or offloading blocking
 * operation to a different {@link Executor}. This can be done using methods with "Async" suffix like
 * {@link CompletionStage#thenApplyAsync(Function)} or {@link CompletionStage#thenApplyAsync(Function, Executor)}.
 *
 * @see AsyncSession
 * @see AsyncTransaction
 * @since 4.0
 */
public interface AsyncQueryRunner
{
    /**
     * Run a query asynchronously and return a {@link CompletionStage} with a
     * result cursor.
     * <p>
     * This method takes a set of parameters that will be injected into the
     * query by Neo4j. Using parameters is highly encouraged, it helps avoid
     * dangerous cypher injection attacks and improves database performance as
     * Neo4j can re-use query plans more often.
     * <p>
     * This particular method takes a {@link Value} as its input. This is useful
     * if you want to take a map-like value that you've gotten from a prior result
     * and send it back as parameters.
     * <p>
     * If you are creating parameters programmatically, {@link #runAsync(String, Map)}
     * might be more helpful, it converts your map to a {@link Value} for you.
     * <h2>Example</h2>
     * <pre>
     * {@code
     *
     * CompletionStage<ResultCursor> cursorStage = session.runAsync(
     *             "MATCH (n) WHERE n.name = $myNameParam RETURN (n)",
     *             Values.parameters("myNameParam", "Bob"));
     * }
     * </pre>
     * It is not allowed to chain blocking operations on the returned {@link CompletionStage}. See class javadoc for
     * more information.
     *
     * @param query text of a Neo4j query
     * @param parameters input parameters, should be a map Value, see {@link Values#parameters(Object...)}.
     * @return new {@link CompletionStage} that gets completed with a result cursor when query execution is successful.
     * Stage can be completed exceptionally when error happens, e.g. connection can't be acquired from the pool.
     */
    CompletionStage<ResultCursor> runAsync(String query, Value parameters );

    /**
     * Run a query asynchronously and return a {@link CompletionStage} with a
     * result cursor.
     * <p>
     * This method takes a set of parameters that will be injected into the
     * query by Neo4j. Using parameters is highly encouraged, it helps avoid
     * dangerous cypher injection attacks and improves database performance as
     * Neo4j can re-use query plans more often.
     * <p>
     * This version of runAsync takes a {@link Map} of parameters. The values in the map
     * must be values that can be converted to Neo4j types. See {@link Values#parameters(Object...)} for
     * a list of allowed types.
     * <h2>Example</h2>
     * <pre>
     * {@code
     *
     * Map<String, Object> parameters = new HashMap<String, Object>();
     * parameters.put("myNameParam", "Bob");
     *
     * CompletionStage<ResultCursor> cursorStage = session.runAsync(
     *             "MATCH (n) WHERE n.name = $myNameParam RETURN (n)",
     *             parameters);
     * }
     * </pre>
     * It is not allowed to chain blocking operations on the returned {@link CompletionStage}. See class javadoc for
     * more information.
     *
     * @param query text of a Neo4j query
     * @param parameters input data for the query
     * @return new {@link CompletionStage} that gets completed with a result cursor when query execution is successful.
     * Stage can be completed exceptionally when error happens, e.g. connection can't be acquired from the pool.
     */
    CompletionStage<ResultCursor> runAsync(String query, Map<String,Object> parameters );

    /**
     * Run a query asynchronously and return a {@link CompletionStage} with a
     * result cursor.
     * <p>
     * This method takes a set of parameters that will be injected into the
     * query by Neo4j. Using parameters is highly encouraged, it helps avoid
     * dangerous cypher injection attacks and improves database performance as
     * Neo4j can re-use query plans more often.
     * <p>
     * This version of runAsync takes a {@link Record} of parameters, which can be useful
     * if you want to use the output of one query as input for another.
     * <p>
     * It is not allowed to chain blocking operations on the returned {@link CompletionStage}. See class javadoc for
     * more information.
     *
     * @param query text of a Neo4j query
     * @param parameters input data for the query
     * @return new {@link CompletionStage} that gets completed with a result cursor when query execution is successful.
     * Stage can be completed exceptionally when error happens, e.g. connection can't be acquired from the pool.
     */
    CompletionStage<ResultCursor> runAsync(String query, Record parameters );

    /**
     * Run a query asynchronously and return a {@link CompletionStage} with a
     * result cursor.
     * <p>
     * It is not allowed to chain blocking operations on the returned {@link CompletionStage}. See class javadoc for
     * more information.
     *
     * @param query text of a Neo4j query
     * @return new {@link CompletionStage} that gets completed with a result cursor when query execution is successful.
     * Stage can be completed exceptionally when error happens, e.g. connection can't be acquired from the pool.
     */
    CompletionStage<ResultCursor> runAsync(String query );

    /**
     * Run a query asynchronously and return a {@link CompletionStage} with a
     * result cursor.
     * <h2>Example</h2>
     * <pre>
     * {@code
     * Query query = new Query( "MATCH (n) WHERE n.name = $myNameParam RETURN n.age" );
     * CompletionStage<ResultCursor> cursorStage = session.runAsync(query);
     * }
     * </pre>
     * It is not allowed to chain blocking operations on the returned {@link CompletionStage}. See class javadoc for
     * more information.
     *
     * @param query a Neo4j query
     * @return new {@link CompletionStage} that gets completed with a result cursor when query execution is successful.
     * Stage can be completed exceptionally when error happens, e.g. connection can't be acquired from the pool.
     */
    CompletionStage<ResultCursor> runAsync(Query query);
}
