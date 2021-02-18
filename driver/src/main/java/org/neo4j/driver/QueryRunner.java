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

/**
 * Common interface for components that can execute Neo4j queries.
 *
 * <h2>Important notes on semantics</h2>
 * <p>
 * queries run in the same {@link QueryRunner} are guaranteed
 * to execute in order, meaning changes made by one query will be seen
 * by all subsequent queries in the same {@link QueryRunner}.
 * <p>
 * However, to allow handling very large results, and to improve performance,
 * result streams are retrieved lazily from the network.
 * This means that when any of {@link #run(Query)}
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
 * {@link Result#next()} or {@link Result#consume()} </li>
 * <li>Explicitly commit/rollback a transaction using blocking {@link Transaction#close()} </li>
 * <li>Close a session using blocking {@link Session#close()}</li>
 * </ul>
 * <p>
 * As noted, most of the time, you will not need to consider this - your writes will
 * always be durably stored as long as you either use the results, explicitly commit
 * {@link Transaction transactions} or close the session you used using {@link Session#close()}.
 * <p>
 * While these semantics introduce some complexity, it gives the driver the ability
 * to handle infinite result streams (like subscribing to events), significantly lowers
 * the memory overhead for your application and improves performance.
 *
 * @see Session
 * @see Transaction
 * @since 1.0
 */
public interface QueryRunner
{
    /**
     * Run a query and return a result stream.
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
     * If you are creating parameters programmatically, {@link #run(String, Map)}
     * might be more helpful, it converts your map to a {@link Value} for you.
     *
     * <h2>Example</h2>
     * <pre class="doctest:QueryRunnerDocIT#parameterTest">
     * {@code
     *
     * Result result = session.run( "MATCH (n) WHERE n.name = $myNameParam RETURN (n)",
     *                                       Values.parameters( "myNameParam", "Bob" ) );
     * }
     * </pre>
     *
     * @param query text of a Neo4j query
     * @param parameters input parameters, should be a map Value, see {@link Values#parameters(Object...)}.
     * @return a stream of result values and associated metadata
     */
    Result run(String query, Value parameters );

    /**
     * Run a query and return a result stream.
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
     * <pre class="doctest:QueryRunnerDocIT#parameterTest">
     * {@code
     *
     * Map<String, Object> parameters = new HashMap<String, Object>();
     * parameters.put("myNameParam", "Bob");
     *
     * Result result = session.run( "MATCH (n) WHERE n.name = $myNameParam RETURN (n)",
     *                                       parameters );
     * }
     * </pre>
     *
     * @param query text of a Neo4j query
     * @param parameters input data for the query
     * @return a stream of result values and associated metadata
     */
    Result run(String query, Map<String,Object> parameters );

    /**
     * Run a query and return a result stream.
     * <p>
     * This method takes a set of parameters that will be injected into the
     * query by Neo4j. Using parameters is highly encouraged, it helps avoid
     * dangerous cypher injection attacks and improves database performance as
     * Neo4j can re-use query plans more often.
     * <p>
     * This version of run takes a {@link Record} of parameters, which can be useful
     * if you want to use the output of one query as input for another.
     *
     * @param query text of a Neo4j query
     * @param parameters input data for the query
     * @return a stream of result values and associated metadata
     */
    Result run(String query, Record parameters );

    /**
     * Run a query and return a result stream.
     *
     * @param query text of a Neo4j query
     * @return a stream of result values and associated metadata
     */
    Result run(String query );

    /**
     * Run a query and return a result stream.
     * <h2>Example</h2>
     * <pre class="doctest:QueryRunnerDocIT#queryObjectTest">
     * {@code
     *
     * Query query = new Query( "MATCH (n) WHERE n.name = $myNameParam RETURN n.age" );
     * Result result = session.run( query.withParameters( Values.parameters( "myNameParam", "Bob" )  ) );
     * }
     * </pre>
     *
     * @param query a Neo4j query
     * @return a stream of result values and associated metadata
     */
    Result run(Query query);
}
