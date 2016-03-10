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

import java.util.Map;

import org.neo4j.driver.v1.util.Experimental;
import org.neo4j.driver.v1.types.TypeSystem;

/**
 * Common interface for components that can execute Neo4j statements.
 *
 * @see Session
 * @see Transaction
 */
public interface StatementRunner
{
    /**
     * Run a statement and return a result stream.
     *
     * This method takes a set of parameters that will be injected into the
     * statement by Neo4j. Using parameters is highly encouraged, it helps avoid
     * dangerous cypher injection attacks and improves database performance as
     * Neo4j can re-use query plans more often.
     *
     * This particular method takes a {@link Value} as its input. This is useful
     * if you want to take a map-like value that you've gotten from a prior result
     * and send it back as parameters.
     *
     * If you are creating parameters programmatically, {@link #run(String, Map)}
     * might be more helpful, it converts your map to a {@link Value} for you.
     *
     * <h2>Example</h2>
     * <pre class="doctest:StatementRunnerDocIT#parameterTest">
     * {@code
     * StatementResult cursor = session.run( "MATCH (n) WHERE n.name = {myNameParam} RETURN (n)",
     *                                       Values.parameters( "myNameParam", "Bob" ) );
     * }
     * </pre>
     *
     * @param statementTemplate text of a Neo4j statement
     * @param parameters input parameters, should be a map Value, see {@link Values#parameters(Object...)}.
     * @return a stream of result values and associated metadata
     */
    StatementResult run( String statementTemplate, Value parameters );

    /**
     * Run a statement and return a result stream.
     *
     * This method takes a set of parameters that will be injected into the
     * statement by Neo4j. Using parameters is highly encouraged, it helps avoid
     * dangerous cypher injection attacks and improves database performance as
     * Neo4j can re-use query plans more often.
     *
     * This version of run takes a {@link Map} of parameters. The values in the map
     * must be values that can be converted to Neo4j types. See {@link Values#parameters(Object...)} for
     * a list of allowed types.
     *
     * <h2>Example</h2>
     * <pre class="doctest:StatementRunnerDocIT#parameterTest">
     * {@code
     * Map<String, Object> parameters = new HashMap<String, Object>();
     * parameters.put("myNameParam", "Bob");
     *
     * StatementResult cursor = session.run( "MATCH (n) WHERE n.name = {myNameParam} RETURN (n)",
     *                                       parameters );
     * }
     * </pre>
     *
     * @param statementTemplate text of a Neo4j statement
     * @param statementParameters input data for the statement
     * @return a stream of result values and associated metadata
     */
    StatementResult run( String statementTemplate, Map<String,Object> statementParameters );

    /**
     * Run a statement and return a result stream.
     *
     * @param statementTemplate text of a Neo4j statement
     * @return a stream of result values and associated metadata
     */
    StatementResult run( String statementTemplate );

    /**
     * Run a statement and return a result stream.
     * <h2>Example</h2>
     * <pre class="doctest:StatementRunnerDocIT#statementObjectTest">
     * {@code
     * Statement statement = new Statement( "MATCH (n) WHERE n.name={myNameParam} RETURN n.age" );
     * StatementResult cursor = session.run( statement.withParameters( Values.parameters( "myNameParam", "Bob" )  ) );
     * }
     * </pre>
     *
     * @param statement a Neo4j statement
     * @return a stream of result values and associated metadata
     */
    StatementResult run( Statement statement );

    /**
     * @return type system used by this statement runner for classifying values
     */
    @Experimental
    TypeSystem typeSystem();
}
