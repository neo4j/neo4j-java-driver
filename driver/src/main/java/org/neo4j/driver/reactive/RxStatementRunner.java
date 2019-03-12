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

import java.util.Map;

import org.neo4j.driver.Record;
import org.neo4j.driver.Statement;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;

/**
 * Common interface for components that can execute Neo4j statements using Reactive API.
 * @see RxSession
 * @see RxTransaction
 * @since 2.0
 */
public interface RxStatementRunner
{
    /**
     * Register running of a statement and return a reactive result stream.
     * The statement is not executed when the reactive result is returned.
     * Instead, the publishers in the result will actually start the execution of the statement.
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
     * @param statementTemplate text of a Neo4j statement
     * @param parameters input parameters, should be a map Value, see {@link Values#parameters(Object...)}.
     * @return a reactive result.
     */
    RxResult run( String statementTemplate, Value parameters );

    /**
     * Register running of a statement and return a reactive result stream.
     * The statement is not executed when the reactive result is returned.
     * Instead, the publishers in the result will actually start the execution of the statement.
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
     * @param statementTemplate text of a Neo4j statement
     * @param statementParameters input data for the statement
     * @return a reactive result.
     */
    RxResult run( String statementTemplate, Map<String,Object> statementParameters );

    /**
     * Register running of a statement and return a reactive result stream.
     * The statement is not executed when the reactive result is returned.
     * Instead, the publishers in the result will actually start the execution of the statement.
     *
     * This method takes a set of parameters that will be injected into the
     * statement by Neo4j. Using parameters is highly encouraged, it helps avoid
     * dangerous cypher injection attacks and improves database performance as
     * Neo4j can re-use query plans more often.
     *
     * This version of run takes a {@link Record} of parameters, which can be useful
     * if you want to use the output of one statement as input for another.
     *
     * @param statementTemplate text of a Neo4j statement
     * @param statementParameters input data for the statement
     * @return a reactive result.
     */
    RxResult run( String statementTemplate, Record statementParameters );

    /**
     * Register running of a statement and return a reactive result stream.
     * The statement is not executed when the reactive result is returned.
     * Instead, the publishers in the result will actually start the execution of the statement.
     *
     * @param statementTemplate text of a Neo4j statement
     * @return a reactive result.
     */
    RxResult run( String statementTemplate );

    /**
     * Register running of a statement and return a reactive result stream.
     * The statement is not executed when the reactive result is returned.
     * Instead, the publishers in the result will actually start the execution of the statement.
     *
     * @param statement a Neo4j statement
     * @return a reactive result.
     */
    RxResult run( Statement statement );
}
