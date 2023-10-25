/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.util.Extract;
import org.neo4j.driver.internal.value.MapValue;

/**
 * Common interface for components that can execute Neo4j queries using Reactive API.
 * @see RxSession
 * @see RxTransaction
 * @since 4.0
 * @deprecated superseded by {@link org.neo4j.driver.reactive.ReactiveQueryRunner} and {@link org.neo4j.driver.reactivestreams.ReactiveQueryRunner}
 */
@Deprecated
public interface RxQueryRunner {
    /**
     * Register running of a query and return a reactive result stream.
     * The query is not executed when the reactive result is returned.
     * Instead, the publishers in the result will actually start the execution of the query.
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
     * @param query text of a Neo4j query
     * @param parameters input parameters, should be a map Value, see {@link Values#parameters(Object...)}.
     * @return a reactive result.
     */
    default RxResult run(String query, Value parameters) {
        return run(new Query(query, parameters));
    }

    /**
     * Register running of a query and return a reactive result stream.
     * The query is not executed when the reactive result is returned.
     * Instead, the publishers in the result will actually start the execution of the query.
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
     * @param query text of a Neo4j query
     * @param parameters input data for the query
     * @return a reactive result.
     */
    default RxResult run(String query, Map<String, Object> parameters) {
        return run(query, parameters(parameters));
    }

    /**
     * Register running of a query and return a reactive result stream.
     * The query is not executed when the reactive result is returned.
     * Instead, the publishers in the result will actually start the execution of the query.
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
     * @return a reactive result.
     */
    default RxResult run(String query, Record parameters) {
        return run(query, parameters(parameters));
    }

    /**
     * Register running of a query and return a reactive result stream. The query is not executed when the reactive result is returned. Instead, the publishers
     * in the result will actually start the execution of the query.
     *
     * @param query text of a Neo4j query
     * @return a reactive result.
     */
    default RxResult run(String query) {
        return run(new Query(query));
    }

    /**
     * Register running of a query and return a reactive result stream. The query is not executed when the reactive result is returned. Instead, the publishers
     * in the result will actually start the execution of the query.
     *
     * @param query a Neo4j query
     * @return a reactive result.
     */
    RxResult run(Query query);

    /**
     * Creates a value from a record instance.
     * @param record the record
     * @return the value
     */
    static Value parameters(Record record) {
        return record == null ? Values.EmptyMap : parameters(record.asMap());
    }

    /**
     * Creates a value from a map instance.
     * @param map the map
     * @return the value
     */
    static Value parameters(Map<String, Object> map) {
        if (map == null || map.isEmpty()) {
            return Values.EmptyMap;
        }
        return new MapValue(Extract.mapOfValues(map));
    }
}
