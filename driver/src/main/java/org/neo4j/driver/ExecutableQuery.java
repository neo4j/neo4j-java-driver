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
package org.neo4j.driver;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import org.neo4j.driver.exceptions.UnsupportedFeatureException;
import org.neo4j.driver.internal.EagerResultValue;
import org.neo4j.driver.summary.ResultSummary;

/**
 * An executable query that executes a query in a managed transaction with automatic retries on retryable errors.
 * <p>
 * This is a high-level API for executing a query. There are more advanced APIs available.
 * For instance, {@link Session}, {@link Transaction} and transaction functions that are accessible via
 * methods like {@link Session#executeWrite(TransactionCallback)}, {@link Session#executeWriteWithoutResult(Consumer)}
 * and {@link Session#executeRead(TransactionCallback)} (there are also overloaded options available).
 * <p>
 * Causal consistency is managed via driver's {@link BookmarkManager} that is enabled by default. It is possible
 * to use a different {@link BookmarkManager} or disable it via
 * {@link QueryConfig.Builder#withBookmarkManager(BookmarkManager)} on individual basis.
 * <p>
 * Sample usage:
 * <pre>
 * {@code
 * var eagerResult = driver.executableQuery("CREATE (n{field: $value}) RETURN n")
 *         .withParameters(Map.of("value", "5"))
 *         .execute();
 * }
 * </pre>
 * The above sample is functionally similar to the following use of the more advanced APIs:
 * <pre>
 * {@code
 * var query = new Query("CREATE (n{field: $value}) RETURN n", Map.of("value", "5"));
 * var sessionConfig = SessionConfig.builder()
 *         .withBookmarkManager(driverConfig.queryBookmarkManager())
 *         .build();
 * try (var session = driver.session(sessionConfig)) {
 *     var eagerResult = session.executeWrite(tx -> {
 *         var result = tx.run(query);
 *         return new EagerResultValue(result.keys(), result.stream().toList(), result.consume());
 *     });
 * }
 * }
 * </pre>
 * In addition, it is possible to transform query result by using a supplied {@link Collector} implementation.
 * <p>
 * <b>It is strongly recommended to use Cypher query language capabilities where possible</b>. The examples below just
 * provide a sample usage of the API.
 * <pre>
 * {@code
 * import static java.util.stream.Collectors.*;
 *
 * var averagingLong = driver.executableQuery("UNWIND range(0, 5) as N RETURN N")
 *         .execute(averagingLong(record -> record.get("N").asLong()));
 *
 * var filteredValues = driver.executableQuery("UNWIND range(0, 5) as N RETURN N")
 *         .execute(mapping(record -> record.get("N").asLong(), filtering(value -> value > 2, toList())));
 *
 * var maxValue = driver.executableQuery("UNWIND range(0, 5) as N RETURN N")
 *         .execute(mapping(record -> record.get("N").asLong(), maxBy(Long::compare)));
 * }
 * </pre>
 * If there is a need to access {@link Result#keys()} and/or {@link ResultSummary} value, another method option is
 * available:
 * <pre>
 * {@code
 * import static java.util.stream.Collectors.*;
 *
 * private record ResultValue(List<String> keys, Set<Long> values, ResultSummary summary) {}
 *
 * var result = driver.executableQuery("UNWIND range(0, 5) as N RETURN N")
 *                     .execute(Collectors.mapping(record -> record.get("N").asLong(), toSet()), ResultValue::new);
 * }
 * </pre>
 *
 * @since 5.7
 */
public interface ExecutableQuery {
    /**
     * Sets query parameters.
     *
     * @param parameters parameters map, must not be {@literal null}
     * @return a new executable query
     */
    ExecutableQuery withParameters(Map<String, Object> parameters);

    /**
     * Sets {@link QueryConfig}.
     * <p>
     * By default, {@link ExecutableQuery} has {@link QueryConfig#defaultConfig()} value.
     *
     * @param config query config, must not be {@literal null}
     * @return a new executable query
     */
    ExecutableQuery withConfig(QueryConfig config);

    /**
     * Sets an {@link AuthToken} to be used for this query.
     * <p>
     * The default value is {@literal null}.
     * <p>
     * The minimum Bolt protocol version for this feature is 5.1. An {@link UnsupportedFeatureException} will be emitted on
     * query execution for previous Bolt versions.
     *
     * @param authToken the {@link AuthToken} for this query or {@literal null} to use the driver default
     * @return a new executable query
     * @since 5.18
     */
    default ExecutableQuery withAuthToken(AuthToken authToken) {
        throw new UnsupportedFeatureException("Session AuthToken is not supported.");
    }

    /**
     * Executes query, collects all results eagerly and returns a result.
     *
     * @return an instance of result containing all records, keys and result summary
     */
    default EagerResult execute() {
        return execute(Collectors.toList(), EagerResultValue::new);
    }

    /**
     * Executes query, collects {@link Record} values using the provided {@link Collector} and produces a final result.
     *
     * @param recordCollector collector instance responsible for processing {@link Record} values and producing a
     *                        collected result, the collector may be used multiple times if query is retried
     * @param <T> the final result type
     * @return the final result value
     */
    default <T> T execute(Collector<Record, ?, T> recordCollector) {
        return execute(recordCollector, (ignoredKeys, collectorResult, ignoredSummary) -> collectorResult);
    }

    /**
     * Executes query, collects {@link Record} values using the provided {@link Collector} and produces a final result
     * by invoking the provided {@link BiFunction} with the collected result and {@link ResultSummary} values.
     * <p>
     * If any of the arguments throws an exception implementing the
     * {@link org.neo4j.driver.exceptions.RetryableException} marker interface, the query is retried automatically in
     * the same way as in the transaction functions. Exceptions not implementing the interface trigger transaction
     * rollback and are then propagated to the user.
     *
     * @param recordCollector collector instance responsible for processing {@link Record} values and producing a
     *                        collected result, the collector may be used multiple times if query is retried
     * @param resultFinisher function accepting the {@link Result#keys()}, collected result and {@link ResultSummary}
     *                       values to output the final result value, the function may be invoked multiple times if
     *                       query is retried
     * @param <A> the mutable accumulation type of the collector's reduction operation
     * @param <R> the collector's result type
     * @param <T> the final result type
     * @return the final result value
     */
    <A, R, T> T execute(Collector<Record, A, R> recordCollector, ResultFinisher<R, T> resultFinisher);

    /**
     * A function accepting the {@link Result#keys()}, collected result and {@link ResultSummary} values to produce a
     * final result value.
     *
     * @param <S> the collected value type
     * @param <T> the final value type
     * @since 5.5
     */
    @FunctionalInterface
    interface ResultFinisher<S, T> {
        /**
         * Accepts the {@link Result#keys()}, collected result and {@link ResultSummary} values to produce the final
         * result value.
         * @param value the collected value
         * @param keys the {@link Result#keys()} value
         * @param summary the {@link ResultSummary} value
         * @return the final value
         */
        T finish(List<String> keys, S value, ResultSummary summary);
    }
}
