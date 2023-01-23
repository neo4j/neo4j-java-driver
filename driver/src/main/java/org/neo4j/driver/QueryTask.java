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
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collector;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.util.Experimental;

/**
 * A task that executes an idempotent query in a managed transaction with automatic retries on retryable errors.
 * <p>
 * This is a high-level API for executing an idempotent query. There are more advanced APIs available.
 * For instance, {@link Session}, {@link Transaction} and transaction functions that are accessible via
 * methods like {@link Session#executeWrite(TransactionCallback)}, {@link Session#executeWriteWithoutResult(Consumer)}
 * and {@link Session#executeRead(TransactionCallback)} (there are also overloaded options available).
 * <p>
 * Causal consistency is managed via driver's {@link BookmarkManager} that is enabled by default and may
 * be replaced using {@link Config.ConfigBuilder#withQueryTaskBookmarkManager(BookmarkManager)}. It is also possible
 * to use a different {@link BookmarkManager} or disable it via
 * {@link QueryConfig.Builder#withBookmarkManager(BookmarkManager)} on individual basis.
 * <p>
 * Sample usage:
 * <pre>
 * {@code
 * var eagerResult = driver.queryTask("CREATE (n{field: $value}) RETURN n")
 *         .withParameters(Map.of("$value", "5"))
 *         .execute();
 * }
 * </pre>
 * The above sample is functionally similar to the following use of the more advanced APIs:
 * <pre>
 * {@code
 * var query = new Query("CREATE (n{field: $value}) RETURN n", Map.of("$value", "5"));
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
 * var averagingLong = driver.queryTask("UNWIND range(0, 5) as N RETURN N")
 *         .execute(averagingLong(record -> record.get("N").asLong()));
 *
 * var filteredValues = driver.queryTask("UNWIND range(0, 5) as N RETURN N")
 *         .execute(mapping(record -> record.get("N").asLong(), filtering(value -> value > 2, toList())));
 *
 * var maxValue = driver.queryTask("UNWIND range(0, 5) as N RETURN N")
 *         .execute(mapping(record -> record.get("N").asLong(), maxBy(Long::compare)));
 * }
 * </pre>
 * If there is a need to access {@link ResultSummary} value, another method option is available:
 * <pre>
 * {@code
 * import static java.util.stream.Collectors.*;
 *
 * private record ResultValue(Set<Long> values, ResultSummary summary) {}
 *
 * var result = driver.queryTask("UNWIND range(0, 5) as N RETURN N")
 *                     .execute(Collectors.mapping(record -> record.get("N").asLong(), toSet()), ResultValue::new);
 * }
 * </pre>
 *
 * @since 5.5
 */
@Experimental
public interface QueryTask {
    /**
     * Sets query parameters.
     *
     * @param parameters parameters map, must not be {@code null}
     * @return a new query task
     */
    QueryTask withParameters(Map<String, Object> parameters);

    /**
     * Sets {@link QueryConfig}.
     * <p>
     * By default, {@link QueryTask} has {@link QueryConfig#defaultConfig()} value.
     *
     * @param config query config, must not be {@code null}
     * @return a new query task
     */
    QueryTask withConfig(QueryConfig config);

    /**
     * Executes query, collects all results eagerly and returns a result.
     *
     * @return an instance of result containing all records, keys and result summary
     */
    EagerResult execute();

    /**
     * Executes query, collects {@link Record} values using the provided {@link Collector} and produces a final result.
     *
     * @param recordCollector collector instance responsible for processing {@link Record} values and producing a
     *                        collected result, the collector may be used multiple times if query is retried
     * @param <T> the final result type
     * @return the final result value
     */
    default <T> T execute(Collector<Record, ?, T> recordCollector) {
        return execute(recordCollector, (collectorResult, ignored) -> collectorResult);
    }

    /**
     * Executes query, collects {@link Record} values using the provided {@link Collector} and produces a final result
     * by invoking the provided {@link BiFunction} with the collected result and {@link ResultSummary} values.
     *
     * @param recordCollector collector instance responsible for processing {@link Record} values and producing a
     *                        collected result, the collector may be used multiple times if query is retried
     * @param finisherWithSummary function accepting both the collected result and {@link ResultSummary} values to
     *                            output the final result, the function may be invoked multiple times if query is
     *                            retried
     * @param <A> the mutable accumulation type of the collector's reduction operation
     * @param <R> the collector's result type
     * @param <T> the final result type
     * @return the final result value
     */
    <A, R, T> T execute(Collector<Record, A, R> recordCollector, BiFunction<R, ResultSummary, T> finisherWithSummary);
}
