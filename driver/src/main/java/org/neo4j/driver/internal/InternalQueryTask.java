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
package org.neo4j.driver.internal;

import static java.util.Objects.requireNonNull;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import org.neo4j.driver.Driver;
import org.neo4j.driver.EagerResult;
import org.neo4j.driver.Query;
import org.neo4j.driver.QueryConfig;
import org.neo4j.driver.QueryTask;
import org.neo4j.driver.Record;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.TransactionCallback;
import org.neo4j.driver.summary.ResultSummary;

public class InternalQueryTask implements QueryTask {
    private static final BiFunction<List<Record>, ResultSummary, EagerResult> EAGER_RESULT_FINISHER =
            (records, summary) -> {
                var keys = records.stream().findFirst().map(Record::keys).orElseGet(Collections::emptyList);
                return new EagerResultValue(keys, records, summary);
            };
    private final Driver driver;
    private final Query query;
    private final QueryConfig config;

    public InternalQueryTask(Driver driver, Query query, QueryConfig config) {
        requireNonNull(driver, "driver must not be null");
        requireNonNull(query, "query must not be null");
        requireNonNull(config, "config must not be null");
        this.driver = driver;
        this.query = query;
        this.config = config;
    }

    @Override
    public QueryTask withParameters(Map<String, Object> parameters) {
        requireNonNull(parameters, "parameters must not be null");
        return new InternalQueryTask(driver, query.withParameters(parameters), config);
    }

    @Override
    public QueryTask withConfig(QueryConfig config) {
        requireNonNull(config, "config must not be null");
        return new InternalQueryTask(driver, query, config);
    }

    @Override
    public EagerResult execute() {
        return execute(Collectors.toList(), EAGER_RESULT_FINISHER);
    }

    @Override
    public <A, R, T> T execute(
            Collector<Record, A, R> recordCollector, BiFunction<R, ResultSummary, T> finisherWithSummary) {
        var sessionConfigBuilder = SessionConfig.builder();
        config.database().ifPresent(sessionConfigBuilder::withDatabase);
        config.impersonatedUser().ifPresent(sessionConfigBuilder::withImpersonatedUser);
        config.bookmarkManager(driver.queryBookmarkManager()).ifPresent(sessionConfigBuilder::withBookmarkManager);
        var supplier = recordCollector.supplier();
        var accumulator = recordCollector.accumulator();
        var finisher = recordCollector.finisher();
        try (var session = driver.session(sessionConfigBuilder.build())) {
            TransactionCallback<T> txCallback = tx -> {
                var result = tx.run(query);
                var container = supplier.get();
                while (result.hasNext()) {
                    accumulator.accept(container, result.next());
                }
                var finishedValue = finisher.apply(container);
                var summary = result.consume();
                return finisherWithSummary.apply(finishedValue, summary);
            };
            return switch (config.routing()) {
                case WRITERS -> session.executeWrite(txCallback);
                case READERS -> session.executeRead(txCallback);
            };
        }
    }

    // For testing only
    public Driver driver() {
        return driver;
    }

    // For testing only
    public String query() {
        return query.text();
    }

    // For testing only
    public Map<String, Object> parameters() {
        return query.parameters().asMap();
    }

    // For testing only
    public QueryConfig config() {
        return config;
    }
}
