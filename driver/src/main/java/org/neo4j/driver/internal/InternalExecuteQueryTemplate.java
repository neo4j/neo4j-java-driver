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

import java.util.Map;
import java.util.stream.Collector;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Query;
import org.neo4j.driver.ExecuteQueryConfig;
import org.neo4j.driver.ExecuteQueryTemplate;
import org.neo4j.driver.Record;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.TransactionCallback;

public class InternalExecuteQueryTemplate implements ExecuteQueryTemplate {
    private final Driver driver;
    private final Query query;
    private final ExecuteQueryConfig config;

    public InternalExecuteQueryTemplate(Driver driver, Query query, ExecuteQueryConfig config) {
        requireNonNull(driver, "driver must not be null");
        requireNonNull(query, "query must not be null");
        requireNonNull(config, "config must not be null");
        this.driver = driver;
        this.query = query;
        this.config = config;
    }

    @Override
    public ExecuteQueryTemplate withParameters(Map<String, Object> parameters) {
        requireNonNull(parameters, "parameters must not be null");
        return new InternalExecuteQueryTemplate(driver, query.withParameters(parameters), config);
    }

    @Override
    public ExecuteQueryTemplate withConfig(ExecuteQueryConfig config) {
        requireNonNull(config, "config must not be null");
        return new InternalExecuteQueryTemplate(driver, query, config);
    }

    @Override
    public <A, R, T> T execute(Collector<Record, A, R> recordCollector, ResultFinisher<R, T> resultFinisher) {
        var sessionConfigBuilder = SessionConfig.builder();
        config.database().ifPresent(sessionConfigBuilder::withDatabase);
        config.impersonatedUser().ifPresent(sessionConfigBuilder::withImpersonatedUser);
        config.bookmarkManager(driver.defaultExecuteQueryBookmarkManager()).ifPresent(sessionConfigBuilder::withBookmarkManager);
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
                return resultFinisher.finish(result.keys(), finishedValue, summary);
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
    public ExecuteQueryConfig config() {
        return config;
    }
}
