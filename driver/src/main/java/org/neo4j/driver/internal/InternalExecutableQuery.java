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
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.ExecutableQuery;
import org.neo4j.driver.Query;
import org.neo4j.driver.QueryConfig;
import org.neo4j.driver.Record;
import org.neo4j.driver.RoutingControl;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.TransactionCallback;
import org.neo4j.driver.TransactionConfig;

public class InternalExecutableQuery implements ExecutableQuery {
    private final Driver driver;
    private final Query query;
    private final QueryConfig config;

    public InternalExecutableQuery(Driver driver, Query query, QueryConfig config) {
        requireNonNull(driver, "driver must not be null");
        requireNonNull(query, "query must not be null");
        requireNonNull(config, "config must not be null");
        this.driver = driver;
        this.query = query;
        this.config = config;
    }

    @Override
    public ExecutableQuery withParameters(Map<String, Object> parameters) {
        requireNonNull(parameters, "parameters must not be null");
        return new InternalExecutableQuery(driver, query.withParameters(parameters), config);
    }

    @Override
    public ExecutableQuery withConfig(QueryConfig config) {
        requireNonNull(config, "config must not be null");
        return new InternalExecutableQuery(driver, query, config);
    }

    @Override
    public <A, R, T> T execute(Collector<Record, A, R> recordCollector, ResultFinisher<R, T> resultFinisher) {
        var sessionConfigBuilder = SessionConfig.builder();
        config.database().ifPresent(sessionConfigBuilder::withDatabase);
        config.impersonatedUser().ifPresent(sessionConfigBuilder::withImpersonatedUser);
        config.bookmarkManager(driver.executableQueryBookmarkManager())
                .ifPresent(sessionConfigBuilder::withBookmarkManager);
        var supplier = recordCollector.supplier();
        var accumulator = recordCollector.accumulator();
        var finisher = recordCollector.finisher();
        try (var session = (InternalSession) driver.session(sessionConfigBuilder.build())) {
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
            var accessMode = config.routing().equals(RoutingControl.WRITE) ? AccessMode.WRITE : AccessMode.READ;
            return session.execute(accessMode, txCallback, TransactionConfig.empty(), false);
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
