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
package org.neo4j.driver.internal;

import static java.util.Objects.requireNonNull;

import java.util.Map;
import java.util.stream.Collector;
import org.neo4j.bolt.connection.TelemetryApi;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.Driver;
import org.neo4j.driver.ExecutableQuery;
import org.neo4j.driver.Query;
import org.neo4j.driver.QueryConfig;
import org.neo4j.driver.Record;
import org.neo4j.driver.RoutingControl;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.TransactionCallback;
import org.neo4j.driver.TransactionConfig;

public class InternalExecutableQuery implements ExecutableQuery {
    private final Driver driver;
    private final Query query;
    private final QueryConfig config;
    private final AuthToken authToken;

    public InternalExecutableQuery(Driver driver, Query query, QueryConfig config, AuthToken authToken) {
        requireNonNull(driver, "driver must not be null");
        requireNonNull(query, "query must not be null");
        requireNonNull(config, "config must not be null");
        this.driver = driver;
        this.query = query;
        this.config = config;
        this.authToken = authToken;
    }

    @Override
    public ExecutableQuery withParameters(Map<String, Object> parameters) {
        requireNonNull(parameters, "parameters must not be null");
        return new InternalExecutableQuery(driver, query.withParameters(parameters), config, authToken);
    }

    @Override
    public ExecutableQuery withConfig(QueryConfig config) {
        requireNonNull(config, "config must not be null");
        return new InternalExecutableQuery(driver, query, config, authToken);
    }

    @Override
    public ExecutableQuery withAuthToken(AuthToken authToken) {
        return new InternalExecutableQuery(driver, query, config, authToken);
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
        try (var session = (InternalSession) driver.session(Session.class, sessionConfigBuilder.build(), authToken)) {
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
            var transactionConfigBuilder = TransactionConfig.builder();
            config.timeout().ifPresent(transactionConfigBuilder::withTimeout);
            transactionConfigBuilder.withMetadata(config.metadata());
            return session.execute(
                    accessMode, txCallback, transactionConfigBuilder.build(), TelemetryApi.EXECUTABLE_QUERY, false);
        }
    }

    // For testing only
    Driver driver() {
        return driver;
    }

    // For testing only
    String query() {
        return query.text();
    }

    // For testing only
    Map<String, Object> parameters() {
        return query.parameters().asMap();
    }

    // For testing only
    QueryConfig config() {
        return config;
    }

    // For testing only
    AuthToken authToken() {
        return authToken;
    }
}
