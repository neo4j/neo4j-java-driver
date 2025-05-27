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
package neo4j.org.testkit.backend.handler;

import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Stream;
import neo4j.org.testkit.backend.request.WorkloadRequest;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.QueryConfig;
import org.neo4j.driver.RoutingControl;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.SimpleQueryRunner;
import org.neo4j.driver.TransactionCallback;

public class WorkloadHandler {
    private static final System.Logger LOGGER = System.getLogger(WorkloadHandler.class.getName());
    private final Driver driver;
    private final Executor executor;

    public WorkloadHandler(Driver driver, Executor executor) {
        this.driver = Objects.requireNonNull(driver);
        this.executor = Objects.requireNonNull(executor);
    }

    public CompletionStage<FullHttpResponse> handle(HttpVersion httpVersion, WorkloadRequest workloadRequest) {
        return CompletableFuture.completedStage(null)
                .thenComposeAsync(
                        ignored -> switch (workloadRequest.getMethod()) {
                            case "executeQuery" -> executeQuery(workloadRequest);
                            case "sessionRun" -> sessionRun(workloadRequest);
                            case "executeRead", "executeWrite" -> execute(workloadRequest);
                            default -> CompletableFuture.failedStage(
                                    new IllegalArgumentException("Unknown workload type."));
                        },
                        executor)
                .handle((ignored, throwable) -> {
                    HttpResponseStatus status;
                    if (throwable != null) {
                        LOGGER.log(System.Logger.Level.ERROR, "An error occured during workload handling.", throwable);
                        status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
                    } else {
                        status = HttpResponseStatus.NO_CONTENT;
                    }
                    return new DefaultFullHttpResponse(httpVersion, status);
                });
    }

    private CompletionStage<Void> executeQuery(WorkloadRequest workloadRequest) {
        var routingControl =
                switch (workloadRequest.getRouting()) {
                    case "read" -> RoutingControl.READ;
                    case "write" -> RoutingControl.WRITE;
                    default -> null;
                };
        if (routingControl == null) {
            return CompletableFuture.failedStage(new IllegalArgumentException("Unknown routing."));
        }
        return switch (workloadRequest.getMode()) {
            case "sequentialSessions" -> runAsStage(() -> executeQueriesSequentially(
                    workloadRequest.getQueries(), workloadRequest.getDatabase(), routingControl));
            case "parallelSessions" -> executeQueriesConcurrently(
                    workloadRequest.getQueries(), workloadRequest.getDatabase(), routingControl);
            default -> CompletableFuture.failedStage(new IllegalArgumentException("Unknown workload type."));
        };
    }

    private void executeQueriesSequentially(
            List<WorkloadRequest.Query> queries, String database, RoutingControl routingControl) {
        for (var query : queries) {
            executeQuery(query.getText(), query.getParameters(), database, routingControl);
        }
    }

    private CompletionStage<Void> executeQueriesConcurrently(
            List<WorkloadRequest.Query> queries, String database, RoutingControl routingControl) {
        return runAsStage(
                queries.stream()
                        .map(query ->
                                () -> executeQuery(query.getText(), query.getParameters(), database, routingControl)),
                executor);
    }

    @SuppressWarnings("unchecked")
    private void executeQuery(String query, Map<String, ?> parameters, String database, RoutingControl routingControl) {
        var configBuilder = QueryConfig.builder().withRouting(routingControl);
        if (database != null) {
            configBuilder.withDatabase(database);
        }
        driver.executableQuery(query)
                .withParameters((Map<String, Object>) parameters)
                .withConfig(configBuilder.build())
                .execute();
    }

    private CompletionStage<Void> sessionRun(WorkloadRequest workloadRequest) {
        var accessMode =
                switch (workloadRequest.getRouting()) {
                    case "read" -> AccessMode.READ;
                    case "write" -> AccessMode.WRITE;
                    default -> null;
                };
        if (accessMode == null) {
            return CompletableFuture.failedStage(new IllegalArgumentException("Unknown routing."));
        }
        return switch (workloadRequest.getMode()) {
            case "sequentialSessions" -> runAsStage(() ->
                    runInMultipleSessions(workloadRequest.getQueries(), workloadRequest.getDatabase(), accessMode));
            case "sequentialTransactions" -> runAsStage(
                    () -> runInSingleSession(workloadRequest.getQueries(), workloadRequest.getDatabase(), accessMode));
            case "parallelSessions" -> runInConcurrentSessions(
                    workloadRequest.getQueries(), workloadRequest.getDatabase(), accessMode);
            default -> CompletableFuture.failedStage(new IllegalArgumentException("Unknown workload type."));
        };
    }

    private void runInMultipleSessions(List<WorkloadRequest.Query> queries, String database, AccessMode accessMode) {
        for (var query : queries) {
            try (var session = driver.session(sessionConfigBuilder(database)
                    .withDefaultAccessMode(accessMode)
                    .build())) {
                run(session, query.getText(), query.getParameters());
            }
        }
    }

    private void runInSingleSession(List<WorkloadRequest.Query> queries, String database, AccessMode accessMode) {
        try (var session = driver.session(
                sessionConfigBuilder(database).withDefaultAccessMode(accessMode).build())) {
            for (var query : queries) {
                run(session, query.getText(), query.getParameters());
            }
        }
    }

    private CompletionStage<Void> runInConcurrentSessions(
            List<WorkloadRequest.Query> queries, String database, AccessMode accessMode) {
        return runAsStage(
                queries.stream()
                        .map(query -> () -> runInSingleSession(Collections.singletonList(query), database, accessMode)),
                executor);
    }

    private void run(SimpleQueryRunner queryRunner, String query, Map<String, ?> parameters) {
        @SuppressWarnings("unchecked")
        var result = queryRunner.run(query, (Map<String, Object>) parameters);
        while (result.hasNext()) {
            result.next();
        }
    }

    private CompletionStage<Void> execute(WorkloadRequest workloadRequest) {
        BiConsumer<Session, TransactionCallback<Void>> runner =
                switch (workloadRequest.getRouting()) {
                    case "read" -> Session::executeRead;
                    case "write" -> Session::executeWrite;
                    default -> null;
                };
        if (runner == null) {
            return CompletableFuture.failedStage(new IllegalArgumentException("Unknown routing."));
        }
        return switch (workloadRequest.getMode()) {
            case "sequentialSessions" -> runAsStage(() ->
                    executeInMultipleSessions(runner, workloadRequest.getQueries(), workloadRequest.getDatabase()));
            case "sequentialTransactions" -> runAsStage(
                    () -> executeSingleSession(runner, workloadRequest.getQueries(), workloadRequest.getDatabase()));
            case "sequentialQueries" -> runAsStage(() ->
                    executeInSingleTransaction(runner, workloadRequest.getQueries(), workloadRequest.getDatabase()));
            case "parallelSessions" -> executeConcurrently(
                    runner, workloadRequest.getQueries(), workloadRequest.getDatabase());
            default -> CompletableFuture.failedStage(new IllegalArgumentException("Unknown workload type."));
        };
    }

    private void executeInMultipleSessions(
            BiConsumer<Session, TransactionCallback<Void>> runner,
            List<WorkloadRequest.Query> queries,
            String database) {
        for (var query : queries) {
            try (var session = driver.session(sessionConfigBuilder(database).build())) {
                runner.accept(session, tx -> {
                    run(tx, query.getText(), query.getParameters());
                    return null;
                });
            }
        }
    }

    private void executeSingleSession(
            BiConsumer<Session, TransactionCallback<Void>> runner,
            List<WorkloadRequest.Query> queries,
            String database) {
        try (var session = driver.session(sessionConfigBuilder(database).build())) {
            for (var query : queries) {
                runner.accept(session, tx -> {
                    run(tx, query.getText(), query.getParameters());
                    return null;
                });
            }
        }
    }

    private void executeInSingleTransaction(
            BiConsumer<Session, TransactionCallback<Void>> runner,
            List<WorkloadRequest.Query> queries,
            String database) {
        var configBuilder = SessionConfig.builder();
        if (database != null) {
            configBuilder.withDatabase(database);
        }
        try (var session = driver.session(configBuilder.build())) {
            runner.accept(session, tx -> {
                for (var query : queries) {
                    run(tx, query.getText(), query.getParameters());
                }
                return null;
            });
        }
    }

    private CompletionStage<Void> executeConcurrently(
            BiConsumer<Session, TransactionCallback<Void>> runner,
            List<WorkloadRequest.Query> queries,
            String database) {
        return runAsStage(
                queries.stream()
                        .map(query -> () -> executeSingleSession(runner, Collections.singletonList(query), database)),
                executor);
    }

    private CompletionStage<Void> runAsStage(Runnable runnable) {
        var future = new CompletableFuture<Void>();
        try {
            runnable.run();
            future.complete(null);
        } catch (Throwable throwable) {
            future.completeExceptionally(throwable);
        }
        return future;
    }

    private CompletionStage<Void> runAsStage(Stream<Runnable> runnables, Executor executor) {
        return CompletableFuture.allOf(runnables
                        .map(runnable -> CompletableFuture.runAsync(runnable, executor))
                        .toArray(CompletableFuture[]::new))
                .orTimeout(1, TimeUnit.MINUTES);
    }

    private SessionConfig.Builder sessionConfigBuilder(String database) {
        var configBuilder = SessionConfig.builder();
        if (database != null) {
            configBuilder.withDatabase(database);
        }
        return configBuilder;
    }
}
