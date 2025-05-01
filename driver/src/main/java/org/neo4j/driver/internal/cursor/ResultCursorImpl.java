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
package org.neo4j.driver.internal.cursor;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.neo4j.driver.internal.types.InternalTypeSystem.TYPE_SYSTEM;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Function;
import org.neo4j.bolt.connection.BoltProtocolVersion;
import org.neo4j.bolt.connection.message.Messages;
import org.neo4j.bolt.connection.summary.BeginSummary;
import org.neo4j.bolt.connection.summary.RunSummary;
import org.neo4j.bolt.connection.summary.TelemetrySummary;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.NoSuchRecordException;
import org.neo4j.driver.internal.DatabaseBookmark;
import org.neo4j.driver.internal.FailableCursor;
import org.neo4j.driver.internal.GqlStatusError;
import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.adaptedbolt.DriverBoltConnection;
import org.neo4j.driver.internal.adaptedbolt.DriverResponseHandler;
import org.neo4j.driver.internal.adaptedbolt.summary.DiscardSummary;
import org.neo4j.driver.internal.adaptedbolt.summary.PullSummary;
import org.neo4j.driver.internal.async.UnmanagedTransaction;
import org.neo4j.driver.internal.telemetry.ApiTelemetryWork;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.internal.util.MetadataExtractor;
import org.neo4j.driver.summary.ResultSummary;

public class ResultCursorImpl extends AbstractRecordStateResponseHandler
        implements ResultCursor, FailableCursor, DriverResponseHandler {
    public static final MetadataExtractor METADATA_EXTRACTOR = new MetadataExtractor("t_last");
    private static final ClientException IGNORED_ERROR = new ClientException(
            GqlStatusError.UNKNOWN.getStatus(),
            GqlStatusError.UNKNOWN.getStatusDescription("A message has been ignored during result streaming."),
            "N/A",
            "A message has been ignored during result streaming.",
            GqlStatusError.DIAGNOSTIC_RECORD,
            null);
    private final DriverBoltConnection boltConnection;
    private final Queue<Record> records = new ArrayDeque<>();
    private final Query query;
    private final long fetchSize;
    private final Consumer<DatabaseBookmark> bookmarkConsumer;
    private final boolean closeOnSummary;
    private final boolean legacyNotifications;
    private final CompletableFuture<ResultCursorImpl> resultCursorFuture = new CompletableFuture<>();
    private final CompletableFuture<UnmanagedTransaction> beginFuture;
    private final ApiTelemetryWork apiTelemetryWork;
    private final CompletableFuture<Void> consumedFuture = new CompletableFuture<>();
    private final Consumer<String> databaseNameConsumer;
    private RunSummary runSummary;
    private State state;

    private boolean apiCallInProgress;
    private CompletableFuture<Record> peekFuture;
    private CompletableFuture<Record> recordFuture;
    private CompletableFuture<Boolean> secondRecordFuture;
    private CompletableFuture<List<Record>> recordsFuture;
    private boolean keepRecords;
    private CompletableFuture<ResultSummary> summaryFuture;
    private ResultSummary summary;
    private Throwable error;
    private boolean errorExposed;

    private enum State {
        READY,
        STREAMING,
        DISCARDING,
        FAILED,
        SUCCEEDED
    }

    public ResultCursorImpl(
            DriverBoltConnection boltConnection,
            Query query,
            long fetchSize,
            Consumer<DatabaseBookmark> bookmarkConsumer,
            boolean closeOnSummary,
            CompletableFuture<UnmanagedTransaction> beginFuture,
            Consumer<String> databaseNameConsumer,
            ApiTelemetryWork apiTelemetryWork) {
        this.boltConnection = Objects.requireNonNull(boltConnection);
        this.legacyNotifications = new BoltProtocolVersion(5, 5).compareTo(boltConnection.protocolVersion()) > 0;
        updateRecordState(RecordState.REQUESTED);
        this.query = Objects.requireNonNull(query);
        this.fetchSize = fetchSize;
        this.bookmarkConsumer = Objects.requireNonNull(bookmarkConsumer);
        this.closeOnSummary = closeOnSummary;
        this.state = State.STREAMING;
        this.beginFuture = beginFuture;
        this.apiTelemetryWork = apiTelemetryWork;
        this.databaseNameConsumer = Objects.requireNonNull(databaseNameConsumer);
    }

    public CompletionStage<ResultCursorImpl> resultCursor() {
        return resultCursorFuture;
    }

    @Override
    public synchronized List<String> keys() {
        return runSummary.keys();
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    public synchronized CompletionStage<ResultSummary> consumeAsync() {
        if (apiCallInProgress) {
            var message = "API calls to result cursor must be sequential.";
            return CompletableFuture.failedStage(new ClientException(
                    GqlStatusError.UNKNOWN.getStatus(),
                    GqlStatusError.UNKNOWN.getStatusDescription(message),
                    "N/A",
                    message,
                    GqlStatusError.DIAGNOSTIC_RECORD,
                    null));
        }
        CompletionStage<ResultSummary> summaryFt =
                switch (state) {
                    case READY -> {
                        apiCallInProgress = true;
                        summaryFuture = new CompletableFuture<>();
                        var future = summaryFuture;
                        state = State.DISCARDING;
                        boltConnection
                                .writeAndFlush(this, Messages.discard(runSummary.queryId(), -1))
                                .whenComplete((ignored, throwable) -> {
                                    var error = Futures.completionExceptionCause(throwable);
                                    CompletableFuture<ResultSummary> summaryFuture;
                                    if (error != null) {
                                        synchronized (this) {
                                            state = State.FAILED;
                                            errorExposed = true;
                                            summaryFuture = this.summaryFuture;
                                            this.summaryFuture = null;
                                            apiCallInProgress = false;
                                        }
                                        summaryFuture.completeExceptionally(error);
                                    }
                                });
                        yield future;
                    }
                    case STREAMING -> {
                        apiCallInProgress = true;
                        summaryFuture = new CompletableFuture<>();
                        yield summaryFuture;
                    }
                    case DISCARDING -> {
                        var message = "Invalid API call.";
                        yield CompletableFuture.failedStage(new ClientException(
                                GqlStatusError.UNKNOWN.getStatus(),
                                GqlStatusError.UNKNOWN.getStatusDescription(message),
                                "N/A",
                                message,
                                GqlStatusError.DIAGNOSTIC_RECORD,
                                null));
                    }
                    case FAILED -> stageExposingError(METADATA_EXTRACTOR.extractSummary(
                            query,
                            boltConnection,
                            runSummary.resultAvailableAfter(),
                            Collections.emptyMap(),
                            legacyNotifications,
                            null));
                    case SUCCEEDED -> CompletableFuture.completedStage(summary);
                };
        var future = new CompletableFuture<ResultSummary>();
        summaryFt.whenComplete((summary, throwable) -> {
            throwable = Futures.completionExceptionCause(throwable);
            if (throwable != null) {
                consumedFuture.completeExceptionally(throwable);
                future.completeExceptionally(throwable);
            } else {
                consumedFuture.complete(null);
                future.complete(summary);
            }
        });
        return future;
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    public synchronized CompletionStage<Record> nextAsync() {
        if (apiCallInProgress) {
            var message = "API calls to result cursor must be sequential.";
            return CompletableFuture.failedStage(new ClientException(
                    GqlStatusError.UNKNOWN.getStatus(),
                    GqlStatusError.UNKNOWN.getStatusDescription(message),
                    "N/A",
                    message,
                    GqlStatusError.DIAGNOSTIC_RECORD,
                    null));
        }
        var record = records.poll();
        if (record == null) {
            // buffer is empty
            return switch (state) {
                case READY -> {
                    apiCallInProgress = true;
                    recordFuture = new CompletableFuture<>();
                    var result = recordFuture;
                    state = State.STREAMING;
                    updateRecordState(RecordState.NO_RECORD);
                    boltConnection
                            .writeAndFlush(this, Messages.pull(runSummary.queryId(), fetchSize))
                            .whenComplete((ignored, throwable) -> {
                                var error = Futures.completionExceptionCause(throwable);
                                CompletableFuture<Record> recordFuture;
                                if (error != null) {
                                    synchronized (this) {
                                        state = State.FAILED;
                                        errorExposed = true;
                                        recordFuture = this.recordFuture;
                                        this.recordFuture = null;
                                        apiCallInProgress = false;
                                    }
                                    recordFuture.completeExceptionally(error);
                                }
                            });
                    yield result;
                }
                case STREAMING -> {
                    apiCallInProgress = true;
                    recordFuture = new CompletableFuture<>();
                    yield recordFuture;
                }
                case DISCARDING -> {
                    var message = "Invalid API call.";
                    yield CompletableFuture.failedStage(new ClientException(
                            GqlStatusError.UNKNOWN.getStatus(),
                            GqlStatusError.UNKNOWN.getStatusDescription(message),
                            "N/A",
                            message,
                            GqlStatusError.DIAGNOSTIC_RECORD,
                            null));
                }
                case FAILED -> stageExposingError(null);
                case SUCCEEDED -> CompletableFuture.completedStage(null);
            };
        } else {
            return completedFuture(record);
        }
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    public synchronized CompletionStage<Record> peekAsync() {
        if (apiCallInProgress) {
            var message = "API calls to result cursor must be sequential.";
            return CompletableFuture.failedStage(new ClientException(
                    GqlStatusError.UNKNOWN.getStatus(),
                    GqlStatusError.UNKNOWN.getStatusDescription(message),
                    "N/A",
                    message,
                    GqlStatusError.DIAGNOSTIC_RECORD,
                    null));
        }
        var record = records.peek();
        if (record == null) {
            // buffer is empty
            return switch (state) {
                case READY -> {
                    apiCallInProgress = true;
                    peekFuture = new CompletableFuture<>();
                    var future = peekFuture;
                    state = State.STREAMING;
                    updateRecordState(RecordState.NO_RECORD);
                    boltConnection
                            .writeAndFlush(this, Messages.pull(runSummary.queryId(), fetchSize))
                            .whenComplete((ignored, throwable) -> {
                                var error = Futures.completionExceptionCause(throwable);
                                if (error != null) {
                                    CompletableFuture<Record> peekFuture;
                                    synchronized (this) {
                                        state = State.FAILED;
                                        errorExposed = true;
                                        recordFuture = this.peekFuture;
                                        this.peekFuture = null;
                                        apiCallInProgress = false;
                                    }
                                    recordFuture.completeExceptionally(error);
                                }
                            });
                    yield future;
                }
                case STREAMING -> {
                    apiCallInProgress = true;
                    peekFuture = new CompletableFuture<>();
                    yield peekFuture;
                }
                case DISCARDING -> {
                    var message = "Invalid API call.";
                    yield CompletableFuture.failedStage(new ClientException(
                            GqlStatusError.UNKNOWN.getStatus(),
                            GqlStatusError.UNKNOWN.getStatusDescription(message),
                            "N/A",
                            message,
                            GqlStatusError.DIAGNOSTIC_RECORD,
                            null));
                }
                case FAILED -> stageExposingError(null);
                case SUCCEEDED -> CompletableFuture.completedStage(null);
            };
        } else {
            return completedFuture(record);
        }
    }

    @Override
    public synchronized CompletionStage<Record> singleAsync() {
        if (apiCallInProgress) {
            var message = "API calls to result cursor must be sequential.";
            return CompletableFuture.failedStage(new ClientException(
                    GqlStatusError.UNKNOWN.getStatus(),
                    GqlStatusError.UNKNOWN.getStatusDescription(message),
                    "N/A",
                    message,
                    GqlStatusError.DIAGNOSTIC_RECORD,
                    null));
        }
        if (records.size() > 1) {
            records.clear();
            return CompletableFuture.failedStage(
                    new NoSuchRecordException(
                            "Expected a result with a single record, but this result contains at least one more. Ensure your query returns only one record."));
        } else {
            return switch (state) {
                case READY -> {
                    if (records.isEmpty()) {
                        apiCallInProgress = true;
                        recordFuture = new CompletableFuture<>();
                        secondRecordFuture = new CompletableFuture<>();
                        var singleFuture = recordFuture.thenCompose(firstRecord -> {
                            if (firstRecord == null) {
                                throw new NoSuchRecordException(
                                        "Cannot retrieve a single record, because this result is empty.");
                            }
                            return secondRecordFuture.thenApply(secondRecord -> {
                                if (secondRecord) {
                                    throw new NoSuchRecordException(
                                            "Expected a result with a single record, but this result contains at least one more. Ensure your query returns only one record.");
                                }
                                return firstRecord;
                            });
                        });
                        state = State.STREAMING;
                        updateRecordState(RecordState.NO_RECORD);
                        boltConnection
                                .writeAndFlush(this, Messages.pull(runSummary.queryId(), fetchSize))
                                .whenComplete((ignored, throwable) -> {
                                    var error = Futures.completionExceptionCause(throwable);
                                    if (error != null) {
                                        CompletableFuture<Record> recordFuture;
                                        CompletableFuture<Boolean> secondRecordFuture;
                                        synchronized (this) {
                                            state = State.FAILED;
                                            errorExposed = true;
                                            recordFuture = this.recordFuture;
                                            this.recordFuture = null;
                                            secondRecordFuture = this.secondRecordFuture;
                                            this.secondRecordFuture = null;
                                            apiCallInProgress = false;
                                        }
                                        recordFuture.completeExceptionally(error);
                                        secondRecordFuture.completeExceptionally(error);
                                    }
                                });
                        yield singleFuture;
                    } else {
                        // records is not empty and the state is READY, meaning the result is not exhausted
                        yield CompletableFuture.failedStage(
                                new NoSuchRecordException(
                                        "Expected a result with a single record, but this result contains at least one more. Ensure your query returns only one record."));
                    }
                }
                case STREAMING -> {
                    apiCallInProgress = true;
                    if (records.isEmpty()) {
                        recordFuture = new CompletableFuture<>();
                        secondRecordFuture = new CompletableFuture<>();
                        yield recordFuture.thenCompose(firstRecord -> {
                            if (firstRecord == null) {
                                throw new NoSuchRecordException(
                                        "Cannot retrieve a single record, because this result is empty.");
                            }
                            return secondRecordFuture.thenApply(secondRecord -> {
                                if (secondRecord) {
                                    throw new NoSuchRecordException(
                                            "Expected a result with a single record, but this result contains at least one more. Ensure your query returns only one record.");
                                }
                                return firstRecord;
                            });
                        });
                    } else {
                        var firstRecord = records.poll();
                        secondRecordFuture = new CompletableFuture<>();
                        yield secondRecordFuture.thenApply(secondRecord -> {
                            if (secondRecord) {
                                throw new NoSuchRecordException(
                                        "Expected a result with a single record, but this result contains at least one more. Ensure your query returns only one record.");
                            }
                            return firstRecord;
                        });
                    }
                }
                case DISCARDING -> {
                    var message = "Invalid API call.";
                    yield CompletableFuture.failedStage(new ClientException(
                            GqlStatusError.UNKNOWN.getStatus(),
                            GqlStatusError.UNKNOWN.getStatusDescription(message),
                            "N/A",
                            message,
                            GqlStatusError.DIAGNOSTIC_RECORD,
                            null));
                }
                case FAILED -> stageExposingError(null).thenApply(ignored -> {
                    throw new NoSuchRecordException("Cannot retrieve a single record, because this result is empty.");
                });
                case SUCCEEDED -> records.size() == 1
                        ? CompletableFuture.completedFuture(records.poll())
                        : CompletableFuture.failedStage(new NoSuchRecordException(
                                "Cannot retrieve a single record, because this result is empty."));
            };
        }
    }

    @Override
    public synchronized CompletionStage<ResultSummary> forEachAsync(Consumer<Record> action) {
        if (apiCallInProgress) {
            var message = "API calls to result cursor must be sequential.";
            return CompletableFuture.failedStage(new ClientException(
                    GqlStatusError.UNKNOWN.getStatus(),
                    GqlStatusError.UNKNOWN.getStatusDescription(message),
                    "N/A",
                    message,
                    GqlStatusError.DIAGNOSTIC_RECORD,
                    null));
        }
        var summaryFuture = new CompletableFuture<ResultSummary>();
        return switch (state) {
            case READY, STREAMING, DISCARDING -> {
                this.summaryFuture = summaryFuture;
                yield listAsync().thenCompose(list -> {
                    list.forEach(action);
                    return summaryFuture;
                });
            }
            case FAILED -> listAsync().thenApply(ignored -> null);
            case SUCCEEDED -> listAsync().thenApply(list -> {
                list.forEach(action);
                return summary;
            });
        };
    }

    @Override
    @SuppressWarnings("DuplicatedCode")
    public synchronized CompletionStage<List<Record>> listAsync() {
        if (apiCallInProgress) {
            var message = "API calls to result cursor must be sequential.";
            return CompletableFuture.failedStage(new ClientException(
                    GqlStatusError.UNKNOWN.getStatus(),
                    GqlStatusError.UNKNOWN.getStatusDescription(message),
                    "N/A",
                    message,
                    GqlStatusError.DIAGNOSTIC_RECORD,
                    null));
        }
        return switch (state) {
            case READY -> {
                apiCallInProgress = true;
                recordsFuture = new CompletableFuture<>();
                var future = recordsFuture;
                state = State.STREAMING;
                updateRecordState(RecordState.NO_RECORD);
                boltConnection
                        .writeAndFlush(this, Messages.pull(runSummary.queryId(), -1))
                        .whenComplete((ignored, throwable) -> {
                            var error = Futures.completionExceptionCause(throwable);
                            CompletableFuture<List<Record>> recordsFuture;
                            if (error != null) {
                                synchronized (this) {
                                    state = State.FAILED;
                                    errorExposed = true;
                                    recordsFuture = this.recordsFuture;
                                    this.recordsFuture = null;
                                    apiCallInProgress = false;
                                }
                                recordsFuture.completeExceptionally(error);
                            }
                        });
                yield future;
            }
            case STREAMING -> {
                apiCallInProgress = true;
                recordsFuture = new CompletableFuture<>();
                yield recordsFuture;
            }
            case DISCARDING -> {
                var message = "Invalid API call.";
                yield CompletableFuture.failedStage(new ClientException(
                        GqlStatusError.UNKNOWN.getStatus(),
                        GqlStatusError.UNKNOWN.getStatusDescription(message),
                        "N/A",
                        message,
                        GqlStatusError.DIAGNOSTIC_RECORD,
                        null));
            }
            case FAILED -> stageExposingError(null).thenApply(ignored -> Collections.emptyList());
            case SUCCEEDED -> {
                var records = this.records.stream().toList();
                this.records.clear();
                yield CompletableFuture.completedStage(records);
            }
        };
    }

    @Override
    public <T> CompletionStage<List<T>> listAsync(Function<Record, T> mapFunction) {
        return listAsync().thenApply(list -> list.stream().map(mapFunction).toList());
    }

    @Override
    public CompletionStage<Boolean> isOpenAsync() {
        if (apiCallInProgress) {
            var message = "API calls to result cursor must be sequential.";
            return CompletableFuture.failedStage(new ClientException(
                    GqlStatusError.UNKNOWN.getStatus(),
                    GqlStatusError.UNKNOWN.getStatusDescription(message),
                    "N/A",
                    message,
                    GqlStatusError.DIAGNOSTIC_RECORD,
                    null));
        }
        return switch (state) {
            case READY, STREAMING, DISCARDING -> CompletableFuture.completedStage(true);
            case FAILED, SUCCEEDED -> CompletableFuture.completedStage(false);
        };
    }

    @Override
    public void onTelemetrySummary(TelemetrySummary summary) {
        if (apiTelemetryWork != null) {
            apiTelemetryWork.acknowledge();
        }
    }

    @Override
    public void onBeginSummary(BeginSummary summary) {
        if (beginFuture != null) {
            summary.databaseName().ifPresent(databaseNameConsumer);
            beginFuture.complete(null);
        }
    }

    @Override
    public void onRunSummary(RunSummary summary) {
        synchronized (this) {
            runSummary = summary;
        }
        summary.databaseName().ifPresent(databaseNameConsumer);
        resultCursorFuture.complete(this);
    }

    @Override
    public void onRecord(Value[] fields) {
        var record = new InternalRecord(runSummary.keys(), fields);
        CompletableFuture<Record> peekFuture;
        CompletableFuture<Record> recordFuture = null;
        CompletableFuture<Boolean> secondRecordFuture = null;
        synchronized (this) {
            updateRecordState(RecordState.HAD_RECORD);
            peekFuture = this.peekFuture;
            this.peekFuture = null;
            if (peekFuture != null) {
                apiCallInProgress = false;
                records.add(record);
            } else {
                recordFuture = this.recordFuture;
                this.recordFuture = null;

                secondRecordFuture = this.secondRecordFuture;
                if (recordFuture == null) {
                    if (secondRecordFuture != null) {
                        apiCallInProgress = false;
                        this.secondRecordFuture = null;
                    }
                    records.add(record);
                } else {
                    if (secondRecordFuture == null) {
                        apiCallInProgress = false;
                    }
                }
            }
        }
        if (peekFuture != null) {
            peekFuture.complete(record);
        } else if (recordFuture != null) {
            recordFuture.complete(record);
        } else if (secondRecordFuture != null) {
            secondRecordFuture.complete(true);
        }
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    public synchronized void onError(Throwable throwable) {
        throwable = Futures.completionExceptionCause(throwable);
        if (error == null) {
            error = throwable;
        } else {
            if (throwable == IGNORED_ERROR) {
                return;
            }
            if (error == IGNORED_ERROR || (error instanceof Neo4jException && !(throwable instanceof Neo4jException))) {
                // higher order error has occurred
                error = throwable;
            }
        }
    }

    @Override
    public void onIgnored() {
        onError(IGNORED_ERROR);
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    public void onDiscardSummary(DiscardSummary summary) {
        synchronized (this) {
            CompletableFuture<Record> peekFuture;
            CompletableFuture<Record> recordFuture = null;
            CompletableFuture<Boolean> secondRecordFuture = null;
            Runnable recordsFutureRunnable = null;
            CompletableFuture<ResultSummary> summaryFuture = null;
            Throwable summaryError = null;
            synchronized (this) {
                try {
                    this.summary = METADATA_EXTRACTOR.extractSummary(
                            query,
                            boltConnection,
                            -1,
                            summary.metadata(),
                            legacyNotifications,
                            generateGqlStatusObject(runSummary.keys()));
                    state = State.SUCCEEDED;
                } catch (Throwable throwable) {
                    summaryError = throwable;
                }
                peekFuture = this.peekFuture;
                this.peekFuture = null;
                if (peekFuture != null) {
                    // peek is pending
                    apiCallInProgress = false;
                } else {
                    recordFuture = this.recordFuture;
                    this.recordFuture = null;
                    if (recordFuture != null) {
                        // next is pending
                        apiCallInProgress = false;
                    } else {
                        secondRecordFuture = this.secondRecordFuture;
                        this.secondRecordFuture = null;

                        if (secondRecordFuture != null) {
                            // single is pending
                            apiCallInProgress = false;
                        } else {
                            if (this.recordsFuture != null) {
                                // list is pending
                                apiCallInProgress = false;
                                var recordsFuture = this.recordsFuture;
                                this.recordsFuture = null;
                                var records = this.records.stream().toList();
                                this.records.clear();
                                recordsFutureRunnable = () -> recordsFuture.complete(records);
                            } else if (this.summaryFuture != null) {
                                // consume is pending
                                apiCallInProgress = false;
                                summaryFuture = this.summaryFuture;
                                this.summaryFuture = null;
                            }
                        }
                    }
                }
            }
            if (summaryError == null) {
                if (closeOnSummary) {
                    var recordFutureSnapshot = recordFuture;
                    var secondRecordFutureSnapshot = secondRecordFuture;
                    var recordsFutureRunnableSnapshot = recordsFutureRunnable;
                    var summaryFutureSnapshot = summaryFuture;
                    boltConnection.close().whenComplete((ignored, throwable) -> {
                        if (peekFuture != null) {
                            peekFuture.complete(null);
                        }
                        if (recordFutureSnapshot != null) {
                            recordFutureSnapshot.complete(null);
                        } else if (secondRecordFutureSnapshot != null) {
                            secondRecordFutureSnapshot.complete(false);
                        } else if (recordsFutureRunnableSnapshot != null) {
                            recordsFutureRunnableSnapshot.run();
                        } else if (summaryFutureSnapshot != null) {
                            summaryFutureSnapshot.complete(this.summary);
                        }
                    });
                } else {
                    if (peekFuture != null) {
                        peekFuture.complete(null);
                    }
                    if (recordFuture != null) {
                        recordFuture.complete(null);
                    } else if (secondRecordFuture != null) {
                        secondRecordFuture.complete(false);
                    } else if (recordsFutureRunnable != null) {
                        recordsFutureRunnable.run();
                    } else if (summaryFuture != null) {
                        summaryFuture.complete(this.summary);
                    }
                }
            } else {
                onError(summaryError);
            }
        }
    }

    @SuppressWarnings({"DuplicatedCode", "DataFlowIssue"})
    @Override
    public void onPullSummary(PullSummary summary) {
        if (summary.hasMore()) {
            CompletableFuture<Boolean> secondRecordFuture = null;
            synchronized (this) {
                if (this.peekFuture != null) {
                    // peek is pending, keep streaming
                    state = State.STREAMING;
                    updateRecordState(RecordState.NO_RECORD);
                    boltConnection
                            .writeAndFlush(this, Messages.pull(runSummary.queryId(), fetchSize))
                            .whenComplete((ignored, throwable) -> {
                                var error = Futures.completionExceptionCause(throwable);
                                if (error != null) {
                                    CompletableFuture<Record> peekFuture;
                                    synchronized (this) {
                                        state = State.FAILED;
                                        errorExposed = true;
                                        peekFuture = this.peekFuture;
                                        this.peekFuture = null;
                                        apiCallInProgress = false;
                                    }
                                    peekFuture.completeExceptionally(error);
                                }
                            });
                } else if (this.recordFuture != null) {
                    // next is pending, keep streaming
                    state = State.STREAMING;
                    updateRecordState(RecordState.NO_RECORD);
                    boltConnection
                            .writeAndFlush(this, Messages.pull(runSummary.queryId(), fetchSize))
                            .whenComplete((ignored, throwable) -> {
                                var error = Futures.completionExceptionCause(throwable);
                                if (error != null) {
                                    CompletableFuture<Record> recordFuture;
                                    synchronized (this) {
                                        state = State.FAILED;
                                        errorExposed = true;
                                        recordFuture = this.recordFuture;
                                        this.recordFuture = null;
                                        apiCallInProgress = false;
                                    }
                                    recordFuture.completeExceptionally(error);
                                }
                            });
                } else {
                    secondRecordFuture = this.secondRecordFuture;
                    this.secondRecordFuture = null;

                    if (secondRecordFuture != null) {
                        // single is pending
                        apiCallInProgress = false;
                        state = State.READY;
                    } else {
                        if (this.recordsFuture != null) {
                            // list is pending, stream all
                            state = State.STREAMING;
                            updateRecordState(RecordState.NO_RECORD);
                            boltConnection
                                    .writeAndFlush(this, Messages.pull(runSummary.queryId(), -1))
                                    .whenComplete((ignored, throwable) -> {
                                        var error = Futures.completionExceptionCause(throwable);
                                        if (error != null) {
                                            CompletableFuture<List<Record>> recordsFuture;
                                            synchronized (this) {
                                                state = State.FAILED;
                                                errorExposed = true;
                                                recordsFuture = this.recordsFuture;
                                                this.recordsFuture = null;
                                                apiCallInProgress = false;
                                            }
                                            recordsFuture.completeExceptionally(error);
                                        }
                                    });
                        } else if (this.summaryFuture != null) {
                            // consume is pending, discard all
                            state = State.DISCARDING;
                            boltConnection
                                    .writeAndFlush(this, Messages.discard(runSummary.queryId(), -1))
                                    .whenComplete((ignored, throwable) -> {
                                        var error = Futures.completionExceptionCause(throwable);
                                        CompletableFuture<ResultSummary> summaryFuture;
                                        if (error != null) {
                                            synchronized (this) {
                                                state = State.FAILED;
                                                errorExposed = true;
                                                summaryFuture = this.summaryFuture;
                                                this.summaryFuture = null;
                                                apiCallInProgress = false;
                                            }
                                            summaryFuture.completeExceptionally(error);
                                        }
                                    });
                        } else {
                            state = State.READY;
                        }
                    }
                }
            }
            if (secondRecordFuture != null) {
                secondRecordFuture.complete(true);
            }
        } else {
            CompletableFuture<Record> peekFuture;
            CompletableFuture<Record> recordFuture = null;
            CompletableFuture<Boolean> secondRecordFuture = null;
            Runnable recordsFutureRunnable = null;
            CompletableFuture<ResultSummary> summaryFuture = null;
            DatabaseBookmark databaseBookmark = null;
            Throwable error = null;
            synchronized (this) {
                state = State.SUCCEEDED;
                updateRecordState(RecordState.NO_RECORD);
                try {
                    this.summary = METADATA_EXTRACTOR.extractSummary(
                            query,
                            boltConnection,
                            runSummary.resultAvailableAfter(),
                            summary.metadata(),
                            legacyNotifications,
                            generateGqlStatusObject(runSummary.keys()));
                } catch (Throwable throwable) {
                    error = throwable;
                    this.error = throwable;
                    state = State.FAILED;
                }
                var metadata = summary.metadata();
                var bookmarkValue = metadata.get("bookmark");
                if (bookmarkValue != null && !bookmarkValue.isNull() && bookmarkValue.hasType(TYPE_SYSTEM.STRING())) {
                    var bookmarkStr = bookmarkValue.asString();
                    if (!bookmarkStr.isEmpty()) {
                        databaseBookmark = new DatabaseBookmark(null, Bookmark.from(bookmarkStr));
                    }
                }
                peekFuture = this.peekFuture;
                this.peekFuture = null;
                if (peekFuture != null) {
                    // peek is pending
                    apiCallInProgress = false;
                    error = this.error;
                    errorExposed = true;
                } else {
                    recordFuture = this.recordFuture;
                    this.recordFuture = null;
                    if (recordFuture != null) {
                        // peek is pending
                        apiCallInProgress = false;
                        error = this.error;
                        errorExposed = true;
                    } else {
                        secondRecordFuture = this.secondRecordFuture;
                        this.secondRecordFuture = null;

                        if (secondRecordFuture != null) {
                            // single is pending
                            apiCallInProgress = false;
                            error = this.error;
                            errorExposed = true;
                        } else {
                            if (this.recordsFuture != null) {
                                if (this.summaryFuture == null) {
                                    // list is pending
                                    apiCallInProgress = false;
                                    if (this.error == null) {
                                        var recordsFuture = this.recordsFuture;
                                        this.recordsFuture = null;
                                        var records = this.records.stream().toList();
                                        if (!keepRecords) {
                                            this.records.clear();
                                        }
                                        keepRecords = false;
                                        recordsFutureRunnable = () -> recordsFuture.complete(records);
                                    } else {
                                        recordsFutureRunnable = () -> recordsFuture.completeExceptionally(this.error);
                                        errorExposed = true;
                                    }
                                } else {
                                    // for-each is pending
                                    apiCallInProgress = false;
                                    summaryFuture = this.summaryFuture;
                                    this.summaryFuture = null;
                                    if (this.error == null) {
                                        var recordsFuture = this.recordsFuture;
                                        this.recordsFuture = null;
                                        var records = this.records.stream().toList();
                                        this.records.clear();
                                        recordsFutureRunnable = () -> recordsFuture.complete(records);
                                    } else {
                                        error = this.error;
                                        errorExposed = true;
                                    }
                                }
                            } else if (this.summaryFuture != null) {
                                // consume is pending
                                apiCallInProgress = false;
                                summaryFuture = this.summaryFuture;
                                this.summaryFuture = null;
                                error = this.error;
                                errorExposed = true;
                            }
                        }
                    }
                }
                if (databaseBookmark != null) {
                    bookmarkConsumer.accept(databaseBookmark);
                }
            }

            if (closeOnSummary) {
                var errorSnapshot = error;
                var recordFutureSnapshot = recordFuture;
                var secondRecordFutureSnapshot = secondRecordFuture;
                var recordsFutureRunnableSnapshot = recordsFutureRunnable;
                var summaryFutureSnapshot = summaryFuture;
                boltConnection.close().whenComplete((ignored, closeThrowable) -> {
                    if (peekFuture != null) {
                        if (errorSnapshot != null) {
                            peekFuture.completeExceptionally(errorSnapshot);
                        }
                        peekFuture.complete(null);
                    }
                    if (recordFutureSnapshot != null) {
                        if (errorSnapshot != null) {
                            recordFutureSnapshot.completeExceptionally(errorSnapshot);
                        }
                        recordFutureSnapshot.complete(null);
                    } else if (secondRecordFutureSnapshot != null) {
                        if (errorSnapshot != null) {
                            secondRecordFutureSnapshot.completeExceptionally(errorSnapshot);
                        }
                        secondRecordFutureSnapshot.complete(false);
                    } else if (recordsFutureRunnableSnapshot != null) {
                        recordsFutureRunnableSnapshot.run();
                        if (summaryFutureSnapshot != null) {
                            // for-each using list
                            summaryFutureSnapshot.complete(this.summary);
                        }
                    } else if (summaryFutureSnapshot != null) {
                        if (errorSnapshot != null) {
                            summaryFutureSnapshot.completeExceptionally(errorSnapshot);
                        }
                        summaryFutureSnapshot.complete(this.summary);
                    }
                });
            } else {
                if (peekFuture != null) {
                    if (error != null) {
                        peekFuture.completeExceptionally(error);
                    }
                    peekFuture.complete(null);
                }
                if (recordFuture != null) {
                    if (error != null) {
                        recordFuture.completeExceptionally(error);
                    }
                    recordFuture.complete(null);
                } else if (secondRecordFuture != null) {
                    if (error != null) {
                        secondRecordFuture.completeExceptionally(error);
                    }
                    secondRecordFuture.complete(false);
                } else if (recordsFutureRunnable != null) {
                    recordsFutureRunnable.run();
                    if (summaryFuture != null) {
                        // for-each using list
                        summaryFuture.complete(this.summary);
                    }
                } else if (summaryFuture != null) {
                    if (error != null) {
                        summaryFuture.completeExceptionally(error);
                    }
                    summaryFuture.complete(this.summary);
                }
            }
        }
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    public void onComplete() {
        Throwable throwable;
        synchronized (this) {
            throwable = this.error;
        }
        if (throwable != null) {
            if (beginFuture != null) {
                if (!beginFuture.isDone()) {
                    // not exposed yet, fail
                    if (closeOnSummary) {
                        boltConnection.close().whenComplete((ignored, closeThrowable) -> {
                            if (closeThrowable != null && throwable != closeThrowable) {
                                throwable.addSuppressed(closeThrowable);
                            }
                            beginFuture.completeExceptionally(throwable);
                        });
                    } else {
                        beginFuture.completeExceptionally(throwable);
                    }
                    return;
                } else if (beginFuture.isCompletedExceptionally()) {
                    return;
                }
            }

            CompletableFuture<Record> peekFuture;
            CompletableFuture<Record> recordFuture = null;
            CompletableFuture<Boolean> secondRecordFuture = null;
            CompletableFuture<List<Record>> recordsFuture = null;
            CompletableFuture<ResultSummary> summaryFuture = null;

            Runnable finisher;

            synchronized (this) {
                state = State.FAILED;
                this.error = throwable;

                if (!resultCursorFuture.isDone()) {
                    // not exposed yet, fail
                    if (closeOnSummary) {
                        finisher = () -> boltConnection.close().whenComplete((ignored, closeThrowable) -> {
                            if (closeThrowable != null && throwable != closeThrowable) {
                                throwable.addSuppressed(closeThrowable);
                            }
                            resultCursorFuture.completeExceptionally(throwable);
                        });
                    } else {
                        finisher = () -> resultCursorFuture.completeExceptionally(throwable);
                    }
                } else if (resultCursorFuture.isCompletedExceptionally()) {
                    finisher = () -> {};
                } else {
                    peekFuture = this.peekFuture;
                    this.peekFuture = null;
                    if (peekFuture != null) {
                        errorExposed = true;
                        apiCallInProgress = false;
                    } else {
                        recordFuture = this.recordFuture;
                        this.recordFuture = null;
                        if (recordFuture != null) {
                            secondRecordFuture = this.secondRecordFuture;
                            this.secondRecordFuture = null;
                            errorExposed = true;
                            apiCallInProgress = false;
                        } else {
                            secondRecordFuture = this.secondRecordFuture;
                            this.secondRecordFuture = null;
                            if (secondRecordFuture != null) {
                                errorExposed = true;
                                apiCallInProgress = false;
                            } else {
                                recordsFuture = this.recordsFuture;
                                this.recordsFuture = null;
                                if (recordsFuture != null) {
                                    errorExposed = true;
                                    apiCallInProgress = false;
                                } else {
                                    summaryFuture = this.summaryFuture;
                                    this.summaryFuture = null;
                                    if (summaryFuture != null) {
                                        errorExposed = true;
                                        apiCallInProgress = false;
                                    }
                                }
                            }
                        }
                    }
                    var recordFutureSnapshot = recordFuture;
                    var secondRecordFutureSnapshot = secondRecordFuture;
                    var recordsFutureSnapshot = recordsFuture;
                    var summaryFutureSnapshot = summaryFuture;
                    if (closeOnSummary) {
                        finisher = () -> boltConnection.close().whenComplete((ignored, closeThrowable) -> {
                            if (closeThrowable != null && throwable != closeThrowable) {
                                throwable.addSuppressed(closeThrowable);
                            }
                            if (peekFuture != null) {
                                peekFuture.completeExceptionally(throwable);
                            }
                            if (recordFutureSnapshot != null) {
                                recordFutureSnapshot.completeExceptionally(throwable);
                            }
                            if (secondRecordFutureSnapshot != null) {
                                secondRecordFutureSnapshot.completeExceptionally(throwable);
                            }
                            if (recordsFutureSnapshot != null) {
                                recordsFutureSnapshot.completeExceptionally(throwable);
                            }
                            if (summaryFutureSnapshot != null) {
                                summaryFutureSnapshot.completeExceptionally(throwable);
                            }
                        });
                    } else {
                        finisher = () -> {
                            if (peekFuture != null) {
                                peekFuture.completeExceptionally(throwable);
                            }
                            if (recordFutureSnapshot != null) {
                                recordFutureSnapshot.completeExceptionally(throwable);
                            }
                            if (secondRecordFutureSnapshot != null) {
                                secondRecordFutureSnapshot.completeExceptionally(throwable);
                            }
                            if (recordsFutureSnapshot != null) {
                                recordsFutureSnapshot.completeExceptionally(throwable);
                            }
                            if (summaryFutureSnapshot != null) {
                                summaryFutureSnapshot.completeExceptionally(throwable);
                            }
                        };
                    }
                }
            }

            finisher.run();
        }
    }

    @Override
    public synchronized CompletionStage<Throwable> discardAllFailureAsync() {
        return consumeAsync().handle((summary, error) -> error);
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    public CompletionStage<Throwable> pullAllFailureAsync() {
        synchronized (this) {
            if (apiCallInProgress) {
                var message = "API calls to result cursor must be sequential.";
                return CompletableFuture.failedStage(new ClientException(
                        GqlStatusError.UNKNOWN.getStatus(),
                        GqlStatusError.UNKNOWN.getStatusDescription(message),
                        "N/A",
                        message,
                        GqlStatusError.DIAGNOSTIC_RECORD,
                        null));
            }
            return switch (state) {
                case READY -> {
                    apiCallInProgress = true;
                    summaryFuture = new CompletableFuture<>();
                    state = State.STREAMING;
                    updateRecordState(RecordState.NO_RECORD);
                    boltConnection
                            .writeAndFlush(this, Messages.pull(runSummary.queryId(), -1))
                            .whenComplete((ignored, throwable) -> {
                                var error = Futures.completionExceptionCause(throwable);
                                CompletableFuture<ResultSummary> summaryFuture;
                                if (error != null) {
                                    synchronized (this) {
                                        state = State.FAILED;
                                        errorExposed = true;
                                        summaryFuture = this.summaryFuture;
                                        this.summaryFuture = null;
                                        apiCallInProgress = false;
                                    }
                                    summaryFuture.completeExceptionally(error);
                                }
                            });
                    yield summaryFuture.handle((ignored, throwable) -> throwable);
                }
                case STREAMING -> {
                    apiCallInProgress = true;
                    // no pending request should be in place
                    recordsFuture = new CompletableFuture<>();
                    keepRecords = true;
                    yield recordsFuture.handle((ignored, throwable) -> throwable);
                }
                case DISCARDING -> {
                    apiCallInProgress = true;
                    // no pending request should be in place
                    summaryFuture = new CompletableFuture<>();
                    yield summaryFuture.handle((ignored, throwable) -> throwable);
                }
                case FAILED -> stageExposingError(null).handle((ignored, throwable) -> throwable);
                case SUCCEEDED -> CompletableFuture.completedStage(null);
            };
        }
    }

    @Override
    public CompletionStage<Void> consumed() {
        return consumedFuture;
    }

    private <T> CompletionStage<T> stageExposingError(T value) {
        synchronized (this) {
            if (error != null && !errorExposed) {
                errorExposed = true;
                return CompletableFuture.failedStage(error);
            }
        }
        return CompletableFuture.completedStage(value);
    }
}
