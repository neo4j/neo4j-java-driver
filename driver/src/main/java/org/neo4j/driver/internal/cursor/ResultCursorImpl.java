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
import java.util.function.Supplier;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.NoSuchRecordException;
import org.neo4j.driver.internal.DatabaseBookmark;
import org.neo4j.driver.internal.FailableCursor;
import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.bolt.api.BoltConnection;
import org.neo4j.driver.internal.bolt.api.ResponseHandler;
import org.neo4j.driver.internal.bolt.api.summary.DiscardSummary;
import org.neo4j.driver.internal.bolt.api.summary.PullSummary;
import org.neo4j.driver.internal.bolt.api.summary.RunSummary;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.internal.util.MetadataExtractor;
import org.neo4j.driver.summary.ResultSummary;

public class ResultCursorImpl implements ResultCursor, FailableCursor, ResponseHandler {
    public static final MetadataExtractor METADATA_EXTRACTOR = new MetadataExtractor("t_last");
    private final BoltConnection boltConnection;
    private final Queue<Record> records;
    private final Query query;
    private final long fetchSize;
    private final Consumer<Throwable> throwableConsumer;
    private final Consumer<DatabaseBookmark> bookmarkConsumer;
    private final Supplier<Throwable> termSupplier;
    private final boolean closeOnSummary;
    private RunSummary runSummary;
    private State state;

    private boolean apiCallInProgress;
    private CompletableFuture<Record> peekFuture;
    private CompletableFuture<Record> recordFuture;
    private CompletableFuture<Boolean> secondRecordFuture;
    private CompletableFuture<List<Record>> recordsFuture;
    private CompletableFuture<ResultSummary> summaryFuture;
    private ResultSummary summary;
    private Throwable error;
    private boolean errorExposed;

    private enum State {
        READY,
        STREAMING,
        DISCARDING,
        FAILED,
        SUCCEDED
    }

    public ResultCursorImpl(
            BoltConnection boltConnection,
            Query query,
            long fetchSize,
            Consumer<Throwable> throwableConsumer,
            Consumer<DatabaseBookmark> bookmarkConsumer,
            boolean closeOnSummary,
            RunSummary runSummary,
            Supplier<Throwable> termSupplier) {
        this.boltConnection = Objects.requireNonNull(boltConnection);
        this.records = new ArrayDeque<>();
        this.query = Objects.requireNonNull(query);
        this.fetchSize = fetchSize;
        this.throwableConsumer = throwableConsumer;
        this.bookmarkConsumer = Objects.requireNonNull(bookmarkConsumer);
        this.closeOnSummary = closeOnSummary;
        this.runSummary = runSummary;
        this.state = State.STREAMING;
        this.termSupplier = termSupplier;
    }

    @Override
    public synchronized List<String> keys() {
        return runSummary.keys();
    }

    @Override
    public synchronized CompletionStage<ResultSummary> consumeAsync() {
        if (apiCallInProgress) {
            return CompletableFuture.failedStage(new ClientException("API calls to result cursor must be sequential."));
        }
        return switch (state) {
            case READY -> {
                var term = termSupplier.get();
                if (term == null) {
                    apiCallInProgress = true;
                    summaryFuture = new CompletableFuture<>();
                    state = State.DISCARDING;
                    boltConnection
                            .discard(runSummary.queryId(), -1)
                            .thenCompose(conn -> conn.flush(this))
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
                    yield summaryFuture;
                } else {
                    this.error = term;
                    this.state = State.FAILED;
                    this.errorExposed = true;
                    yield CompletableFuture.failedStage(error);
                }
            }
            case STREAMING -> {
                apiCallInProgress = true;
                summaryFuture = new CompletableFuture<>();
                yield summaryFuture;
            }
            case DISCARDING -> CompletableFuture.failedStage(new ClientException("Invalid API call."));
            case FAILED -> stageExposingError();
            case SUCCEDED -> CompletableFuture.completedStage(summary);
        };
    }

    @Override
    public synchronized CompletionStage<Record> nextAsync() {
        if (apiCallInProgress) {
            return CompletableFuture.failedStage(new ClientException("API calls to result cursor must be sequential."));
        }
        var record = records.poll();
        if (record == null) {
            // buffer is empty
            return switch (state) {
                case READY -> {
                    var term = termSupplier.get();
                    if (term == null) {
                        apiCallInProgress = true;
                        recordFuture = new CompletableFuture<>();
                        state = State.STREAMING;
                        boltConnection
                                .pull(runSummary.queryId(), fetchSize)
                                .thenCompose(conn -> conn.flush(this))
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
                        yield recordFuture;
                    } else {
                        this.error = term;
                        this.state = State.FAILED;
                        this.errorExposed = true;
                        yield CompletableFuture.failedStage(error);
                    }
                }
                case STREAMING -> {
                    apiCallInProgress = true;
                    recordFuture = new CompletableFuture<>();
                    yield recordFuture;
                }
                case DISCARDING -> CompletableFuture.failedStage(new ClientException("Invalid API call."));
                case FAILED -> stageExposingError();
                case SUCCEDED -> CompletableFuture.completedStage(null);
            };
        } else {
            return completedFuture(record);
        }
    }

    @Override
    public synchronized CompletionStage<Record> peekAsync() {
        if (apiCallInProgress) {
            return CompletableFuture.failedStage(new ClientException("API calls to result cursor must be sequential."));
        }
        var record = records.peek();
        if (record == null) {
            // buffer is empty
            return switch (state) {
                case READY -> {
                    var term = termSupplier.get();
                    if (term == null) {
                        apiCallInProgress = true;
                        peekFuture = new CompletableFuture<>();
                        state = State.STREAMING;
                        boltConnection
                                .pull(runSummary.queryId(), fetchSize)
                                .thenCompose(conn -> conn.flush(this))
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
                        yield peekFuture;
                    } else {
                        this.error = term;
                        this.state = State.FAILED;
                        this.errorExposed = true;
                        yield CompletableFuture.failedStage(error);
                    }
                }
                case STREAMING -> {
                    apiCallInProgress = true;
                    peekFuture = new CompletableFuture<>();
                    yield peekFuture;
                }
                case DISCARDING -> CompletableFuture.failedStage(new ClientException("Invalid API call."));
                case FAILED -> stageExposingError();
                case SUCCEDED -> CompletableFuture.completedStage(null);
            };
        } else {
            return completedFuture(record);
        }
    }

    @Override
    public synchronized CompletionStage<Record> singleAsync() {
        if (apiCallInProgress) {
            return CompletableFuture.failedStage(new ClientException("API calls to result cursor must be sequential."));
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
                        var term = termSupplier.get();
                        if (term == null) {
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
                            boltConnection
                                    .pull(runSummary.queryId(), fetchSize)
                                    .thenCompose(conn -> conn.flush(this))
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
                            this.error = term;
                            this.state = State.FAILED;
                            this.errorExposed = true;
                            yield CompletableFuture.failedStage(error);
                        }
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
                case DISCARDING -> CompletableFuture.failedStage(new ClientException("Invalid API call."));
                case FAILED -> stageExposingError().thenApply(ignored -> {
                    throw new NoSuchRecordException("Cannot retrieve a single record, because this result is empty.");
                });
                case SUCCEDED -> records.size() == 1
                        ? CompletableFuture.completedFuture(records.poll())
                        : CompletableFuture.failedStage(new NoSuchRecordException(
                                "Cannot retrieve a single record, because this result is empty."));
            };
        }
    }

    @Override
    public synchronized CompletionStage<ResultSummary> forEachAsync(Consumer<Record> action) {
        if (apiCallInProgress) {
            return CompletableFuture.failedStage(new ClientException("API calls to result cursor must be sequential."));
        }
        return null;
    }

    @Override
    public synchronized CompletionStage<List<Record>> listAsync() {
        if (apiCallInProgress) {
            return CompletableFuture.failedStage(new ClientException("API calls to result cursor must be sequential."));
        }
        return switch (state) {
            case READY -> {
                var term = termSupplier.get();
                if (term == null) {
                    apiCallInProgress = true;
                    recordsFuture = new CompletableFuture<>();
                    state = State.STREAMING;
                    boltConnection
                            .pull(runSummary.queryId(), -1)
                            .thenCompose(conn -> conn.flush(this))
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
                    yield recordsFuture;
                } else {
                    this.error = term;
                    this.state = State.FAILED;
                    this.errorExposed = true;
                    yield CompletableFuture.failedStage(error);
                }
            }
            case STREAMING -> {
                apiCallInProgress = true;
                recordsFuture = new CompletableFuture<>();
                yield recordsFuture;
            }
            case DISCARDING -> CompletableFuture.failedStage(new ClientException("Invalid API call."));
            case FAILED -> stageExposingError().thenApply(ignored -> Collections.emptyList());
            case SUCCEDED -> {
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
            return CompletableFuture.failedStage(new ClientException("API calls to result cursor must be sequential."));
        }
        return switch (state) {
            case READY, STREAMING, DISCARDING -> CompletableFuture.completedStage(true);
            case FAILED, SUCCEDED -> CompletableFuture.completedStage(false);
        };
    }

    @Override
    public void onRecord(Value[] fields) {
        var record = new InternalRecord(runSummary.keys(), fields);
        CompletableFuture<Record> peekFuture;
        CompletableFuture<Record> recordFuture = null;
        CompletableFuture<Boolean> secondRecordFuture = null;
        synchronized (this) {
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

    @Override
    public void onError(Throwable throwable) {
        CompletableFuture<Record> peekFuture;
        CompletableFuture<Record> recordFuture = null;
        CompletableFuture<Boolean> secondRecordFuture = null;
        CompletableFuture<List<Record>> recordsFuture = null;
        CompletableFuture<ResultSummary> summaryFuture = null;

        synchronized (this) {
            state = State.FAILED;
            this.error = throwable;

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
        }

        if (peekFuture != null) {
            peekFuture.completeExceptionally(throwable);
        }
        if (recordFuture != null) {
            recordFuture.completeExceptionally(throwable);
        }
        if (secondRecordFuture != null) {
            secondRecordFuture.completeExceptionally(throwable);
        }
        if (recordsFuture != null) {
            recordsFuture.completeExceptionally(throwable);
        }
        if (summaryFuture != null) {
            summaryFuture.completeExceptionally(throwable);
        }

        if (throwableConsumer != null) {
            throwableConsumer.accept(throwable);
        }
        if (closeOnSummary) {
            boltConnection.close();
        }
    }

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
                    this.summary = METADATA_EXTRACTOR.extractSummary(query, boltConnection, -1, summary.metadata());
                    state = State.SUCCEDED;
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
                if (closeOnSummary) {
                    boltConnection.close();
                }
            } else {
                onError(summaryError);
            }
        }
    }

    @Override
    public void onPullSummary(PullSummary summary) {
        if (summary.hasMore()) {
            CompletableFuture<Boolean> secondRecordFuture = null;
            synchronized (this) {
                if (this.peekFuture != null) {
                    var term = termSupplier.get();
                    if (term == null) {
                        // peek is pending, keep streaming
                        state = State.STREAMING;
                        boltConnection
                                .pull(runSummary.queryId(), fetchSize)
                                .thenCompose(conn -> conn.flush(this))
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
                    } else {
                        this.error = term;
                        this.state = State.FAILED;
                        this.errorExposed = true;
                        var peekFuture = this.peekFuture;
                        this.peekFuture = null;
                        peekFuture.completeExceptionally(error);
                    }
                } else if (this.recordFuture != null) {
                    var term = termSupplier.get();
                    if (term == null) {
                        // next is pending, keep streaming
                        state = State.STREAMING;
                        boltConnection
                                .pull(runSummary.queryId(), fetchSize)
                                .thenCompose(conn -> conn.flush(this))
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
                        this.error = term;
                        this.state = State.FAILED;
                        this.errorExposed = true;
                        var recordFuture = this.recordFuture;
                        this.recordFuture = null;
                        recordFuture.completeExceptionally(error);
                    }
                } else {
                    secondRecordFuture = this.secondRecordFuture;
                    this.secondRecordFuture = null;

                    if (secondRecordFuture != null) {
                        // single is pending
                        apiCallInProgress = false;
                        state = State.READY;
                    } else {
                        if (this.recordsFuture != null) {
                            var term = termSupplier.get();
                            if (term == null) {
                                // list is pending, stream all
                                state = State.STREAMING;
                                boltConnection
                                        .pull(runSummary.queryId(), -1)
                                        .thenCompose(conn -> conn.flush(this))
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
                            } else {
                                this.error = term;
                                this.state = State.FAILED;
                                this.errorExposed = true;
                                var recordsFuture = this.recordsFuture;
                                this.recordsFuture = null;
                                recordsFuture.completeExceptionally(error);
                            }
                        } else if (this.summaryFuture != null) {
                            var term = termSupplier.get();
                            if (term == null) {
                                // consume is pending, discard all
                                state = State.DISCARDING;
                                boltConnection
                                        .discard(runSummary.queryId(), -1)
                                        .thenCompose(conn -> conn.flush(this))
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
                                this.error = term;
                                this.state = State.FAILED;
                                this.errorExposed = true;
                                var summaryFuture = this.recordsFuture;
                                this.summaryFuture = null;
                                summaryFuture.completeExceptionally(error);
                            }
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
                state = State.SUCCEDED;
                try {
                    this.summary = METADATA_EXTRACTOR.extractSummary(
                            query, boltConnection, runSummary.resultAvailableAfter(), summary.metadata());
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
                                // list is pending
                                apiCallInProgress = false;
                                if (this.error == null) {
                                    var recordsFuture = this.recordsFuture;
                                    this.recordsFuture = null;
                                    var records = this.records.stream().toList();
                                    this.records.clear();
                                    recordsFutureRunnable = () -> recordsFuture.complete(records);
                                } else {
                                    recordsFutureRunnable = () -> recordsFuture.completeExceptionally(this.error);
                                    errorExposed = true;
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
            }
            if (databaseBookmark != null) {
                bookmarkConsumer.accept(databaseBookmark);
            }
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
            } else if (summaryFuture != null) {
                if (error != null) {
                    summaryFuture.completeExceptionally(error);
                }
                summaryFuture.complete(this.summary);
            }
            if (throwableConsumer != null && error != null) {
                throwableConsumer.accept(error);
            }
            if (closeOnSummary) {
                boltConnection.close();
            }
        }
    }

    @Override
    public synchronized CompletionStage<Throwable> discardAllFailureAsync() {
        return consumeAsync().handle((summary, error) -> error);
    }

    @Override
    public CompletionStage<Throwable> pullAllFailureAsync() {
        synchronized (this) {
            if (apiCallInProgress) {
                return CompletableFuture.failedStage(
                        new ClientException("API calls to result cursor must be sequential."));
            }
            return switch (state) {
                case READY -> {
                    var term = termSupplier.get();
                    if (term == null) {
                        apiCallInProgress = true;
                        summaryFuture = new CompletableFuture<>();
                        state = State.STREAMING;
                        boltConnection
                                .pull(runSummary.queryId(), -1)
                                .thenCompose(conn -> conn.flush(this))
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
                    } else {
                        this.error = term;
                        this.state = State.FAILED;
                        this.errorExposed = true;
                        yield CompletableFuture.failedStage(error);
                    }
                }
                case STREAMING -> {
                    var term = termSupplier.get();
                    if (term == null) {
                        apiCallInProgress = true;
                        // no pending request should be in place
                        recordsFuture = new CompletableFuture<>();
                        yield recordsFuture.handle((ignored, throwable) -> throwable);
                    } else {
                        this.error = term;
                        this.state = State.FAILED;
                        this.errorExposed = true;
                        yield CompletableFuture.failedStage(error);
                    }
                }
                case DISCARDING -> {
                    var term = termSupplier.get();
                    if (term == null) {
                        apiCallInProgress = true;
                        // no pending request should be in place
                        summaryFuture = new CompletableFuture<>();
                        yield summaryFuture.handle((ignored, throwable) -> throwable);
                    } else {
                        this.error = term;
                        this.state = State.FAILED;
                        this.errorExposed = true;
                        yield CompletableFuture.failedStage(error);
                    }
                }
                case FAILED -> stageExposingError().handle((ignored, throwable) -> throwable);
                case SUCCEDED -> CompletableFuture.completedStage(null);
            };
        }
    }

    private <T> CompletionStage<T> stageExposingError() {
        synchronized (this) {
            if (error != null && !errorExposed) {
                errorExposed = true;
                return CompletableFuture.failedStage(error);
            }
        }
        return CompletableFuture.completedStage(null);
    }
}
