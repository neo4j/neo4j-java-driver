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

import static org.neo4j.driver.internal.util.ErrorUtil.newResultConsumedError;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Function;
import org.neo4j.driver.Record;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.internal.FailableCursor;
import org.neo4j.driver.summary.ResultSummary;

public class DisposableResultCursorImpl implements ResultCursor, FailableCursor {
    private final ResultCursorImpl delegate;
    private boolean isDisposed;

    public DisposableResultCursorImpl(ResultCursorImpl delegate) {
        this.delegate = Objects.requireNonNull(delegate);
    }

    @Override
    public List<String> keys() {
        return delegate.keys();
    }

    @Override
    public CompletionStage<ResultSummary> consumeAsync() {
        isDisposed = true;
        return delegate.consumeAsync();
    }

    @Override
    public CompletionStage<Record> nextAsync() {
        return assertNotDisposed().thenCompose(ignored -> delegate.nextAsync());
    }

    @Override
    public CompletionStage<Record> peekAsync() {
        return assertNotDisposed().thenCompose(ignored -> delegate.peekAsync());
    }

    @Override
    public CompletionStage<Record> singleAsync() {
        return assertNotDisposed().thenCompose(ignored -> delegate.singleAsync());
    }

    @Override
    public CompletionStage<ResultSummary> forEachAsync(Consumer<Record> action) {
        return assertNotDisposed().thenCompose(ignored -> delegate.forEachAsync(action));
    }

    @Override
    public CompletionStage<List<Record>> listAsync() {
        return assertNotDisposed().thenCompose(ignored -> delegate.listAsync());
    }

    @Override
    public <T> CompletionStage<List<T>> listAsync(Function<Record, T> mapFunction) {
        return assertNotDisposed().thenCompose(ignored -> delegate.listAsync(mapFunction));
    }

    @Override
    public CompletionStage<Boolean> isOpenAsync() {
        return CompletableFuture.completedFuture(!isDisposed());
    }

    private <T> CompletableFuture<T> assertNotDisposed() {
        if (isDisposed) {
            return CompletableFuture.failedFuture(newResultConsumedError());
        }
        return completedWithNull();
    }

    boolean isDisposed() {
        return this.isDisposed;
    }

    @Override
    public CompletionStage<Throwable> discardAllFailureAsync() {
        isDisposed = true;
        return delegate.discardAllFailureAsync();
    }

    @Override
    public CompletionStage<Throwable> pullAllFailureAsync() {
        return delegate.pullAllFailureAsync();
    }

    @Override
    public CompletionStage<Void> consumed() {
        return delegate().consumed();
    }

    public ResultCursorImpl delegate() {
        return delegate;
    }
}
