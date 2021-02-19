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
package org.neo4j.driver.internal.cursor;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Function;

import org.neo4j.driver.Record;
import org.neo4j.driver.summary.ResultSummary;

import static org.neo4j.driver.internal.util.ErrorUtil.newResultConsumedError;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.internal.util.Futures.failedFuture;

public class DisposableAsyncResultCursor implements AsyncResultCursor
{
    private final AsyncResultCursor delegate;
    private boolean isDisposed;

    public DisposableAsyncResultCursor(AsyncResultCursor delegate )
    {
        this.delegate = delegate;
    }

    @Override
    public List<String> keys()
    {
        return delegate.keys();
    }

    @Override
    public CompletionStage<ResultSummary> consumeAsync()
    {
        isDisposed = true;
        return delegate.consumeAsync();
    }

    @Override
    public CompletionStage<Record> nextAsync()
    {
        return assertNotDisposed().thenCompose( ignored -> delegate.nextAsync() );
    }

    @Override
    public CompletionStage<Record> peekAsync()
    {
        return assertNotDisposed().thenCompose( ignored -> delegate.peekAsync() );
    }

    @Override
    public CompletionStage<Record> singleAsync()
    {
        return assertNotDisposed().thenCompose( ignored -> delegate.singleAsync() );
    }

    @Override
    public CompletionStage<ResultSummary> forEachAsync( Consumer<Record> action )
    {
        return assertNotDisposed().thenCompose( ignored -> delegate.forEachAsync( action ) );
    }

    @Override
    public CompletionStage<List<Record>> listAsync()
    {
        return assertNotDisposed().thenCompose( ignored -> delegate.listAsync() );
    }

    @Override
    public <T> CompletionStage<List<T>> listAsync( Function<Record,T> mapFunction )
    {
        return assertNotDisposed().thenCompose( ignored -> delegate.listAsync( mapFunction ) );
    }

    @Override
    public CompletionStage<Throwable> discardAllFailureAsync()
    {
        isDisposed = true;
        return delegate.discardAllFailureAsync();
    }

    @Override
    public CompletionStage<Throwable> pullAllFailureAsync()
    {
        // This one does not dispose the result so that a user could still visit the buffered result after this method call.
        // This also does not assert not disposed so that this method can be called after summary.
        return delegate.pullAllFailureAsync();
    }

    private <T> CompletableFuture<T> assertNotDisposed()
    {
        if ( isDisposed )
        {
            return failedFuture( newResultConsumedError() );
        }
        return completedWithNull();
    }

    boolean isDisposed()
    {
        return this.isDisposed;
    }
}
