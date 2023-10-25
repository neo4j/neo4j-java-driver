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

import static java.util.Objects.requireNonNull;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.handlers.PullAllResponseHandler;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.Futures;

/**
 * Used by Bolt V1, V2, V3
 */
public class AsyncResultCursorOnlyFactory implements ResultCursorFactory {
    protected final Connection connection;
    protected final Message runMessage;
    protected final RunResponseHandler runHandler;
    private final CompletableFuture<Void> runFuture;
    protected final PullAllResponseHandler pullAllHandler;

    public AsyncResultCursorOnlyFactory(
            Connection connection,
            Message runMessage,
            RunResponseHandler runHandler,
            CompletableFuture<Void> runFuture,
            PullAllResponseHandler pullHandler) {
        requireNonNull(connection);
        requireNonNull(runMessage);
        requireNonNull(runHandler);
        requireNonNull(runFuture);
        requireNonNull(pullHandler);

        this.connection = connection;
        this.runMessage = runMessage;
        this.runHandler = runHandler;
        this.runFuture = runFuture;

        this.pullAllHandler = pullHandler;
    }

    public CompletionStage<AsyncResultCursor> asyncResult() {
        // only write and flush messages when async result is wanted.
        connection.write(runMessage, runHandler); // queues the run message, will be flushed with pull message together
        pullAllHandler.prePopulateRecords();

        return runFuture.handle((ignored, error) ->
                new DisposableAsyncResultCursor(new AsyncResultCursorImpl(error, runHandler, pullAllHandler)));
    }

    public CompletionStage<RxResultCursor> rxResult() {
        return Futures.failedFuture(
                new ClientException("Driver is connected to the database that does not support driver reactive API. "
                        + "In order to use the driver reactive API, please upgrade to neo4j 4.0.0 or later."));
    }
}
