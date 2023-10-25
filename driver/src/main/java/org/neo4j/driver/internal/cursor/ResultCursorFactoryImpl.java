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
import org.neo4j.driver.internal.handlers.PullAllResponseHandler;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.handlers.pulln.PullResponseHandler;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.spi.Connection;

/**
 * Bolt V4
 */
public class ResultCursorFactoryImpl implements ResultCursorFactory {
    private final RunResponseHandler runHandler;
    private final Connection connection;

    private final PullResponseHandler pullHandler;
    private final PullAllResponseHandler pullAllHandler;
    private final Message runMessage;
    private final CompletableFuture<Void> runFuture;

    public ResultCursorFactoryImpl(
            Connection connection,
            Message runMessage,
            RunResponseHandler runHandler,
            CompletableFuture<Void> runFuture,
            PullResponseHandler pullHandler,
            PullAllResponseHandler pullAllHandler) {
        requireNonNull(connection);
        requireNonNull(runMessage);
        requireNonNull(runHandler);
        requireNonNull(runFuture);
        requireNonNull(pullHandler);
        requireNonNull(pullAllHandler);

        this.connection = connection;
        this.runMessage = runMessage;
        this.runHandler = runHandler;
        this.runFuture = runFuture;
        this.pullHandler = pullHandler;
        this.pullAllHandler = pullAllHandler;
    }

    @Override
    public CompletionStage<AsyncResultCursor> asyncResult() {
        // only write and flush messages when async result is wanted.
        connection.write(runMessage, runHandler); // queues the run message, will be flushed with pull message together
        pullAllHandler.prePopulateRecords();
        return runFuture.handle((ignored, error) ->
                new DisposableAsyncResultCursor(new AsyncResultCursorImpl(error, runHandler, pullAllHandler)));
    }

    @Override
    public CompletionStage<RxResultCursor> rxResult() {
        connection.writeAndFlush(runMessage, runHandler);
        return runFuture.handle(
                (ignored, error) -> new RxResultCursorImpl(error, runHandler, pullHandler, connection::release));
    }
}
