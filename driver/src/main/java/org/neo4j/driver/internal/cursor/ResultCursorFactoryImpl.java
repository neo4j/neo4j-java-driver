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

import java.util.concurrent.CompletionStage;

import org.neo4j.driver.internal.handlers.PullAllResponseHandler;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.handlers.pulln.PullResponseHandler;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.spi.Connection;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * Bolt V4
 */
public class ResultCursorFactoryImpl implements ResultCursorFactory
{
    private final RunResponseHandler runHandler;
    private final Connection connection;

    private final PullResponseHandler pullHandler;
    private final PullAllResponseHandler pullAllHandler;
    private final boolean waitForRunResponse;
    private final Message runMessage;

    public ResultCursorFactoryImpl(Connection connection, Message runMessage, RunResponseHandler runHandler, PullResponseHandler pullHandler,
                                   PullAllResponseHandler pullAllHandler, boolean waitForRunResponse )
    {
        requireNonNull( connection );
        requireNonNull( runMessage );
        requireNonNull( runHandler );
        requireNonNull( pullHandler );
        requireNonNull( pullAllHandler );

        this.connection = connection;
        this.runMessage = runMessage;
        this.runHandler = runHandler;
        this.pullHandler = pullHandler;
        this.pullAllHandler = pullAllHandler;
        this.waitForRunResponse = waitForRunResponse;
    }

    @Override
    public CompletionStage<AsyncResultCursor> asyncResult()
    {
        // only write and flush messages when async result is wanted.
        connection.write( runMessage, runHandler ); // queues the run message, will be flushed with pull message together
        pullAllHandler.prePopulateRecords();

        if ( waitForRunResponse )
        {
            // wait for response of RUN before proceeding
            return runHandler.runFuture().thenApply(
                    ignore -> new DisposableAsyncResultCursor( new AsyncResultCursorImpl( runHandler, pullAllHandler ) ) );
        }
        else
        {
            return completedFuture( new DisposableAsyncResultCursor( new AsyncResultCursorImpl( runHandler, pullAllHandler ) ) );
        }
    }

    @Override
    public CompletionStage<RxResultCursor> rxResult()
    {
        connection.writeAndFlush( runMessage, runHandler );
        // we always wait for run reply
        return runHandler.runFuture().thenApply( this::composeRxCursor );
    }

    private RxResultCursor composeRxCursor(Throwable runError )
    {
        return new RxResultCursorImpl( runError, runHandler, pullHandler );
    }
}
