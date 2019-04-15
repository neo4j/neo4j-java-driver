/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.driver.internal.async.AsyncStatementResultCursor;
import org.neo4j.driver.internal.handlers.PullAllResponseHandler;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.handlers.pulln.BasicPullResponseHandler;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.request.PullMessage;
import org.neo4j.driver.internal.spi.Connection;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class InternalStatementResultCursorFactory implements StatementResultCursorFactory
{
    private final RunResponseHandler runHandler;
    private final Connection connection;

    private final BasicPullResponseHandler pullHandler;
    private final PullAllResponseHandler pullAllHandler;
    private final boolean waitForRunResponse;
    private final Message runMessage;

    public InternalStatementResultCursorFactory( Connection connection, Message runMessage, RunResponseHandler runHandler, BasicPullResponseHandler pullHandler,
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
    public CompletionStage<InternalStatementResultCursor> asyncResult()
    {
        // only write and flush messages when async result is wanted.
        connection.writeAndFlush( runMessage, runHandler, PullMessage.PULL_ALL, pullAllHandler );

        if ( waitForRunResponse )
        {
            // wait for response of RUN before proceeding
            return runHandler.runFuture().thenApply( ignore -> new AsyncStatementResultCursor( runHandler, pullAllHandler ) );
        }
        else
        {
            return completedFuture( new AsyncStatementResultCursor( runHandler, pullAllHandler ) );
        }
    }

    @Override
    public CompletionStage<RxStatementResultCursor> rxResult()
    {
        connection.writeAndFlush( runMessage, runHandler );
        // we always wait for run reply
        return runHandler.runFuture().thenApply( this::composeRxCursor );
    }

    private RxStatementResultCursor composeRxCursor( Throwable runError )
    {
        return new RxStatementResultCursor( runError, runHandler, pullHandler );
    }
}
